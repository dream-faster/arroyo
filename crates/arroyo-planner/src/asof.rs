//! Support for ASOF joins.
//!
//! ASOF semantics: for each row on the left side, match the single most recent
//! row on the right side whose join keys match and whose timestamp is `<=` the
//! left row's timestamp.
//!
//! Implementation strategy:
//!  - The `sqlparser` fork already parses Snowflake-style
//!    `ASOF JOIN <right> MATCH_CONDITION (<left.ts> >= <right.ts>) ON <equi>`
//!    into [`sqlparser::ast::JoinOperator::AsOf`].
//!  - Before handing the AST to DataFusion (which does not support ASOF), we
//!    rewrite each `JoinOperator::AsOf { match_condition, constraint }` into a
//!    plain `JoinOperator::Inner` whose `ON` expression is
//!    `<constraint> AND <match_condition> AND _arroyo_asof(<lhs>, <rhs>)`.
//!  - `_arroyo_asof` is a placeholder UDF (always `TRUE`) that the planner
//!    detects in the resulting `LogicalPlan::Join`'s filter, extracts the
//!    timestamp arguments from, and uses to mark the join as ASOF.
//!  - The runtime operator (`JoinWithExpiration`) then narrows each
//!    left/right batch to the rows that satisfy ASOF semantics before feeding
//!    the inner-join physical plan.
//!
//! Restrictions: exactly one inequality in the `MATCH_CONDITION` (one of
//! `>=`, `>`, `<=`, `<`). Additional non-marker filter conditions in the ON
//! clause beyond the match condition and equi-conditions are preserved and
//! pushed through to the underlying DataFusion join.

use sqlparser::ast::{
    BinaryOperator, Cte, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    FunctionArguments, Join, JoinConstraint, JoinOperator, ObjectName, ObjectNamePart, Query,
    Select, SetExpr, Statement, TableFactor, TableWithJoins,
};
use sqlparser::parser::ParserError;

/// Marker UDF that the planner injects into the join `ON` expression to flag
/// the join as ASOF and to carry the left/right timestamp expressions through
/// DataFusion's SQL → logical plan stage.
pub const ASOF_MARKER_UDF: &str = "_arroyo_asof";

/// The bundled sqlparser fork supports `ASOF JOIN`, but not DuckDB's
/// `ASOF LEFT JOIN` spelling. Normalize the DuckDB surface syntax into an
/// equivalent plain `LEFT JOIN` whose `ON` clause carries the ASOF inequality
/// and marker UDF so the rest of planning can proceed unchanged.
pub fn normalize_duckdb_asof_left_joins(sql: &str) -> Result<String, ParserError> {
    let mut normalized = String::with_capacity(sql.len());
    let mut cursor = 0;

    while let Some(asof_idx) = find_keyword_outside(sql, "ASOF", cursor).filter(|idx| {
        find_keyword_outside(sql, "LEFT", idx + "ASOF".len())
            .is_some_and(|left_idx| sql[idx + "ASOF".len()..left_idx].trim().is_empty())
            && find_keyword_outside(sql, "JOIN", idx + "ASOF LEFT".len())
                .is_some_and(|join_idx| sql[idx + "ASOF LEFT".len()..join_idx].trim().is_empty())
    }) {
        let left_idx = find_keyword_outside(sql, "LEFT", asof_idx + "ASOF".len()).unwrap();
        let join_idx = find_keyword_outside(sql, "JOIN", left_idx + "LEFT".len()).unwrap();
        let match_idx = find_keyword_outside(sql, "MATCH_CONDITION", join_idx + "JOIN".len())
            .ok_or_else(|| {
                ParserError::ParserError(
                    "ASOF LEFT JOIN requires a MATCH_CONDITION clause".to_string(),
                )
            })?;

        normalized.push_str(&sql[cursor..asof_idx]);
        normalized.push_str("LEFT JOIN");
        normalized.push_str(&sql[join_idx + "JOIN".len()..match_idx]);

        let mut pos = match_idx + "MATCH_CONDITION".len();
        pos = skip_whitespace(sql, pos);
        let (match_condition, after_match) = extract_parenthesized(sql, pos)?;

        let on_idx = find_keyword_outside(sql, "ON", after_match).ok_or_else(|| {
            ParserError::ParserError("ASOF LEFT JOIN requires an ON clause".to_string())
        })?;
        if !sql[after_match..on_idx].trim().is_empty() {
            return Err(ParserError::ParserError(
                "unexpected tokens between MATCH_CONDITION and ON in ASOF LEFT JOIN".to_string(),
            ));
        }

        let on_start = skip_whitespace(sql, on_idx + "ON".len());
        let on_end = find_join_constraint_end(sql, on_start);
        let on_expr = sql[on_start..on_end].trim();
        let (left_ts, right_ts) = split_match_condition(match_condition.trim())?;

        normalized.push_str(" ON (");
        normalized.push_str(on_expr);
        normalized.push_str(") AND (");
        normalized.push_str(match_condition.trim());
        normalized.push_str(") AND ");
        normalized.push_str(ASOF_MARKER_UDF);
        normalized.push('(');
        normalized.push_str(left_ts.trim());
        normalized.push_str(", ");
        normalized.push_str(right_ts.trim());
        normalized.push(')');

        cursor = on_end;
    }

    normalized.push_str(&sql[cursor..]);
    Ok(normalized)
}

/// Rewrite every `ASOF JOIN ... MATCH_CONDITION (...) ON (...)` in `statements`
/// into a regular `INNER JOIN` whose ON expression carries an `_arroyo_asof`
/// marker call.
pub fn rewrite_asof_joins(statements: &mut [Statement]) -> Result<(), ParserError> {
    for stmt in statements.iter_mut() {
        rewrite_statement(stmt)?;
    }
    Ok(())
}

fn rewrite_statement(stmt: &mut Statement) -> Result<(), ParserError> {
    match stmt {
        Statement::Query(query) => rewrite_query(query),
        Statement::Insert(insert) => {
            if let Some(source) = insert.source.as_deref_mut() {
                rewrite_query(source)?;
            }
            Ok(())
        }
        Statement::CreateTable(t) => {
            if let Some(query) = t.query.as_deref_mut() {
                rewrite_query(query)?;
            }
            Ok(())
        }
        Statement::CreateView { query, .. } => rewrite_query(query),
        _ => Ok(()),
    }
}

fn rewrite_query(query: &mut Query) -> Result<(), ParserError> {
    if let Some(with) = query.with.as_mut() {
        for cte in with.cte_tables.iter_mut() {
            rewrite_cte(cte)?;
        }
    }
    rewrite_set_expr(&mut query.body)
}

fn rewrite_cte(cte: &mut Cte) -> Result<(), ParserError> {
    rewrite_query(cte.query.as_mut())
}

fn rewrite_set_expr(set_expr: &mut SetExpr) -> Result<(), ParserError> {
    match set_expr {
        SetExpr::Select(select) => rewrite_select(select),
        SetExpr::Query(q) => rewrite_query(q),
        SetExpr::SetOperation { left, right, .. } => {
            rewrite_set_expr(left)?;
            rewrite_set_expr(right)
        }
        SetExpr::Insert(stmt) | SetExpr::Update(stmt) => rewrite_statement(stmt),
        SetExpr::Values(_) | SetExpr::Table(_) => Ok(()),
    }
}

fn rewrite_select(select: &mut Select) -> Result<(), ParserError> {
    for table_with_joins in select.from.iter_mut() {
        rewrite_table_with_joins(table_with_joins)?;
    }
    Ok(())
}

fn rewrite_table_with_joins(t: &mut TableWithJoins) -> Result<(), ParserError> {
    rewrite_table_factor(&mut t.relation)?;
    for join in t.joins.iter_mut() {
        rewrite_table_factor(&mut join.relation)?;
        rewrite_join_operator(join)?;
    }
    Ok(())
}

fn rewrite_table_factor(tf: &mut TableFactor) -> Result<(), ParserError> {
    if let TableFactor::Derived { subquery, .. } = tf {
        rewrite_query(subquery)?;
    }
    Ok(())
}

fn rewrite_join_operator(join: &mut Join) -> Result<(), ParserError> {
    let JoinOperator::AsOf {
        match_condition,
        constraint,
    } = &join.join_operator
    else {
        return Ok(());
    };

    // Validate match_condition: must be a single supported inequality.
    let (lhs, rhs) = match match_condition {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::GtEq
            | BinaryOperator::Gt
            | BinaryOperator::LtEq
            | BinaryOperator::Lt => ((**left).clone(), (**right).clone()),
            _ => {
                return Err(ParserError::ParserError(format!(
                    "ASOF JOIN MATCH_CONDITION must be a single inequality (`>=`, `>`, `<=`, `<`), got `{op}`"
                )));
            }
        },
        other => {
            return Err(ParserError::ParserError(format!(
                "ASOF JOIN MATCH_CONDITION must be a single inequality comparison, got `{other}`"
            )));
        }
    };

    let on_expr = match constraint {
        JoinConstraint::On(e) => e.clone(),
        JoinConstraint::None => {
            return Err(ParserError::ParserError(
                "ASOF JOIN requires an ON clause with at least one equi-condition".into(),
            ));
        }
        _ => {
            return Err(ParserError::ParserError(
                "ASOF JOIN only supports ON constraints".into(),
            ));
        }
    };

    let marker = make_marker_call(lhs.clone(), rhs.clone());
    let combined = Expr::BinaryOp {
        left: Box::new(Expr::BinaryOp {
            left: Box::new(on_expr),
            op: BinaryOperator::And,
            right: Box::new(match_condition.clone()),
        }),
        op: BinaryOperator::And,
        right: Box::new(marker),
    };

    join.join_operator = JoinOperator::Inner(JoinConstraint::On(combined));
    Ok(())
}

fn make_marker_call(lhs: Expr, rhs: Expr) -> Expr {
    Expr::Function(Function {
        name: ObjectName(vec![ObjectNamePart::Identifier(
            sqlparser::ast::Ident::new(ASOF_MARKER_UDF),
        )]),
        uses_odbc_syntax: false,
        parameters: FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(lhs)),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(rhs)),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

fn find_keyword_outside(sql: &str, keyword: &str, start: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let keyword_bytes = keyword.as_bytes();
    let mut depth = 0usize;
    let mut i = start;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' if !in_double_quote => in_single_quote = !in_single_quote,
            b'"' if !in_single_quote => in_double_quote = !in_double_quote,
            b'(' if !in_single_quote && !in_double_quote => depth += 1,
            b')' if !in_single_quote && !in_double_quote && depth > 0 => depth -= 1,
            _ => {}
        }

        if depth == 0
            && !in_single_quote
            && !in_double_quote
            && i + keyword_bytes.len() <= bytes.len()
            && sql[i..i + keyword_bytes.len()].eq_ignore_ascii_case(keyword)
            && is_word_boundary(bytes, i.saturating_sub(1))
            && is_word_boundary(bytes, i + keyword_bytes.len())
        {
            return Some(i);
        }
        i += 1;
    }

    None
}

fn is_word_boundary(bytes: &[u8], index: usize) -> bool {
    if index >= bytes.len() {
        return true;
    }
    !bytes[index].is_ascii_alphanumeric() && bytes[index] != b'_'
}

fn skip_whitespace(sql: &str, mut pos: usize) -> usize {
    while pos < sql.len() && sql.as_bytes()[pos].is_ascii_whitespace() {
        pos += 1;
    }
    pos
}

fn extract_parenthesized(sql: &str, open_paren: usize) -> Result<(&str, usize), ParserError> {
    if sql.as_bytes().get(open_paren) != Some(&b'(') {
        return Err(ParserError::ParserError(
            "expected parenthesized MATCH_CONDITION expression".to_string(),
        ));
    }

    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut i = open_paren;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' if !in_double_quote => in_single_quote = !in_single_quote,
            b'"' if !in_single_quote => in_double_quote = !in_double_quote,
            b'(' if !in_single_quote && !in_double_quote => depth += 1,
            b')' if !in_single_quote && !in_double_quote => {
                depth -= 1;
                if depth == 0 {
                    return Ok((&sql[open_paren + 1..i], i + 1));
                }
            }
            _ => {}
        }
        i += 1;
    }

    Err(ParserError::ParserError(
        "unterminated MATCH_CONDITION expression".to_string(),
    ))
}

fn find_join_constraint_end(sql: &str, start: usize) -> usize {
    const BOUNDARY_KEYWORDS: &[&str] = &[
        "LEFT", "RIGHT", "FULL", "INNER", "CROSS", "JOIN", "WHERE", "GROUP", "HAVING", "ORDER",
        "LIMIT", "UNION", "QUALIFY", "WINDOW", ";",
    ];

    let bytes = sql.as_bytes();
    let mut depth = 0usize;
    let mut i = start;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' if !in_double_quote => in_single_quote = !in_single_quote,
            b'"' if !in_single_quote => in_double_quote = !in_double_quote,
            b'(' if !in_single_quote && !in_double_quote => depth += 1,
            b')' if !in_single_quote && !in_double_quote => {
                if depth == 0 {
                    return i;
                }
                depth -= 1;
            }
            _ => {}
        }

        if depth == 0 && !in_single_quote && !in_double_quote {
            for keyword in BOUNDARY_KEYWORDS {
                if *keyword == ";" && bytes[i] == b';' {
                    return i;
                }

                if i + keyword.len() <= bytes.len()
                    && sql[i..i + keyword.len()].eq_ignore_ascii_case(keyword)
                    && is_word_boundary(bytes, i.saturating_sub(1))
                    && is_word_boundary(bytes, i + keyword.len())
                {
                    return i;
                }
            }
        }

        i += 1;
    }

    sql.len()
}

fn split_match_condition(match_condition: &str) -> Result<(&str, &str), ParserError> {
    let bytes = match_condition.as_bytes();
    let mut depth = 0usize;
    let mut i = 0usize;
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    while i < bytes.len() {
        match bytes[i] {
            b'\'' if !in_double_quote => in_single_quote = !in_single_quote,
            b'"' if !in_single_quote => in_double_quote = !in_double_quote,
            b'(' if !in_single_quote && !in_double_quote => depth += 1,
            b')' if !in_single_quote && !in_double_quote && depth > 0 => depth -= 1,
            b'>' | b'<' if depth == 0 && !in_single_quote && !in_double_quote => {
                let op_len = if i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                    2
                } else {
                    1
                };
                let left = match_condition[..i].trim();
                let right = match_condition[i + op_len..].trim();
                if left.is_empty() || right.is_empty() {
                    break;
                }
                return Ok((left, right));
            }
            _ => {}
        }
        i += 1;
    }

    Err(ParserError::ParserError(
        "ASOF LEFT JOIN MATCH_CONDITION must be a single inequality".to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::ArroyoDialect;
    use sqlparser::parser::Parser;

    fn parse(sql: &str) -> Vec<Statement> {
        Parser::parse_sql(&ArroyoDialect {}, sql).unwrap()
    }

    fn rewrite(sql: &str) -> Result<Vec<Statement>, ParserError> {
        let mut stmts = parse(sql);
        rewrite_asof_joins(&mut stmts)?;
        Ok(stmts)
    }

    #[test]
    fn normalizes_duckdb_asof_left_join() {
        let sql = "SELECT * FROM left_t l ASOF LEFT JOIN right_t r \
                   MATCH_CONDITION (l.ts >= r.ts) ON l.k = r.k \
                   WHERE l.k > 10";
        let normalized = normalize_duckdb_asof_left_joins(sql).unwrap();
        assert!(normalized.contains("LEFT JOIN right_t r"), "{normalized}");
        assert!(!normalized.contains("ASOF LEFT JOIN"), "{normalized}");
        assert!(
            normalized.contains("_arroyo_asof(l.ts, r.ts)"),
            "{normalized}"
        );
        assert!(
            normalized.contains("ON (l.k = r.k) AND (l.ts >= r.ts)"),
            "{normalized}"
        );
        assert!(normalized.contains("WHERE l.k > 10"), "{normalized}");
        Parser::parse_sql(&ArroyoDialect {}, &normalized).unwrap();
    }

    /// Render each statement back to SQL — this is a stable way to assert the
    /// rewriter produced the structure we expect.
    fn rendered(stmts: &[Statement]) -> String {
        stmts
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn rewrites_basic_asof_to_inner_with_marker() {
        let sql = "SELECT * FROM left_t l ASOF JOIN right_t r \
                   MATCH_CONDITION (l.ts >= r.ts) ON l.k = r.k";
        let out = rewrite(sql).unwrap();
        let s = rendered(&out);
        assert!(
            s.to_uppercase().contains("INNER JOIN") || s.to_uppercase().contains("JOIN"),
            "expected an inner join in the rewrite, got {s}"
        );
        assert!(
            !s.to_uppercase().contains("ASOF JOIN"),
            "expected `ASOF JOIN` syntax to be removed, got {s}"
        );
        assert!(
            s.contains(ASOF_MARKER_UDF),
            "expected `{ASOF_MARKER_UDF}` marker call, got {s}"
        );
        // The original equi- and match-conditions must still be present.
        assert!(s.contains("l.k = r.k"), "got {s}");
        assert!(s.contains("l.ts >= r.ts"), "got {s}");
    }

    #[test]
    fn rewrites_strict_gt_match_condition() {
        let sql = "SELECT * FROM l ASOF JOIN r MATCH_CONDITION (l.ts > r.ts) ON l.k = r.k";
        let out = rewrite(sql).unwrap();
        let s = rendered(&out);
        assert!(s.contains("l.ts > r.ts"), "got {s}");
        assert!(s.contains(ASOF_MARKER_UDF), "got {s}");
    }

    #[test]
    fn rewrites_lte_match_condition() {
        let sql = "SELECT * FROM l ASOF JOIN r MATCH_CONDITION (l.ts <= r.ts) ON l.k = r.k";
        let out = rewrite(sql).unwrap();
        let s = rendered(&out);
        assert!(s.contains("l.ts <= r.ts"), "got {s}");
        assert!(s.contains(ASOF_MARKER_UDF), "got {s}");
    }

    #[test]
    fn rewrites_lt_match_condition() {
        let sql = "SELECT * FROM l ASOF JOIN r MATCH_CONDITION (l.ts < r.ts) ON l.k = r.k";
        let out = rewrite(sql).unwrap();
        let s = rendered(&out);
        assert!(s.contains("l.ts < r.ts"), "got {s}");
        assert!(s.contains(ASOF_MARKER_UDF), "got {s}");
    }

    #[test]
    fn rejects_non_inequality_match_condition() {
        // A boolean column reference is not a `>=` comparison.
        let err = rewrite("SELECT * FROM l ASOF JOIN r MATCH_CONDITION (l.flag) ON l.k = r.k")
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("MATCH_CONDITION"),
            "expected MATCH_CONDITION error, got: {err}"
        );
    }

    #[test]
    fn rewrites_asof_inside_cte_and_subquery() {
        // The rewriter must descend into CTEs and derived subqueries.
        let sql = "WITH q AS (\
                     SELECT * FROM l ASOF JOIN r MATCH_CONDITION (l.ts >= r.ts) ON l.k = r.k\
                   ) \
                   SELECT * FROM q";
        let out = rewrite(sql).unwrap();
        let s = rendered(&out);
        assert!(
            s.contains(ASOF_MARKER_UDF) && !s.to_uppercase().contains("ASOF JOIN"),
            "expected CTE ASOF to be rewritten, got: {s}"
        );

        let sql2 = "SELECT * FROM (\
                      SELECT * FROM l ASOF JOIN r MATCH_CONDITION (l.ts >= r.ts) ON l.k = r.k\
                    ) sub";
        let out2 = rewrite(sql2).unwrap();
        let s2 = rendered(&out2);
        assert!(
            s2.contains(ASOF_MARKER_UDF) && !s2.to_uppercase().contains("ASOF JOIN"),
            "expected subquery ASOF to be rewritten, got: {s2}"
        );
    }

    #[test]
    fn leaves_plain_inner_joins_untouched() {
        let sql = "SELECT * FROM l JOIN r ON l.k = r.k";
        let out = rewrite(sql).unwrap();
        let s = rendered(&out);
        assert!(
            !s.contains(ASOF_MARKER_UDF),
            "non-ASOF joins must not get a marker, got: {s}"
        );
    }

    #[test]
    fn rejects_asof_without_on() {
        // The Arroyo dialect requires an ON clause for ASOF (we surface a
        // clear error rather than silently dropping it).
        let err = rewrite("SELECT * FROM l ASOF JOIN r MATCH_CONDITION (l.ts >= r.ts)")
            .unwrap_err()
            .to_string();
        assert!(
            err.to_uppercase().contains("ON"),
            "expected error to mention ON clause, got: {err}"
        );
    }
}
