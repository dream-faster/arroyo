use crate::asof::ASOF_MARKER_UDF;
use crate::extension::join::{AsofConfig, JoinExtension};
use crate::extension::key_calculation::KeyCalculationExtension;
use crate::extension::lookup::{LookupJoin, LookupSource};
use crate::plan::WindowDetectingVisitor;
use crate::schemas::add_timestamp_field;
use crate::tables::ConnectorTable;
use crate::{ArroyoSchemaProvider, fields_with_qualifiers, schema_from_df_fields_with_metadata};
use arrow_schema::DataType;
use arroyo_datastream::WindowType;
use arroyo_rpc::UPDATING_META_FIELD;
use arroyo_rpc::grpc::api::AsofInequality;
use datafusion::common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion::common::{
    Column, DFSchema, DataFusionError, JoinConstraint, JoinType, Result, ScalarValue, Spans,
    TableReference, not_impl_err, plan_err,
};
use datafusion::logical_expr;
use datafusion::logical_expr::expr::{Alias, ScalarFunction};
use datafusion::logical_expr::expr_fn::cast;
use datafusion::logical_expr::utils::{
    can_hash, find_valid_equijoin_key_pair, split_conjunction_owned,
};
use datafusion::logical_expr::{
    BinaryExpr, Case, Expr, ExprSchemable, Extension, Join, LogicalPlan, Operator, Projection,
    build_join_schema,
};
use datafusion::prelude::coalesce;
use datafusion::sql::unparser::expr_to_sql;
use std::sync::Arc;

pub(crate) struct JoinRewriter<'a> {
    pub schema_provider: &'a ArroyoSchemaProvider,
}

impl JoinRewriter<'_> {
    fn check_join_windowing(join: &Join) -> Result<bool> {
        let left_window = WindowDetectingVisitor::get_window(&join.left)?;
        let right_window = WindowDetectingVisitor::get_window(&join.right)?;
        match (left_window, right_window) {
            (None, None) => {
                if join.join_type == JoinType::Inner {
                    Ok(false)
                } else {
                    Err(DataFusionError::NotImplemented(
                        "can't handle non-inner joins without windows".into(),
                    ))
                }
            }
            (None, Some(_)) => Err(DataFusionError::NotImplemented(
                "can't handle mixed windowing between left (non-windowed) and right (windowed)."
                    .into(),
            )),
            (Some(_), None) => Err(DataFusionError::NotImplemented(
                "can't handle mixed windowing between left (windowed) and right (non-windowed)."
                    .into(),
            )),
            (Some(left_window), Some(right_window)) => {
                if left_window != right_window {
                    return Err(DataFusionError::NotImplemented(
                        "can't handle mixed windowing between left and right".into(),
                    ));
                }
                // exclude session windows
                if let WindowType::Session { .. } = left_window {
                    return Err(DataFusionError::NotImplemented(
                        "can't handle session windows in joins".into(),
                    ));
                }

                Ok(true)
            }
        }
    }

    fn check_updating(left: &LogicalPlan, right: &LogicalPlan) -> Result<()> {
        if left
            .schema()
            .has_column_with_unqualified_name(UPDATING_META_FIELD)
        {
            return plan_err!("can't handle updating left side of join");
        }
        if right
            .schema()
            .has_column_with_unqualified_name(UPDATING_META_FIELD)
        {
            return plan_err!("can't handle updating right side of join");
        }
        Ok(())
    }

    fn create_join_key_plan(
        input: Arc<LogicalPlan>,
        join_expressions: Vec<Expr>,
        name: &'static str,
    ) -> Result<LogicalPlan> {
        let key_count = join_expressions.len();

        let join_expressions: Vec<_> = join_expressions
            .into_iter()
            .enumerate()
            .map(|(index, expr)| {
                expr.alias_qualified(
                    Some(TableReference::bare("_arroyo")),
                    format!("_key_{index}"),
                )
            })
            .chain(
                fields_with_qualifiers(input.schema())
                    .iter()
                    .map(|field| Expr::Column(field.qualified_column())),
            )
            .collect();

        // Calculate initial projection with default names
        let projection = Projection::try_new(join_expressions, input)?;
        let key_calculation_extension = KeyCalculationExtension::new_named_and_trimmed(
            LogicalPlan::Projection(projection),
            (0..key_count).collect(),
            name.to_string(),
        );
        Ok(LogicalPlan::Extension(Extension {
            node: Arc::new(key_calculation_extension),
        }))
    }

    fn post_join_timestamp_projection(&mut self, input: LogicalPlan) -> Result<LogicalPlan> {
        let schema = input.schema().clone();
        let mut schema_with_timestamp = fields_with_qualifiers(&schema);
        let timestamp_fields = schema_with_timestamp
            .iter()
            .filter(|field| field.name() == "_timestamp")
            .cloned()
            .collect::<Vec<_>>();

        if timestamp_fields.len() != 2 {
            return not_impl_err!("join must have two timestamp fields");
        }

        schema_with_timestamp.retain(|field| field.name() != "_timestamp");
        let mut projection_expr = schema_with_timestamp
            .iter()
            .map(|field| {
                Expr::Column(Column {
                    relation: field.qualifier().cloned(),
                    name: field.name().to_string(),
                    spans: Spans::default(),
                })
            })
            .collect::<Vec<_>>();
        // add a _timestamp field to the schema
        schema_with_timestamp.push(timestamp_fields[0].clone());

        let output_schema = Arc::new(schema_from_df_fields_with_metadata(
            &schema_with_timestamp,
            schema.metadata().clone(),
        )?);
        // then take a max of the two timestamp columns
        let left_field = &timestamp_fields[0];
        let left_column = Expr::Column(Column {
            relation: left_field.qualifier().cloned(),
            name: left_field.name().to_string(),
            spans: Spans::default(),
        });
        let right_field = &timestamp_fields[1];
        let right_column = Expr::Column(Column {
            relation: right_field.qualifier().cloned(),
            name: right_field.name().to_string(),
            spans: Spans::default(),
        });
        let max_timestamp = Expr::Case(Case {
            expr: Some(Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left_column.clone()),
                op: logical_expr::Operator::GtEq,
                right: Box::new(right_column.clone()),
            }))),
            when_then_expr: vec![
                (
                    Box::new(Expr::Literal(ScalarValue::Boolean(Some(true)), None)),
                    Box::new(left_column.clone()),
                ),
                (
                    Box::new(Expr::Literal(ScalarValue::Boolean(Some(false)), None)),
                    Box::new(right_column.clone()),
                ),
            ],
            else_expr: Some(Box::new(coalesce(vec![
                left_column.clone(),
                right_column.clone(),
            ]))),
        });

        projection_expr.push(Expr::Alias(Alias {
            expr: Box::new(max_timestamp),
            relation: timestamp_fields[0].qualifier().cloned(),
            name: timestamp_fields[0].name().to_string(),
            metadata: None,
        }));
        Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
            projection_expr,
            Arc::new(input),
            output_schema.clone(),
        )?))
    }
}

#[derive(Default)]
struct FindLookupExtension {
    table: Option<ConnectorTable>,
    filter: Option<Expr>,
    alias: Option<TableReference>,
}

impl TreeNodeVisitor<'_> for FindLookupExtension {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> Result<TreeNodeRecursion> {
        match node {
            LogicalPlan::Extension(e) => {
                if let Some(s) = e.node.as_any().downcast_ref::<LookupSource>() {
                    self.table = Some(s.table.clone());
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
            LogicalPlan::Filter(filter) => {
                if self.filter.replace(filter.predicate.clone()).is_some() {
                    return plan_err!(
                        "multiple filters found in lookup join, which is not supported"
                    );
                }
            }
            LogicalPlan::SubqueryAlias(s) => {
                self.alias = Some(s.alias.clone());
            }
            _ => {
                return plan_err!("lookup tables must be used directly within a join");
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

fn has_lookup(plan: &LogicalPlan) -> Result<bool> {
    plan.exists(|p| {
        Ok(match p {
            LogicalPlan::Extension(e) => e.node.as_any().is::<LookupSource>(),
            _ => false,
        })
    })
}

fn maybe_plan_lookup_join(join: &Join) -> Result<Option<LogicalPlan>> {
    if has_lookup(&join.left)? {
        return plan_err!("lookup sources must be on the right side of an inner or left join");
    }

    if !has_lookup(&join.right)? {
        return Ok(None);
    }

    match join.join_type {
        JoinType::Inner | JoinType::Left => {}
        t => {
            return plan_err!(
                "{} join is not supported for lookup tables; must be a left or inner join",
                t
            );
        }
    }

    if join.filter.is_some() {
        return plan_err!(
            "filter join conditions are not supported for lookup joins; must have an equality condition"
        );
    }

    let mut lookup = FindLookupExtension::default();
    join.right.visit(&mut lookup)?;

    let connector = lookup
        .table
        .expect("right side of join does not have lookup");

    let on = join.on.iter().map(|(l, r)| {
        match r {
            Expr::Column(c) => {
                if !connector.primary_keys.contains(&c.name) {
                    plan_err!("the right-side of a look-up join condition must be a PRIMARY KEY column, but '{}' is not", c.name)
                } else {
                    Ok((l.clone(), c.clone()))
                }
            },
            e => {
                plan_err!("invalid right-side condition for lookup join: `{}`; only column references are supported", 
                expr_to_sql(e).map(|e| e.to_string()).unwrap_or_else(|_| e.to_string()))
            }
        }
    }).collect::<Result<_>>()?;

    let left_input = JoinRewriter::create_join_key_plan(
        join.left.clone(),
        join.on.iter().map(|(l, _)| l.clone()).collect(),
        "left",
    )?;

    Ok(Some(LogicalPlan::Extension(Extension {
        node: Arc::new(LookupJoin {
            input: left_input,
            schema: add_timestamp_field(join.schema.clone(), None)?,
            connector,
            on,
            filter: lookup.filter,
            alias: lookup.alias,
            join_type: join.join_type,
        }),
    })))
}

impl TreeNodeRewriter for JoinRewriter<'_> {
    type Node = LogicalPlan;

    fn f_up(&mut self, node: Self::Node) -> Result<Transformed<Self::Node>> {
        let LogicalPlan::Join(join) = node else {
            return Ok(Transformed::no(node));
        };

        if let Some(plan) = maybe_plan_lookup_join(&join)? {
            return Ok(Transformed::yes(plan));
        }

        // Detect and consume an ASOF marker (`_arroyo_asof(left_ts, right_ts)`)
        // injected by the AST pre-pass in `crate::asof`. If found, capture the
        // timestamp expressions and remove the marker from the join's filter.
        let (asof_marker, filter_without_marker) = take_asof_marker(join.filter.clone())?;

        let is_asof = asof_marker.is_some();
        if is_asof {
            check_asof_join(&join)?;
        }

        let is_instant = if is_asof {
            false
        } else {
            Self::check_join_windowing(&join)?
        };

        let Join {
            left,
            right,
            on,
            filter: _,
            join_type,
            join_constraint: JoinConstraint::On,
            schema: _,
            null_equals_null: false,
        } = join
        else {
            return not_impl_err!("can't handle join constraint other than ON");
        };
        Self::check_updating(&left, &right)?;

        if on.is_empty() && !is_instant && !is_asof {
            return not_impl_err!("Updating joins must include an equijoin condition");
        }

        let mut left_expressions = on.iter().map(|(l, _)| l.clone()).collect::<Vec<_>>();
        let mut right_expressions = on.iter().map(|(_, r)| r.clone()).collect::<Vec<_>>();

        if is_asof {
            let null_safe_keys =
                extract_null_safe_join_keys(join.filter.as_ref(), left.schema(), right.schema())?;
            left_expressions.extend(null_safe_keys.iter().map(|(l, _)| l.clone()));
            right_expressions.extend(null_safe_keys.into_iter().map(|(_, r)| r));
        }

        // For ASOF, locate the timestamp column indices in the pre-key-calc
        // (unkeyed) input schemas. The marker args reference columns from
        // these schemas, and `unkeyed_batch()` at runtime exposes columns in
        // the same order.
        let asof = if let Some((left_ts_expr, right_ts_expr, inequality)) = asof_marker {
            let left_idx = column_index(left.schema(), &left_ts_expr, "left")?;
            let right_idx = column_index(right.schema(), &right_ts_expr, "right")?;
            Some(AsofConfig {
                left_ts_index: left_idx as u32,
                right_ts_index: right_idx as u32,
                inequality,
                left_outer: join_type == JoinType::Left,
            })
        } else {
            None
        };

        let left_input = Self::create_join_key_plan(left, left_expressions, "left")?;
        let right_input = Self::create_join_key_plan(right, right_expressions, "right")?;

        let rewritten_join = LogicalPlan::Join(Join {
            schema: Arc::new(build_join_schema(
                left_input.schema(),
                right_input.schema(),
                &join_type,
            )?),
            left: Arc::new(left_input),
            right: Arc::new(right_input),
            on,
            join_type,
            join_constraint: JoinConstraint::On,
            null_equals_null: false,
            filter: filter_without_marker,
        });

        let final_logical_plan = self.post_join_timestamp_projection(rewritten_join)?;

        let join_extension = JoinExtension {
            rewritten_join: final_logical_plan,
            is_instant,
            // both ASOF and updating joins use the keyed-by-time TTL state
            ttl: (!is_instant).then_some(self.schema_provider.planning_options.ttl),
            asof,
        };

        Ok(Transformed::yes(LogicalPlan::Extension(Extension {
            node: Arc::new(join_extension),
        })))
    }
}

fn extract_null_safe_join_keys(
    filter: Option<&Expr>,
    left_schema: &DFSchema,
    right_schema: &DFSchema,
) -> Result<Vec<(Expr, Expr)>> {
    let Some(filter) = filter else {
        return Ok(vec![]);
    };

    let mut keys = vec![];
    for expr in split_conjunction_owned(filter.clone()) {
        let Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::IsNotDistinctFrom,
            right,
        }) = expr
        else {
            continue;
        };

        let Some((left_expr, right_expr)) =
            find_valid_equijoin_key_pair(&left, &right, left_schema, right_schema)?
        else {
            continue;
        };

        if !can_hash(&left_expr.get_type(left_schema)?)
            || !can_hash(&right_expr.get_type(right_schema)?)
        {
            return plan_err!("ASOF JOIN IS NOT DISTINCT FROM keys must be hashable on both sides");
        }

        keys.push((
            Expr::IsNull(Box::new(left_expr.clone())),
            Expr::IsNull(Box::new(right_expr.clone())),
        ));
        keys.push((
            coalesce(vec![
                cast(left_expr, DataType::Utf8),
                Expr::Literal(ScalarValue::Utf8(Some(String::new())), None),
            ]),
            coalesce(vec![
                cast(right_expr, DataType::Utf8),
                Expr::Literal(ScalarValue::Utf8(Some(String::new())), None),
            ]),
        ));
    }

    Ok(keys)
}

type AsofExtractedFilter = (Option<(Expr, Expr, AsofInequality)>, Option<Expr>);

/// If `filter` contains exactly one `_arroyo_asof(left_ts, right_ts)` call,
/// extract the two argument expressions, recover the associated inequality
/// operator from the filter, and return the filter with the marker stripped out
/// (replaced by `TRUE`, then constant-folded).
fn take_asof_marker(filter: Option<Expr>) -> Result<AsofExtractedFilter> {
    let Some(filter) = filter else {
        return Ok((None, None));
    };

    let mut found: Option<(Expr, Expr)> = None;
    let transformed = filter.clone().transform_up(&mut |e: Expr| {
        if let Expr::ScalarFunction(ScalarFunction { func, args }) = &e
            && func.name() == ASOF_MARKER_UDF
        {
            if found.is_some() {
                return Err(DataFusionError::Plan(
                    "multiple ASOF markers in a single join are not supported".to_string(),
                ));
            }
            if args.len() != 2 {
                return Err(DataFusionError::Plan(format!(
                    "{ASOF_MARKER_UDF} marker must have exactly 2 arguments"
                )));
            }
            found = Some((args[0].clone(), args[1].clone()));
            return Ok(Transformed::yes(Expr::Literal(
                ScalarValue::Boolean(Some(true)),
                None,
            )));
        }
        Ok(Transformed::no(e))
    })?;

    let inequality = if let Some((left, right)) = &found {
        Some(find_asof_inequality(&filter, left, right)?)
    } else {
        None
    };

    let found = match (found, inequality) {
        (Some((left, right)), Some(inequality)) => Some((left, right, inequality)),
        _ => None,
    };

    let stripped = simplify_trivial_and(transformed.data);
    Ok((found, stripped))
}

/// Drops `AND TRUE` / `TRUE AND` and reduces a top-level `Literal(true)` filter
/// to `None`, leaving any remaining predicates untouched.
fn simplify_trivial_and(expr: Expr) -> Option<Expr> {
    match expr {
        Expr::Literal(ScalarValue::Boolean(Some(true)), _) => None,
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => match (simplify_trivial_and(*left), simplify_trivial_and(*right)) {
            (None, None) => None,
            (Some(l), None) => Some(l),
            (None, Some(r)) => Some(r),
            (Some(l), Some(r)) => Some(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(l),
                op: Operator::And,
                right: Box::new(r),
            })),
        },
        other => Some(other),
    }
}

fn find_asof_inequality(filter: &Expr, left: &Expr, right: &Expr) -> Result<AsofInequality> {
    let mut found = None;
    filter.apply(|expr| {
        if let Expr::BinaryExpr(BinaryExpr {
            left: l,
            op,
            right: r,
        }) = expr
            && **l == *left
            && **r == *right
        {
            let inequality = match op {
                Operator::GtEq => Some(AsofInequality::Gte),
                Operator::Gt => Some(AsofInequality::Gt),
                Operator::LtEq => Some(AsofInequality::Lte),
                Operator::Lt => Some(AsofInequality::Lt),
                _ => None,
            };
            if let Some(inequality) = inequality
                && found.replace(inequality).is_some()
            {
                return Err(DataFusionError::Plan(
                    "multiple ASOF inequalities in a single join are not supported".to_string(),
                ));
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;

    found.ok_or_else(|| {
        DataFusionError::Plan(
            "ASOF JOIN marker was present but matching inequality could not be recovered"
                .to_string(),
        )
    })
}

/// Validate ASOF join restrictions: inner/left join, no windows, non-updating.
fn check_asof_join(join: &Join) -> Result<()> {
    if join.join_type != JoinType::Inner && join.join_type != JoinType::Left {
        return plan_err!("ASOF JOIN only supports INNER and LEFT joins");
    }
    if WindowDetectingVisitor::get_window(&join.left)?.is_some()
        || WindowDetectingVisitor::get_window(&join.right)?.is_some()
    {
        return plan_err!("ASOF JOIN does not support windowed inputs");
    }
    Ok(())
}

/// Resolve a marker argument to a column index in the given schema.
fn column_index(schema: &DFSchema, expr: &Expr, side: &str) -> Result<usize> {
    let Expr::Column(col) = expr else {
        return plan_err!(
            "ASOF JOIN MATCH_CONDITION arguments must be column references; got `{expr}` for the {side} side"
        );
    };

    schema.index_of_column(col).map_err(|_| {
        DataFusionError::Plan(format!(
            "ASOF JOIN {side} timestamp column `{col}` not found in {side} input schema"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion::common::Column;

    fn join_schema(relation: &str) -> DFSchema {
        let fields = vec![
            Field::new("symbol", DataType::Utf8, true),
            Field::new(
                "_timestamp",
                DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
                true,
            ),
        ];
        DFSchema::from_unqualified_fields(fields.into(), std::collections::HashMap::new())
            .unwrap()
            .replace_qualifier(relation)
    }

    #[test]
    fn extracts_null_safe_join_keys_for_is_not_distinct_from() {
        let left_schema = join_schema("left");
        let right_schema = join_schema("right");
        let filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new(Some("left"), "symbol"))),
            op: Operator::IsNotDistinctFrom,
            right: Box::new(Expr::Column(Column::new(Some("right"), "symbol"))),
        });

        let keys = extract_null_safe_join_keys(Some(&filter), &left_schema, &right_schema).unwrap();
        assert_eq!(keys.len(), 2);
        assert!(matches!(keys[0].0, Expr::IsNull(_)));
        assert!(matches!(keys[0].1, Expr::IsNull(_)));
        assert!(format!("{}", keys[1].0).contains("Utf8"));
        assert!(format!("{}", keys[1].1).contains("Utf8"));
    }
}
