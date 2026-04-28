use crate::builder::{NamedNode, Planner};
use crate::extension::{ArroyoExtension, NodeWithIncomingEdges};
use crate::multifield_partial_ord;
use arroyo_datastream::logical::{LogicalEdge, LogicalEdgeType, LogicalNode, OperatorName};
use arroyo_rpc::df::{ArroyoSchema, ArroyoSchemaRef};
use arroyo_rpc::grpc::api::JoinOperator;
use datafusion::common::{DFSchemaRef, Result, plan_err};
use datafusion::logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore};
use prost::Message;
use std::time::Duration;

pub(crate) const ASOF_JOIN_NODE_NAME: &str = "AsofJoinNode";

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AsofJoinExtension {
    pub(crate) left_input: LogicalPlan,
    pub(crate) right_input: LogicalPlan,
    pub(crate) schema: DFSchemaRef,
    pub(crate) left_time_index: usize,
    pub(crate) right_time_index: usize,
    pub(crate) left_gte_right: bool,
    pub(crate) ttl: Duration,
}

multifield_partial_ord!(
    AsofJoinExtension,
    left_input,
    right_input,
    left_time_index,
    right_time_index,
    left_gte_right,
    ttl
);

impl ArroyoExtension for AsofJoinExtension {
    fn node_name(&self) -> Option<NamedNode> {
        None
    }

    fn plan_node(
        &self,
        _planner: &Planner,
        index: usize,
        input_schemas: Vec<ArroyoSchemaRef>,
    ) -> Result<NodeWithIncomingEdges> {
        if input_schemas.len() != 2 {
            return plan_err!("asof join should have exactly two inputs");
        }

        let left_schema = input_schemas[0].clone();
        let right_schema = input_schemas[1].clone();

        let config = JoinOperator {
            name: format!("asof_join_{index}"),
            left_schema: Some(left_schema.as_ref().clone().into()),
            right_schema: Some(right_schema.as_ref().clone().into()),
            output_schema: Some(self.output_schema().into()),
            join_plan: vec![],
            ttl_micros: Some(self.ttl.as_micros() as u64),
            asof_left_time_index: Some(self.left_time_index as u32),
            asof_right_time_index: Some(self.right_time_index as u32),
            asof_left_gte_right: Some(self.left_gte_right),
        };

        let logical_node = LogicalNode::single(
            index as u32,
            format!("asof_join_{index}"),
            OperatorName::AsofJoin,
            config.encode_to_vec(),
            "asof_join".to_string(),
            1,
        );

        let left_edge =
            LogicalEdge::project_all(LogicalEdgeType::LeftJoin, left_schema.as_ref().clone());
        let right_edge =
            LogicalEdge::project_all(LogicalEdgeType::RightJoin, right_schema.as_ref().clone());

        Ok(NodeWithIncomingEdges {
            node: logical_node,
            edges: vec![left_edge, right_edge],
        })
    }

    fn output_schema(&self) -> ArroyoSchema {
        ArroyoSchema::from_schema_unkeyed(self.schema.inner().clone()).unwrap()
    }
}

impl UserDefinedLogicalNodeCore for AsofJoinExtension {
    fn name(&self) -> &str {
        ASOF_JOIN_NODE_NAME
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        vec![&self.left_input, &self.right_input]
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "AsofJoinExtension: {}", self.schema)
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        Ok(Self {
            left_input: inputs[0].clone(),
            right_input: inputs[1].clone(),
            schema: self.schema.clone(),
            left_time_index: self.left_time_index,
            right_time_index: self.right_time_index,
            left_gte_right: self.left_gte_right,
            ttl: self.ttl,
        })
    }
}
