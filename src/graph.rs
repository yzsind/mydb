//! The GQL syntax and graph namespace shared by the parser and executor.

use crate::parser::Expr;
use serde::{Deserialize, Serialize};

mod binding;
mod syntax;
pub(crate) use syntax::parse;

pub(crate) const TABLE_PREFIX: &str = "__mydb_graph_";
pub(crate) const STORAGE_TYPE: &str = "GRAPH STORAGE";
pub(crate) const MAX_HOPS: u8 = 16;

pub(crate) fn is_storage_table(name: &str) -> bool {
    name.get(..TABLE_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(TABLE_PREFIX))
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Command {
    Create {
        graph: String,
        conditional: bool,
    },
    Drop {
        graph: String,
        conditional: bool,
    },
    SetGraph(String),
    ResetGraph,
    CreateIndex {
        graph: String,
        name: String,
        spec: PropertyIndex,
    },
    DropIndex {
        graph: String,
        name: String,
    },
    Program(Program),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PropertyIndex {
    pub label: String,
    pub property: String,
    pub kind: IndexType,
    pub unique: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum IndexType {
    String,
    Int64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Program {
    // SQL atoms are retained in source order for normal protocol parameter binding.
    pub atoms: Vec<Expr>,
    pub clauses: Vec<Clause>,
    pub result: Option<Projection>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Clause {
    For(String, Vec<Expression>),
    Match {
        optional: bool,
        patterns: Vec<Pattern>,
        predicate: Option<Expression>,
    },
    Insert(Vec<Pattern>),
    Set(Vec<Change>),
    Remove(Vec<Change>),
    Delete {
        detach: bool,
        variables: Vec<String>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Projection {
    pub distinct: bool,
    pub items: Vec<(Expression, String)>,
    pub order: Vec<(Expression, bool)>,
    pub limit: Option<Expression>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Change {
    Property {
        variable: String,
        key: String,
        value: Option<Expression>,
    },
    Label {
        variable: String,
        label: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Pattern {
    pub path: Option<String>,
    pub mode: PathMode,
    pub shortest: bool,
    pub start: Element,
    pub steps: Vec<Step>,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum PathMode {
    Walk,
    Trail,
    Acyclic,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct Element {
    pub variable: Option<String>,
    pub labels: Vec<String>,
    pub properties: Vec<(String, Expression)>,
    pub predicate: Option<Expression>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Step {
    pub edge: Element,
    pub direction: Direction,
    pub lower: u8,
    pub upper: u8,
    pub quantified: bool,
    pub node: Element,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Direction {
    Out,
    In,
    Either,
    Undirected,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Expression {
    Atom(usize),
    Record(Vec<(String, Expression)>),
    Variable(String),
    Property(String, String),
    ElementId(String),
    PathLength(String),
    Cast(Box<Expression>, Cast),
    Not(Box<Expression>),
    IsNull(Box<Expression>, bool),
    Binary(Box<Expression>, Operator, Box<Expression>),
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Cast {
    String,
    Int64,
    Float64,
    Boolean,
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Operator {
    Or,
    And,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl Program {
    // The executor also requires a visible unique anchor and caps traversal
    // before attempting a read on a shared connection reactor.
    pub(crate) fn may_inline_read(&self) -> bool {
        let [Clause::Match {
            optional: false,
            patterns,
            predicate: None,
        }] = self.clauses.as_slice()
        else {
            return false;
        };
        let [pattern] = patterns.as_slice() else {
            return false;
        };
        let [step] = pattern.steps.as_slice() else {
            return false;
        };
        let [(_, Expression::Atom(_))] = pattern.start.properties.as_slice() else {
            return false;
        };
        let plain = |element: &Element| {
            element.labels.is_empty()
                && element.properties.is_empty()
                && element.predicate.is_none()
        };
        fn scalar(expr: &Expression) -> bool {
            match expr {
                Expression::Atom(_) | Expression::Property(..) | Expression::ElementId(_) => true,
                Expression::Cast(expr, _) => scalar(expr),
                _ => false,
            }
        }
        pattern.mode == PathMode::Walk
            && !pattern.shortest
            && pattern.path.is_none()
            && !pattern.start.labels.is_empty()
            && pattern.start.predicate.is_none()
            && step.upper <= 1
            && step.edge.variable.is_none()
            && plain(&step.edge)
            && plain(&step.node)
            && self.result.as_ref().is_some_and(|projection| {
                projection.items.len() <= 8
                    && projection.order.len() <= 8
                    && projection.items.iter().all(|(expr, _)| scalar(expr))
                    && projection.order.iter().all(|(expr, _)| {
                        scalar(expr)
                            || matches!(expr, Expression::Variable(name)
                            if projection.items.iter().any(|(_, alias)| alias == name))
                    })
                    && projection
                        .limit
                        .as_ref()
                        .is_none_or(|expr| matches!(expr, Expression::Atom(_)))
            })
    }

    pub(crate) fn writes(&self) -> bool {
        self.clauses
            .iter()
            .any(|clause| !matches!(clause, Clause::For(..) | Clause::Match { .. }))
    }
}

impl Command {
    pub(crate) fn ddl(&self) -> bool {
        matches!(
            self,
            Self::Create { .. }
                | Self::Drop { .. }
                | Self::CreateIndex { .. }
                | Self::DropIndex { .. }
        )
    }
    pub(crate) fn writes(&self) -> bool {
        self.ddl() || matches!(self, Self::Program(program) if program.writes())
    }
}
