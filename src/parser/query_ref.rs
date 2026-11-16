//! Allocation-free query views at the statement/query dispatch boundary.
use super::{CteDef, QueryExpr, RrfFusion, Select, SetOperation, Statement};

#[derive(Debug, Clone, Copy)]
pub enum QueryRef<'a> {
    Select(&'a Select),
    SetOperation(&'a SetOperation),
    Rrf(&'a RrfFusion),
    WithCte(WithRef<'a>),
    Other(&'a Statement),
}

#[derive(Clone, Copy)]
pub struct WithRef<'a> {
    pub ctes: &'a [CteDef],
    pub recursive: bool,
    pub body: QueryBodyRef<'a>,
}

impl std::fmt::Debug for WithRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WithCte")
            .field("ctes", &self.ctes)
            .field("recursive", &self.recursive)
            .field("body", &self.body.as_ref())
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
pub enum QueryBodyRef<'a> {
    Statement(&'a Statement),
    Query(&'a QueryExpr),
}
impl<'a> QueryBodyRef<'a> {
    pub fn as_ref(self) -> QueryRef<'a> {
        match self {
            Self::Statement(statement) => statement.into(),
            Self::Query(query) => query.into(),
        }
    }
}
impl<'a> From<&'a Statement> for QueryRef<'a> {
    fn from(statement: &'a Statement) -> Self {
        match statement {
            Statement::Select(select) => Self::Select(select),
            Statement::SetOperation(set) => Self::SetOperation(set),
            Statement::WithCte(with) => Self::WithCte(WithRef {
                ctes: &with.ctes,
                recursive: with.recursive,
                body: QueryBodyRef::Statement(&with.body),
            }),
            _ => Self::Other(statement),
        }
    }
}
impl<'a> From<&'a QueryExpr> for QueryRef<'a> {
    fn from(query: &'a QueryExpr) -> Self {
        match query {
            QueryExpr::Select(select) => Self::Select(select),
            QueryExpr::SetOp(set) => Self::SetOperation(set),
            QueryExpr::Rrf(rrf) => Self::Rrf(rrf),
            QueryExpr::WithCte(with) => Self::WithCte(WithRef {
                ctes: &with.ctes,
                recursive: with.recursive,
                body: QueryBodyRef::Query(&with.body),
            }),
        }
    }
}
impl<'a> From<&'a Select> for QueryRef<'a> {
    fn from(select: &'a Select) -> Self {
        Self::Select(select)
    }
}

impl<'a> From<&'a SetOperation> for QueryRef<'a> {
    fn from(set: &'a SetOperation) -> Self {
        Self::SetOperation(set)
    }
}

impl<'a> From<&'a Box<SetOperation>> for QueryRef<'a> {
    fn from(set: &'a Box<SetOperation>) -> Self {
        set.as_ref().into()
    }
}
impl<'a> From<&'a Box<Statement>> for QueryRef<'a> {
    fn from(statement: &'a Box<Statement>) -> Self {
        statement.as_ref().into()
    }
}
impl<'a> From<&QueryBodyRef<'a>> for QueryRef<'a> {
    fn from(body: &QueryBodyRef<'a>) -> Self {
        body.as_ref()
    }
}

impl<'a> From<&'a mut Statement> for QueryRef<'a> {
    fn from(statement: &'a mut Statement) -> Self {
        (&*statement).into()
    }
}
impl<'a> From<&'a mut QueryExpr> for QueryRef<'a> {
    fn from(query: &'a mut QueryExpr) -> Self {
        (&*query).into()
    }
}

/// Mutable views expose components, while query slots keep QueryExpr owners.
pub(crate) enum QueryMut<'a> {
    Select(&'a mut Select),
    SetOperation(&'a mut SetOperation),
    Rrf(&'a mut RrfFusion),
    WithCte(WithMut<'a>),
    Other(&'a mut Statement),
}
pub(crate) struct WithMut<'a> {
    pub ctes: &'a mut Vec<CteDef>,
    pub recursive: bool,
    pub body: QueryBodyMut<'a>,
}
pub(crate) enum QueryBodyMut<'a> {
    Statement(&'a mut Statement),
    Query(&'a mut QueryExpr),
}
impl QueryBodyMut<'_> {
    pub fn as_mut(&mut self) -> QueryMut<'_> {
        match self {
            Self::Statement(statement) => (&mut **statement).into(),
            Self::Query(query) => (&mut **query).into(),
        }
    }
}
impl<'a> From<&'a mut Statement> for QueryMut<'a> {
    fn from(statement: &'a mut Statement) -> Self {
        match statement {
            Statement::Select(select) => Self::Select(select),
            Statement::SetOperation(set) => Self::SetOperation(set),
            Statement::WithCte(with) => Self::WithCte(WithMut {
                ctes: &mut with.ctes,
                recursive: with.recursive,
                body: QueryBodyMut::Statement(&mut with.body),
            }),
            _ => Self::Other(statement),
        }
    }
}
impl<'a> From<&'a mut QueryExpr> for QueryMut<'a> {
    fn from(query: &'a mut QueryExpr) -> Self {
        match query {
            QueryExpr::Select(select) => Self::Select(select),
            QueryExpr::SetOp(set) => Self::SetOperation(set),
            QueryExpr::Rrf(rrf) => Self::Rrf(rrf),
            QueryExpr::WithCte(with) => Self::WithCte(WithMut {
                ctes: &mut with.ctes,
                recursive: with.recursive,
                body: QueryBodyMut::Query(&mut with.body),
            }),
        }
    }
}

impl<'a> From<&'a mut Box<Statement>> for QueryMut<'a> {
    fn from(statement: &'a mut Box<Statement>) -> Self {
        statement.as_mut().into()
    }
}
impl<'a> From<&'a mut SetOperation> for QueryMut<'a> {
    fn from(set: &'a mut SetOperation) -> Self {
        Self::SetOperation(set)
    }
}
impl<'a> From<&'a mut Box<SetOperation>> for QueryMut<'a> {
    fn from(set: &'a mut Box<SetOperation>) -> Self {
        set.as_mut().into()
    }
}
impl<'a> From<&'a mut QueryBodyMut<'_>> for QueryMut<'a> {
    fn from(body: &'a mut QueryBodyMut<'_>) -> Self {
        body.as_mut()
    }
}

impl<'a> From<&'a std::sync::Arc<Statement>> for QueryRef<'a> {
    fn from(statement: &'a std::sync::Arc<Statement>) -> Self {
        statement.as_ref().into()
    }
}

impl<'a> QueryRef<'a> {
    pub fn leftmost_select(mut self) -> Option<&'a Select> {
        loop {
            match self {
                Self::Select(select) => return Some(select),
                Self::SetOperation(set) => self = (&set.left).into(),
                Self::Rrf(rrf) => self = (&rrf.branches.first()?.query).into(),
                Self::WithCte(with) => self = with.body.as_ref(),
                Self::Other(_) => return None,
            }
        }
    }
    pub fn to_owned_query(self) -> Option<QueryExpr> {
        match self {
            Self::Select(select) => Some(select.clone().into()),
            Self::SetOperation(set) => Some(set.clone().into()),
            Self::Rrf(rrf) => Some(QueryExpr::Rrf(Box::new(rrf.clone()))),
            Self::WithCte(with) => Some(QueryExpr::WithCte(Box::new(super::QueryWithCte {
                ctes: with.ctes.to_vec(),
                recursive: with.recursive,
                body: with.body.as_ref().to_owned_query()?,
            }))),
            Self::Other(_) => None,
        }
    }
}
impl QueryMut<'_> {
    pub fn as_mut(&mut self) -> QueryMut<'_> {
        match self {
            Self::Select(select) => QueryMut::Select(select),
            Self::SetOperation(set) => QueryMut::SetOperation(set),
            Self::Rrf(rrf) => QueryMut::Rrf(rrf),
            Self::WithCte(with) => QueryMut::WithCte(WithMut {
                ctes: with.ctes,
                recursive: with.recursive,
                body: match &mut with.body {
                    QueryBodyMut::Statement(statement) => QueryBodyMut::Statement(statement),
                    QueryBodyMut::Query(query) => QueryBodyMut::Query(query),
                },
            }),
            Self::Other(statement) => QueryMut::Other(statement),
        }
    }

    pub fn as_ref(&self) -> QueryRef<'_> {
        match self {
            Self::Select(select) => QueryRef::Select(select),
            Self::SetOperation(set) => QueryRef::SetOperation(set),
            Self::Rrf(rrf) => QueryRef::Rrf(rrf),
            Self::WithCte(with) => QueryRef::WithCte(WithRef {
                ctes: with.ctes,
                recursive: with.recursive,
                body: match &with.body {
                    QueryBodyMut::Statement(statement) => QueryBodyRef::Statement(statement),
                    QueryBodyMut::Query(query) => QueryBodyRef::Query(query),
                },
            }),
            Self::Other(statement) => QueryRef::Other(statement),
        }
    }
}

impl<'a> From<QueryMut<'a>> for QueryRef<'a> {
    fn from(query: QueryMut<'a>) -> Self {
        match query {
            QueryMut::Select(select) => Self::Select(select),
            QueryMut::SetOperation(set) => Self::SetOperation(set),
            QueryMut::Rrf(rrf) => Self::Rrf(rrf),
            QueryMut::WithCte(with) => Self::WithCte(WithRef {
                ctes: with.ctes,
                recursive: with.recursive,
                body: match with.body {
                    QueryBodyMut::Statement(statement) => QueryBodyRef::Statement(statement),
                    QueryBodyMut::Query(query) => QueryBodyRef::Query(query),
                },
            }),
            QueryMut::Other(statement) => Self::Other(statement),
        }
    }
}
