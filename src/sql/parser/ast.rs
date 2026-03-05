use std::borrow::Cow;

#[derive(Debug)]
pub enum Stmt<'source> {
    Select {
        distinct: bool,
        columns: Vec<Expression<'source>>,
        from: Vec<From<'source>>,
        // r#where: Option<String>,
        // group_by: Option<String>,
        // having: Option<String>,
        // window: Option<String>,
    },
}

// #[derive(Debug)]
// pub enum Column<'source> {
//     Asterisk,
//     Name(&'source str),
//     Expression,
// }

#[derive(Debug)]
pub struct From<'source>(pub &'source str);

#[derive(Debug)]
pub enum Expression<'source> {
    // All columns.
    All,
    // Column name and if specified, a table name.
    Column {
        table: Option<&'source str>,
        name: &'source str,
    },
    // A literal.
    Literal(Literal<'source>),
    // An operator (arithmetic expressions and more).
    Operator(Operator<'source>),
}

#[derive(Debug)]
pub enum Operator<'source> {
    Plus(Box<Expression<'source>>, Box<Expression<'source>>),
    Minus(Box<Expression<'source>>, Box<Expression<'source>>),
    Mul(Box<Expression<'source>>, Box<Expression<'source>>),
    Div(Box<Expression<'source>>, Box<Expression<'source>>),

    // Unary
    Identity(Box<Expression<'source>>),
    Negate(Box<Expression<'source>>),
}

#[derive(Debug)]
pub enum Literal<'source> {
    Ident(Cow<'source, str>),
    String(Cow<'source, str>),
    Boolean(bool),
    Integer(i64),
    Float(f64),
}
