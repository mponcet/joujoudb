use std::borrow::Cow;

#[derive(Debug)]
pub enum Stmt<'source> {
    Select {
        distinct: bool,
        columns: Vec<Expr<'source>>,
        from: Option<Vec<From<'source>>>,
        r#where: Option<Where<'source>>,
        // group_by: Option<String>,
        // having: Option<String>,
        // window: Option<String>,
    },
}

impl<'source> std::fmt::Display for Stmt<'source> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Stmt::Select {
                distinct,
                columns,
                from,
                r#where,
            } => {
                if *distinct {
                    write!(f, "SELECT DISTINCT ")?;
                } else {
                    write!(f, "SELECT ")?;
                }

                for (i, col) in columns.iter().enumerate() {
                    let comma = if i < columns.len() - 1 { "," } else { "" };
                    write!(f, "{col}{comma}")?;
                }

                if let Some(from) = from {
                    write!(f, " FROM ")?;
                    for (i, table) in from.iter().enumerate() {
                        let comma = if i < from.len() - 1 { "," } else { "" };
                        write!(f, "{table}{comma}")?;
                    }
                }
                if let Some(r#where) = r#where {
                    write!(f, " WHERE {}", r#where)?;
                }

                Ok(())
            }
        }
    }
}

#[derive(Debug)]
pub struct From<'source> {
    pub table: Cow<'source, str>,
}

impl<'source> std::fmt::Display for From<'source> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.table)
    }
}

#[derive(Debug)]
pub struct Where<'source> {
    pub expr: Expr<'source>,
}

impl<'source> std::fmt::Display for Where<'source> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.expr)
    }
}

#[derive(Debug)]
pub enum Expr<'source> {
    // All columns.
    All,
    // Column name and if specified, a table name.
    Column {
        table: Option<Cow<'source, str>>,
        name: Cow<'source, str>,
    },
    // A literal.
    Literal(Literal<'source>),
    // An operator (arithmetic expressions and more).
    Operator(Operator<'source>),
}

impl<'source> std::fmt::Display for Expr<'source> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::All => write!(f, "*"),
            Expr::Column { table, name } => {
                if let Some(table) = table {
                    write!(f, "{table}.{name}")
                } else {
                    write!(f, "{name}")
                }
            }
            Expr::Literal(literal) => write!(f, "{literal}"),
            Expr::Operator(operator) => write!(f, "{operator}"),
        }
    }
}

#[derive(Debug)]
pub enum Operator<'source> {
    Plus(Box<Expr<'source>>, Box<Expr<'source>>),
    Minus(Box<Expr<'source>>, Box<Expr<'source>>),
    Mul(Box<Expr<'source>>, Box<Expr<'source>>),
    Div(Box<Expr<'source>>, Box<Expr<'source>>),
    Or(Box<Expr<'source>>, Box<Expr<'source>>),
    And(Box<Expr<'source>>, Box<Expr<'source>>),
    Equal(Box<Expr<'source>>, Box<Expr<'source>>),
    NotEqual(Box<Expr<'source>>, Box<Expr<'source>>),
    Less(Box<Expr<'source>>, Box<Expr<'source>>),
    LessEqual(Box<Expr<'source>>, Box<Expr<'source>>),
    Greater(Box<Expr<'source>>, Box<Expr<'source>>),
    GreaterEqual(Box<Expr<'source>>, Box<Expr<'source>>),

    // Unary
    Identity(Box<Expr<'source>>),
    Negate(Box<Expr<'source>>),
}

impl<'source> std::fmt::Display for Operator<'source> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operator::Plus(lhs, rhs) => write!(f, "{lhs} + {rhs}"),
            Operator::Minus(lhs, rhs) => write!(f, "{lhs} - {rhs}"),
            Operator::Mul(lhs, rhs) => write!(f, "{lhs} * {rhs}"),
            Operator::Div(lhs, rhs) => write!(f, "{lhs} / {rhs}"),
            Operator::Or(lhs, rhs) => write!(f, "{lhs} OR {rhs}"),
            Operator::And(lhs, rhs) => write!(f, "{lhs} AND {rhs}"),
            Operator::Identity(expr) => write!(f, "{expr}"),
            Operator::Negate(expr) => write!(f, "-{expr}"),
            Operator::Equal(lhs, rhs) => write!(f, "{lhs} = {rhs}"),
            Operator::NotEqual(lhs, rhs) => write!(f, "{lhs} != {rhs}"),
            Operator::Less(lhs, rhs) => write!(f, "{lhs} < {rhs}"),
            Operator::LessEqual(lhs, rhs) => write!(f, "{lhs} <= {rhs}"),
            Operator::Greater(lhs, rhs) => write!(f, "{lhs} > {rhs}"),
            Operator::GreaterEqual(lhs, rhs) => write!(f, "{lhs} >= {rhs}"),
        }
    }
}

#[derive(Debug)]
pub enum Literal<'source> {
    Ident(Cow<'source, str>),
    String(Cow<'source, str>),
    Boolean(bool),
    Integer(i64),
    Float(f64),
}

impl<'source> std::fmt::Display for Literal<'source> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Literal::Ident(cow) => write!(f, "{}", cow),
            Literal::String(cow) => write!(f, "'{}'", cow),
            Literal::Boolean(b) => write!(f, "{}", b),
            Literal::Integer(i) => write!(f, "{}", i),
            Literal::Float(fl) => write!(f, "{}", fl),
        }
    }
}
