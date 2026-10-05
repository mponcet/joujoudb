use std::borrow::Cow;

use crate::sql::parser::ast;
use crate::sql::schema::{self, Schema};

// SELECT id, first_name, salary/12 AS monthly_salary
// FROM employee
// WHERE state = 'CO' AND monthly_salary > 1000
//
// Projection: #id, #first_name, #monthly_salary
//   Filter: #state = 'CO' AND #monthly_salary > 1000
//     Projection: #id, #first_name, #salary/12 AS monthly_salary, #state
//       Scan: employee

enum LogicalExpr<'source> {
    // All columns.
    All,
    // Column name and if specified, a table name.
    Column {
        table: Option<Cow<'source, str>>,
        name: Cow<'source, str>,
    },
    // A literal.
    // Literal(Literal<'source>),
    // An operator (arithmetic expressions and more).
    // Operator(Operator<'source>),
}

struct Projection<'a> {
    columns: Vec<ast::Expr<'a>>,
}

struct Filter<'a> {
    expr: ast::Expr<'a>,
}

struct Scan;

impl<'source> TryFrom<ast::Expr<'_>> for LogicalExpr<'source> {
    type Error = &'static str;

    fn try_from(value: ast::Expr) -> Result<Self, Self::Error> {
        match value {
            ast::Expr::All => todo!(),
            ast::Expr::Column { table, name } => todo!(),
            ast::Expr::Literal(literal) => todo!(),
            ast::Expr::Operator(operator) => match operator {
                ast::Operator::Plus(expr, expr1) => todo!(),
                ast::Operator::Minus(expr, expr1) => todo!(),
                ast::Operator::Mul(expr, expr1) => todo!(),
                ast::Operator::Div(expr, expr1) => todo!(),
                ast::Operator::Or(expr, expr1) => todo!(),
                ast::Operator::And(expr, expr1) => todo!(),
                ast::Operator::Equal(expr, expr1) => todo!(),
                ast::Operator::NotEqual(expr, expr1) => todo!(),
                ast::Operator::Less(expr, expr1) => todo!(),
                ast::Operator::LessEqual(expr, expr1) => todo!(),
                ast::Operator::Greater(expr, expr1) => todo!(),
                ast::Operator::GreaterEqual(expr, expr1) => todo!(),
                ast::Operator::Identity(expr) => todo!(),
                ast::Operator::Negate(expr) => todo!(),
            },
        }
    }
}

enum LogicalPlan {
    Projection {
        schema: Schema,
        children: Option<Box<LogicalPlan>>,
    },
    Filter {
        schema: Schema,
        children: Option<Box<LogicalPlan>>,
    },
    Scan {
        schema: Schema,
    },
}

fn logical_plan(ast: ast::Stmt) -> LogicalPlan {
    match ast {
        ast::Stmt::Select {
            distinct,
            columns,
            from,
            r#where,
        } => {
            // TODO: load schema from table
            LogicalPlan::Scan { schema: todo!() }
        }
    }
}
