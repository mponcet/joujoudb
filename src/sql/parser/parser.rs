use crate::sql::parser::ast::{self, Stmt};
use crate::sql::parser::lexer::{Keyword, Lexer, Token, TokenKind};

use std::iter::Peekable;

use miette::{Diagnostic, Result, SourceSpan, miette};
use thiserror::Error;

// A SQL recursive descent parser.
//
// Expressions are handled with a Pratt parser described here:
// https://matklad.github.io/2020/04/13/simple-but-powerful-pratt-parsing.html
//
// Right now, postfix operators are not supported.

#[derive(Error, Debug, Diagnostic)]
#[error("ParserError: {message}")]
pub struct ParserError {
    message: String,
    #[source_code]
    src: String,
    #[label("here")]
    err_span: SourceSpan,
}

pub struct Parser<'source> {
    tokens: Peekable<Lexer<'source>>,
    source: &'source str,
}

trait TokenKindExt {
    fn prefix_binding_power(&self) -> ((), u8);
    fn infix_binding_power(&self) -> Option<(u8, u8)>;
}

impl TokenKindExt for TokenKind<'_> {
    fn prefix_binding_power(&self) -> ((), u8) {
        match self {
            TokenKind::Plus | TokenKind::Minus => ((), 5),
            _ => panic!("not an operator: {self}"),
        }
    }

    fn infix_binding_power(&self) -> Option<(u8, u8)> {
        let bp = match self {
            TokenKind::Plus | TokenKind::Minus => (1, 2),
            TokenKind::Asterisk | TokenKind::Slash => (3, 4),
            _ => return None,
        };

        Some(bp)
    }
}

impl<'source> Parser<'source> {
    fn new(source: &'source str) -> Self {
        Self {
            tokens: Lexer::new(source).peekable(),
            source,
        }
    }

    fn peek(&mut self) -> Result<Option<&'_ Token<'source>>> {
        self.tokens
            .peek()
            .map(|token| {
                token.as_ref().map_err(|e| {
                    // Can't clone e: &Report, so we construct a new miette Report.
                    let labels = e
                        .labels()
                        .map(|l| l.collect::<Vec<_>>())
                        .unwrap_or(Vec::new());
                    miette!(labels = labels, "{e}").with_source_code(self.source.to_string())
                })
            })
            .transpose()
    }

    fn next(&mut self) -> Result<Option<Token<'source>>> {
        self.tokens.next().transpose()
    }

    fn next_if<P>(&mut self, predicate: P) -> Option<Token<'source>>
    where
        P: FnOnce(&TokenKind) -> bool,
    {
        self.tokens
            .next_if(|token| token.as_ref().is_ok_and(|token| predicate(&token.kind)))?
            .ok()
    }

    fn next_if_map<F, T>(&mut self, op: F) -> Option<T>
    where
        F: FnOnce(&Token) -> Option<T>,
    {
        let token = self.tokens.peek()?.as_ref().ok();
        let result = token.and_then(op)?;
        Some(result)
    }

    fn expect(&mut self, expected: TokenKind) -> Result<()> {
        match self.next()? {
            Some(token) if token.kind == expected => Ok(()),
            Some(token) => match token.kind {
                TokenKind::Eof => Err(ParserError {
                    message: format!("unexpected end of file, expected '{}'", expected),
                    src: self.source.to_string(),
                    err_span: token.offset.into(),
                })?,
                _ => Err(ParserError {
                    message: format!("expected '{}', found '{}'", expected, token.kind),
                    src: self.source.to_string(),
                    err_span: token.offset.into(),
                })?,
            },
            _ => unreachable!(),
        }
    }

    fn next_eq(&mut self, expected: TokenKind) -> bool {
        self.next_if(|kind| *kind == expected).is_some()
    }

    pub fn parse(source: &'source str) -> Result<Vec<Stmt<'source>>> {
        let mut parser = Parser::new(source);
        parser.parse_statement()
    }

    fn parse_expr(&mut self) -> Result<ast::Expression<'source>> {
        self.parse_expr_bp(0)
    }

    /// min_bp: minimal binding power to fold the expression.
    fn parse_expr_bp(&mut self, min_bp: u8) -> Result<ast::Expression<'source>> {
        let token = self.next()?.expect("should not happen");

        let mut lhs = match token.kind {
            TokenKind::Asterisk => ast::Expression::All,
            TokenKind::Ident(s) => ast::Expression::Column {
                // TODO: handle table name
                table: None,
                name: s,
            },
            TokenKind::Number(n) => {
                if n.find('.').is_some() {
                    ast::Expression::Literal(ast::Literal::Float(n.parse().map_err(|e| {
                        ParserError {
                            message: format!("{e}"),
                            src: self.source.to_string(),
                            err_span: token.offset.into(),
                        }
                    })?))
                } else {
                    ast::Expression::Literal(ast::Literal::Integer(n.parse().map_err(|e| {
                        ParserError {
                            message: format!("{e}"),
                            src: self.source.to_string(),
                            err_span: token.offset.into(),
                        }
                    })?))
                }
            }
            TokenKind::LeftParen => {
                let lhs = self.parse_expr_bp(0)?;
                self.expect(TokenKind::RightParen)?;
                lhs
            }
            TokenKind::Plus | TokenKind::Minus => {
                let (_, r_bp) = token.kind.prefix_binding_power();
                let rhs = self.parse_expr_bp(r_bp)?;
                let operator = match token.kind {
                    TokenKind::Plus => ast::Operator::Identity(Box::new(rhs)),
                    TokenKind::Minus => ast::Operator::Negate(Box::new(rhs)),
                    _ => unreachable!(),
                };
                ast::Expression::Operator(operator)
            }
            _ => {
                return Err(ParserError {
                    message: format!("unexpected token '{}'", token.kind),
                    src: self.source.to_string(),
                    err_span: token.offset.into(),
                })?;
            }
        };

        loop {
            let Some(next_token) = self.peek()? else {
                break;
            };
            let kind = match next_token.kind {
                TokenKind::Plus => TokenKind::Plus,
                TokenKind::Minus => TokenKind::Minus,
                TokenKind::Asterisk => TokenKind::Asterisk,
                TokenKind::Slash => TokenKind::Slash,
                TokenKind::RightParen => TokenKind::RightParen,
                TokenKind::Eof => break,
                _ => {
                    break;
                }
            };

            if let Some((l_bp, r_bp)) = kind.infix_binding_power() {
                if l_bp < min_bp {
                    break;
                }
                self.next()?;

                let Ok(rhs) = self.parse_expr_bp(r_bp) else {
                    break;
                };
                let operator = match kind {
                    TokenKind::Plus => ast::Operator::Plus(Box::new(lhs), Box::new(rhs)),
                    TokenKind::Minus => ast::Operator::Minus(Box::new(lhs), Box::new(rhs)),
                    TokenKind::Asterisk => ast::Operator::Mul(Box::new(lhs), Box::new(rhs)),
                    TokenKind::Slash => ast::Operator::Div(Box::new(lhs), Box::new(rhs)),
                    _ => todo!(),
                };
                lhs = ast::Expression::Operator(operator);
                continue;
            }

            break;
        }

        Ok(lhs)
    }

    fn parse_statement(&mut self) -> Result<Vec<Stmt<'source>>> {
        let mut stmts = Vec::new();

        while let Some(token) = self.next()? {
            match token.kind {
                TokenKind::Eof => break,
                TokenKind::SemiColon => continue,
                _ => {}
            }

            if let TokenKind::Keyword(ref keyword) = token.kind {
                let stmt = match keyword {
                    Keyword::Select => self.parse_select()?,
                    Keyword::Insert => todo!(),
                    Keyword::Update => todo!(),
                    Keyword::Delete => todo!(),
                    _ => todo!("error: unknown statement"),
                };
                stmts.push(stmt);
            }
        }
        // if let TokenKind::Keyword(keyword) = token.kind {}

        Ok(stmts)
    }

    fn parse_select(&mut self) -> Result<ast::Stmt<'source>> {
        let distinct = self
            .next_if_map(|token| {
                if token.kind == TokenKind::Keyword(Keyword::Distinct) {
                    Some(true)
                } else if token.kind == TokenKind::Keyword(Keyword::All) {
                    Some(false)
                } else {
                    None
                }
            })
            .unwrap_or(false);

        let columns = self.parse_select_list()?;
        let from = self.parse_select_from()?;

        self.next_if(|kind| *kind == TokenKind::SemiColon);

        Ok(ast::Stmt::Select {
            distinct,
            columns,
            from,
        })
    }

    fn parse_select_list(&mut self) -> Result<Vec<ast::Expression<'source>>> {
        let mut select_list = Vec::new();

        loop {
            let expr = self.parse_expr()?;
            select_list.push(expr);
            if !self.next_eq(TokenKind::Comma) {
                break;
            }
        }

        Ok(select_list)
    }

    fn parse_select_from(&mut self) -> Result<Vec<ast::From<'source>>> {
        self.expect(TokenKind::Keyword(Keyword::From))?;
        let mut select_from = Vec::new();

        while let Some(token) = self.peek()? {
            if let TokenKind::Ident(ident) = token.kind {
                select_from.push(ast::From(ident));
            } else {
                break;
            }
            self.next()?;
            if !self.next_eq(TokenKind::Comma) {
                break;
            }
        }

        Ok(select_from)
    }
}
