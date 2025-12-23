use super::peekable_ext::PeekableExt;

use std::borrow::Cow;
use std::fmt::Display;
use std::iter::Peekable;
use std::str::Chars;

use miette::{ByteOffset, Diagnostic, Result, SourceSpan};
use thiserror::Error;

#[derive(Debug, PartialEq, Eq)]
pub enum TokenKind<'source> {
    // Single-character tokens.
    LeftParen,
    RightParen,
    Comma,
    Dot,
    Minus,
    Plus,
    SemiColon,
    Slash,
    Asterisk,
    // One or two character tokens.
    Bang,
    BangEqual,
    Equal,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    // Literals.
    Ident(Cow<'source, str>),
    String(Cow<'source, str>),
    Number(Cow<'source, str>),
    // Keywords.
    Keyword(Keyword),
}

#[derive(Debug, PartialEq, Eq)]
pub enum Keyword {
    Select,
    Insert,
    Update,
    Delete,
    All,
    Distinct,
    From,
    And,
    Or,
    False,
    True,
    Null,
}

impl TryFrom<&str> for Keyword {
    type Error = &'static str;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let is = |s: &str| -> bool { s.eq_ignore_ascii_case(value) };
        Ok(if is("SELECT") {
            Keyword::Select
        } else if is("INSERT") {
            Keyword::Insert
        } else if is("UPDATE") {
            Keyword::Update
        } else if is("DELETE") {
            Keyword::Delete
        } else if is("ALL") {
            Keyword::All
        } else if is("DISTINCT") {
            Keyword::Distinct
        } else if is("FROM") {
            Keyword::From
        } else if is("AND") {
            Keyword::And
        } else if is("OR") {
            Keyword::Or
        } else if is("FALSE") {
            Keyword::False
        } else if is("TRUE") {
            Keyword::True
        } else if is("NULL") {
            Keyword::Null
        } else {
            return Err("not a keyword");
        })
    }
}

impl Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keyword = match self {
            Keyword::Select => "SELECT",
            Keyword::Insert => "INSERT",
            Keyword::Update => "UPDATE",
            Keyword::Delete => "DELETE",
            Keyword::All => "ALL",
            Keyword::Distinct => "DISTINCT",
            Keyword::From => "FROM",
            Keyword::And => "AND",
            Keyword::Or => "OR",
            Keyword::False => "FALSE",
            Keyword::True => "TRUE",
            Keyword::Null => "NULL",
        };

        f.write_str(keyword)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct Token<'source> {
    pub kind: TokenKind<'source>,
    offset: ByteOffset,
}

#[derive(Error, Debug, Diagnostic)]
#[error("SyntaxError: unterminated string literal")]
pub struct UnterminatedStringError {
    #[source_code]
    src: String,
    #[label("here")]
    err_span: SourceSpan,
}

#[derive(Error, Debug, Diagnostic)]
#[error("SyntaxError: unexpected token")]
pub struct UnexpectedTokenError {
    #[source_code]
    src: String,
    #[label("here")]
    err_span: SourceSpan,
}

pub struct Lexer<'source> {
    source: &'source str,
    offset: ByteOffset,
    chars: Peekable<Chars<'source>>,
}

impl<'source> Lexer<'source> {
    pub fn new(source: &'source str) -> Self {
        Self {
            source,
            offset: 0,
            chars: source.chars().peekable(),
        }
    }

    fn next_if<P>(&mut self, predicate: P) -> Option<char>
    where
        P: FnOnce(&char) -> bool,
    {
        self.chars.next_if(predicate)
    }

    fn next_eq(&mut self, c: char) -> bool {
        self.chars.next_if_eq(&c).is_some()
    }

    fn peekable_take_while<P>(&mut self, predicate: P) -> impl Iterator<Item = char>
    where
        P: Fn(&char) -> bool,
    {
        self.chars.peekable_take_while(predicate)
    }

    fn scan_symbol(&mut self) -> Result<Option<Token<'source>>> {
        let offset = self.offset;
        let single_char_token = match self.chars.next().unwrap() {
            '(' => TokenKind::LeftParen,
            ')' => TokenKind::RightParen,
            ',' => TokenKind::Comma,
            '.' => TokenKind::Dot,
            '-' => TokenKind::Minus,
            '+' => TokenKind::Plus,
            ';' => TokenKind::SemiColon,
            '/' => TokenKind::Slash,
            '*' => TokenKind::Asterisk,
            '!' => TokenKind::Bang,
            '=' => TokenKind::Equal,
            '<' => TokenKind::Less,
            '>' => TokenKind::Greater,
            _ => {
                return Err(UnexpectedTokenError {
                    src: self.source.to_string(),
                    err_span: offset.into(),
                })?;
            }
        };

        let double_char_token = match (&single_char_token, self.chars.peek()) {
            (TokenKind::Bang, Some('=')) => Some(TokenKind::BangEqual),
            (TokenKind::Less, Some('=')) => Some(TokenKind::LessEqual),
            (TokenKind::Greater, Some('=')) => Some(TokenKind::GreaterEqual),
            _ => None,
        };

        if let Some(double_char_token) = double_char_token {
            self.offset += 2;
            Ok(Some(Token {
                kind: double_char_token,
                offset,
            }))
        } else {
            self.offset += 1;
            Ok(Some(Token {
                kind: single_char_token,
                offset,
            }))
        }
    }

    fn scan_ident(&mut self) -> Option<Token<'source>> {
        let offset = self.offset;
        let len = self
            .peekable_take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .map(char::len_utf8)
            .sum::<usize>();

        let ident = &self.source[offset..offset + len];
        self.offset += ident.len();

        if let Ok(keyword) = Keyword::try_from(ident) {
            Some(Token {
                kind: TokenKind::Keyword(keyword),
                offset,
            })
        } else {
            Some(Token {
                kind: TokenKind::Ident(Cow::Borrowed(ident)),
                offset,
            })
        }
    }

    fn scan_number(&mut self) -> Option<Token<'source>> {
        let offset = self.offset;
        let mut len = self
            .peekable_take_while(char::is_ascii_digit)
            .map(char::len_utf8)
            .sum::<usize>();

        if self.next_eq('.') {
            len += '.'.len_utf8();
            len += self
                .peekable_take_while(char::is_ascii_digit)
                .map(char::len_utf8)
                .sum::<usize>();

            if let Some(e) = self.next_if(|&c| c == 'e' || c == 'E') {
                len += e.len_utf8();
                if let Some(sign) = self.next_if(|&c| c == '+' || c == '-') {
                    len += sign.len_utf8();
                }
                len += self
                    .peekable_take_while(char::is_ascii_digit)
                    .map(char::len_utf8)
                    .sum::<usize>();
            }
        }
        let number = &self.source[self.offset..self.offset + len];
        self.offset += len;

        Some(Token {
            kind: TokenKind::Number(Cow::Borrowed(number)),
            offset,
        })
    }

    fn scan_string_quoted(&mut self) -> Result<Option<Token<'source>>> {
        let token_start = self.offset;
        self.chars.next().unwrap();
        self.offset += '"'.len_utf8();
        let str_start = self.offset;
        let mut s = Cow::Borrowed(&self.source[str_start..str_start]);

        loop {
            let c = match self.chars.next() {
                // \" is escaped to "
                Some('\\') if self.next_eq('"') => {
                    self.offset += "\\\"".len();
                    s = Cow::Owned(s.into_owned());
                    '"'
                }
                // "" is escaped to "
                Some('"') if self.next_eq('"') => {
                    self.offset += "\"\"".len();
                    s = Cow::Owned(s.into_owned());
                    '"'
                }
                Some('"') => {
                    self.offset += '"'.len_utf8();
                    break;
                }
                Some(c) => {
                    self.offset += c.len_utf8();
                    c
                }
                None => {
                    return Err(UnterminatedStringError {
                        src: self.source.to_string(),
                        err_span: token_start.into(),
                    })?;
                }
            };

            match s {
                Cow::Borrowed(_) => s = Cow::Borrowed(&self.source[str_start..self.offset]),
                Cow::Owned(ref mut s) => s.push(c),
            }
        }

        Ok(Some(Token {
            kind: TokenKind::String(s),
            offset: token_start,
        }))
    }

    fn scan_string_const(&mut self) -> Result<Option<Token<'source>>> {
        let token_start = self.offset;
        self.chars.next().unwrap();
        self.offset += '\''.len_utf8();
        let str_start = self.offset;
        let mut s = Cow::Borrowed(&self.source[str_start..str_start]);

        loop {
            let c = match self.chars.next() {
                // '' is escaped to '
                Some('\'') if self.next_eq('\'') => {
                    self.offset += "''".len();
                    s = Cow::Owned(s.into_owned());
                    '\''
                }
                Some('\'') => {
                    self.offset += '\''.len_utf8();
                    break;
                }
                Some(c) => {
                    self.offset += c.len_utf8();
                    c
                }
                None => {
                    return Err(UnterminatedStringError {
                        src: self.source.to_string(),
                        err_span: token_start.into(),
                    })?;
                }
            };

            match s {
                Cow::Borrowed(_) => s = Cow::Borrowed(&self.source[str_start..self.offset]),
                Cow::Owned(ref mut s) => s.push(c),
            }
        }

        Ok(Some(Token {
            kind: TokenKind::String(s),
            offset: token_start,
        }))
    }

    fn scan(&mut self) -> Result<Option<Token<'source>>> {
        // skip whitespaces
        self.offset += self
            .peekable_take_while(char::is_ascii_whitespace)
            .map(char::len_utf8)
            .sum::<usize>();

        let Some(c) = self.chars.peek() else {
            return Ok(None);
        };
        match c {
            '"' => self.scan_string_quoted(),
            '\'' => self.scan_string_const(),
            '0'..='9' => Ok(self.scan_number()),
            c if c.is_ascii_alphabetic() => Ok(self.scan_ident()),
            _ => self.scan_symbol(),
        }
    }
}

impl<'source> Iterator for Lexer<'source> {
    type Item = Result<Token<'source>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
