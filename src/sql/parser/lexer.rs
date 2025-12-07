use super::peekable_ext::PeekableExt;

use std::fmt::Display;
use std::iter::Peekable;
use std::str::Chars;

use miette::{ByteOffset, Diagnostic, Result, SourceSpan};
use thiserror::Error;

#[derive(Debug, PartialEq, Eq)]
enum TokenType {
    // Single-character token.
    LeftParen,
    RightParen,
    Comma,
    Dot,
    Minus,
    Plus,
    SemiColon,
    Slash,
    Star,
    // One or two character tokens.
    Bang,
    BangEqual,
    Equal,
    Greater,
    GreaterEqual,
    Less,
    LessEqual,
    // Literals.
    Ident(String),
    String(String),
    Number(String),
    // Keywords.
    Keyword(Keyword),
}

#[derive(Debug, PartialEq, Eq)]
enum Keyword {
    Select,
    Insert,
    Update,
    Delete,
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
        Ok(match value {
            "SELECT" => Keyword::Select,
            "INSERT" => Keyword::Insert,
            "UPDATE" => Keyword::Update,
            "DELETE" => Keyword::Delete,
            "FROM" => Keyword::From,
            "AND" => Keyword::And,
            "OR" => Keyword::Or,
            "FALSE" => Keyword::False,
            "TRUE" => Keyword::True,
            "NULL" => Keyword::Null,
            _ => return Err("not a keyword"),
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
pub struct Token {
    token_type: TokenType,
    offset: ByteOffset,
}

impl Token {
    fn new(token_type: TokenType, offset: ByteOffset) -> Self {
        Self { token_type, offset }
    }
}

#[derive(Error, Debug, Diagnostic)]
#[error("SyntaxError: unterminated string literal")]
struct UnterminatedStringError {
    #[source_code]
    src: String,
    #[label("here")]
    span: SourceSpan,
}

#[derive(Error, Debug, Diagnostic)]
#[error("SyntaxError: unexpected token")]
struct UnexpectedTokenError {
    #[source_code]
    src: String,
    #[label("here")]
    span: SourceSpan,
}

pub struct Lexer<'a> {
    source: &'a str,
    offset: ByteOffset,
    chars: Peekable<Chars<'a>>,
}

impl<'a> Lexer<'a> {
    pub fn new(source: &'a str) -> Self {
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

    fn scan_symbol(&mut self) -> Result<Option<Token>> {
        let current_offset = self.offset;
        let single_char_token = match self.chars.next().unwrap() {
            '(' => TokenType::LeftParen,
            ')' => TokenType::RightParen,
            ',' => TokenType::Comma,
            '.' => TokenType::Dot,
            '-' => TokenType::Minus,
            '+' => TokenType::Plus,
            ';' => TokenType::SemiColon,
            '/' => TokenType::Slash,
            '*' => TokenType::Star,
            '!' => TokenType::Bang,
            '=' => TokenType::Equal,
            '<' => TokenType::Less,
            '>' => TokenType::Greater,
            _ => {
                return Err(UnexpectedTokenError {
                    src: self.source.to_string(),
                    span: current_offset.into(),
                })?;
            }
        };

        let double_char_token = match (&single_char_token, self.chars.peek()) {
            (TokenType::Bang, Some('=')) => Some(TokenType::BangEqual),
            (TokenType::Less, Some('=')) => Some(TokenType::LessEqual),
            (TokenType::Greater, Some('=')) => Some(TokenType::GreaterEqual),
            _ => None,
        };

        if let Some(double_char_token) = double_char_token {
            self.offset += 2;
            Ok(Some(Token::new(double_char_token, current_offset)))
        } else {
            self.offset += 1;
            Ok(Some(Token::new(single_char_token, current_offset)))
        }
    }

    fn scan_ident(&mut self) -> Option<Token> {
        let current_offset = self.offset;
        let mut ident = self
            .next_if(|c| c.is_alphabetic())?
            .to_uppercase()
            .to_string();
        ident.extend(
            self.chars
                .peekable_take_while(|c| c.is_alphanumeric() || *c == '_')
                .flat_map(|c| c.to_uppercase()),
        );
        self.offset += ident.len();

        if let Ok(keyword) = Keyword::try_from(ident.as_str()) {
            Some(Token::new(TokenType::Keyword(keyword), current_offset))
        } else {
            Some(Token::new(TokenType::Ident(ident), current_offset))
        }
    }

    fn scan_number(&mut self) -> Option<Token> {
        let current_offset = self.offset;
        let mut number = self
            .chars
            .peekable_take_while(char::is_ascii_digit)
            .collect::<String>();

        if self.next_eq('.') {
            number.push('.');

            number.extend(self.chars.peekable_take_while(char::is_ascii_digit));

            if let Some(e) = self.next_if(|&c| c == 'e' || c == 'E') {
                number.push(e);
                if let Some(sign) = self.next_if(|&c| c == '+' || c == '-') {
                    number.push(sign);
                }
                number.extend(self.chars.peekable_take_while(char::is_ascii_digit));
            }
        }
        self.offset += number.len();

        Some(Token::new(TokenType::Number(number), current_offset))
    }

    fn scan_string_quoted(&mut self) -> Result<Option<Token>> {
        let mut s = String::new();
        let current_offset = self.offset;

        self.chars.next().unwrap();
        self.offset += '"'.len_utf8();
        loop {
            match self.chars.next() {
                Some('\\') if self.next_eq('"') => {
                    self.offset += '\"'.len_utf8();
                    s.push('"');
                }
                Some('"') if self.next_eq('"') => {
                    s.push('"');
                }
                Some('"') => {
                    self.offset += '"'.len_utf8();
                    break;
                }
                Some(c) => {
                    self.offset += c.len_utf8();
                    s.push(c);
                }
                _ => {
                    return Err(UnterminatedStringError {
                        src: self.source.to_string(),
                        span: current_offset.into(),
                    })?;
                }
            }
        }

        Ok(Some(Token::new(TokenType::String(s), current_offset)))
    }

    fn scan_string_const(&mut self) -> Result<Option<Token>> {
        let mut s = String::new();
        let current_offset = self.offset;

        self.chars.next().unwrap();
        self.offset += '\''.len_utf8();
        loop {
            match self.chars.next() {
                // '' is escaped to '
                Some('\'') if self.next_eq('\'') => {
                    self.offset += "''".len();
                    s.push('\'');
                }
                Some('\'') => {
                    self.offset += '\''.len_utf8();
                    break;
                }
                Some(c) => {
                    self.offset += c.len_utf8();
                    s.push(c);
                }
                _ => {
                    return Err(UnterminatedStringError {
                        src: self.source.to_string(),
                        span: current_offset.into(),
                    })?;
                }
            }
        }

        Ok(Some(Token::new(TokenType::String(s), current_offset)))
    }

    fn scan(&mut self) -> Result<Option<Token>> {
        // skip whitespaces
        while self.chars.next_if(char::is_ascii_whitespace).is_some() {
            self.offset += ' '.len_utf8();
        }

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

impl<'a> Iterator for Lexer<'a> {
    type Item = Result<Token>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.scan() {
            Ok(Some(token)) => Some(Ok(token)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}
