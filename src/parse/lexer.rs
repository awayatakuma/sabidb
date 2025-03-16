use std::collections::HashSet;
use std::iter::Peekable;
use std::str::Chars;

#[derive(Debug)]
pub struct Lexer<'a> {
    keywords: HashSet<&'a str>,
    input: Peekable<Chars<'a>>,
    current_token: Option<Token>,
}

#[derive(Debug, PartialEq)]
pub enum Token {
    Delim(char),
    IntConstant(i32),
    StringConstant(String),
    Keyword(String),
    Id(String),
}

#[derive(Debug, PartialEq)]
pub struct BadSyntaxException;

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        let mut lexer = Lexer {
            keywords: HashSet::from([
                "select", "from", "where", "and", "insert", "into", "values", "delete", "update",
                "set", "create", "table", "int", "varchar", "view", "as", "index", "on",
            ]),
            input: input.chars().peekable(),
            current_token: None,
        };
        lexer.next_token();
        lexer
    }

    pub fn match_delim(&self, d: char) -> bool {
        if let Some(Token::Delim(c)) = &self.current_token {
            *c == d
        } else {
            false
        }
    }

    pub fn match_int_constant(&self) -> bool {
        matches!(&self.current_token, Some(Token::IntConstant(_)))
    }

    pub fn match_string_constant(&self) -> bool {
        matches!(&self.current_token, Some(Token::StringConstant(_)))
    }

    pub fn match_keyword(&self, w: &str) -> bool {
        if let Some(Token::Keyword(kw)) = &self.current_token {
            kw == w
        } else {
            false
        }
    }

    pub fn match_id(&self) -> bool {
        if let Some(Token::Id(id)) = &self.current_token {
            !self.keywords.contains(id.as_str())
        } else {
            false
        }
    }

    pub fn eat_delim(&mut self, d: char) -> Result<(), BadSyntaxException> {
        if !self.match_delim(d) {
            return Err(BadSyntaxException);
        }
        self.next_token();
        Ok(())
    }

    pub fn eat_int_constant(&mut self) -> Result<i32, BadSyntaxException> {
        if let Some(Token::IntConstant(i)) = self.current_token {
            self.next_token();
            Ok(i)
        } else {
            Err(BadSyntaxException)
        }
    }

    pub fn eat_string_constant(&mut self) -> Result<String, BadSyntaxException> {
        if let Some(Token::StringConstant(s)) = self.current_token.take() {
            self.next_token();
            Ok(s)
        } else {
            Err(BadSyntaxException)
        }
    }

    pub fn eat_keyword(&mut self, w: &str) -> Result<(), BadSyntaxException> {
        if !self.match_keyword(w) {
            return Err(BadSyntaxException);
        }
        self.next_token();
        Ok(())
    }

    pub fn eat_id(&mut self) -> Result<String, BadSyntaxException> {
        if let Some(Token::Id(id)) = self.current_token.take() {
            self.next_token();
            Ok(id)
        } else {
            Err(BadSyntaxException)
        }
    }

    fn next_token(&mut self) {
        self.current_token = self.read_token();
    }

    fn read_token(&mut self) -> Option<Token> {
        self.skip_whitespace();
        if let Some(&c) = self.input.peek() {
            match c {
                '\'' => self.read_string_constant(),
                '0'..='9' => self.read_int_constant(),
                'a'..='z' | 'A'..='Z' | '_' => self.read_word(),
                _ => self.read_delim(),
            }
        } else {
            None
        }
    }

    fn read_string_constant(&mut self) -> Option<Token> {
        self.input.next(); // Consume the opening quote
        let mut s = String::new();
        while let Some(&c) = self.input.peek() {
            if c == '\'' {
                self.input.next(); // Consume the closing quote
                return Some(Token::StringConstant(s));
            }
            s.push(c);
            self.input.next();
        }
        None
    }

    fn read_int_constant(&mut self) -> Option<Token> {
        let mut num = 0;
        while let Some(&c) = self.input.peek() {
            if c.is_digit(10) {
                num = num * 10 + (c as i32 - '0' as i32);
                self.input.next();
            } else {
                break;
            }
        }
        Some(Token::IntConstant(num))
    }

    fn read_word(&mut self) -> Option<Token> {
        let mut word = String::new();
        while let Some(&c) = self.input.peek() {
            if c.is_alphabetic() || c == '_' {
                word.push(c);
                self.input.next();
            } else {
                break;
            }
        }
        if self.keywords.contains(word.to_lowercase().as_str()) {
            Some(Token::Keyword(word.to_lowercase()))
        } else {
            Some(Token::Id(word))
        }
    }

    fn read_delim(&mut self) -> Option<Token> {
        if let Some(c) = self.input.next() {
            Some(Token::Delim(c))
        } else {
            None
        }
    }

    fn skip_whitespace(&mut self) {
        while let Some(&c) = self.input.peek() {
            if c.is_whitespace() {
                self.input.next();
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::Lexer;

    #[test]
    fn test_lexer1() {
        let s = "a_b = 111";
        let mut lex = Lexer::new(s);
        lex.match_id();
        let x = lex.eat_id().unwrap();
        lex.eat_delim('=').unwrap();
        let y = lex.eat_int_constant().unwrap();
        assert_eq!("a_b", x);
        assert_eq!(111, y);
    }

    #[test]
    fn test_lexer2() {
        let s = "222 = c_d";
        let mut lex = Lexer::new(s);
        lex.match_id();
        let y = lex.eat_int_constant().unwrap();
        lex.eat_delim('=').unwrap();
        let x = lex.eat_id().unwrap();
        assert_eq!(222, y);
        assert_eq!("c_d", x);
    }
}
