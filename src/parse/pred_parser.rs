use super::lexer::Lexer;

#[derive(Debug)]
pub struct PredParser<'a> {
    lex: Lexer<'a>,
}

impl<'a> PredParser<'a> {
    pub fn new(s: &'a str) -> Self {
        PredParser { lex: Lexer::new(s) }
    }

    pub fn field(&mut self) -> Result<String, super::lexer::BadSyntaxException> {
        self.lex.eat_id()
    }

    pub fn constant(&mut self) -> Result<(), super::lexer::BadSyntaxException> {
        if self.lex.match_string_constant() {
            self.lex.eat_string_constant()?;
        } else if self.lex.match_int_constant() {
            self.lex.eat_int_constant()?;
        } else {
            panic!("unreachable!!")
        }
        Ok(())
    }

    pub fn expression(&mut self) -> Result<(), super::lexer::BadSyntaxException> {
        if self.lex.match_id() {
            self.field()?;
        } else {
            self.constant()?;
        }

        Ok(())
    }

    pub fn term(&mut self) -> Result<(), super::lexer::BadSyntaxException> {
        self.expression()?;
        self.lex.eat_delim('=')?;
        self.expression()?;

        Ok(())
    }

    pub fn predicate(&mut self) -> Result<(), super::lexer::BadSyntaxException> {
        self.term()?;
        if self.lex.match_keyword("and") {
            self.lex.eat_keyword("and")?;
            self.predicate()?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::parse::{lexer::BadSyntaxException, pred_parser::PredParser};

    #[test]
    fn test_pred_parser1() {
        let s = "a=100";
        let mut p = PredParser::new(s);
        assert_eq!(Ok(()), p.predicate());
    }

    #[test]
    fn test_pred_parser2() {
        let s = "a";
        let mut p = PredParser::new(s);
        assert_eq!(Err(BadSyntaxException), p.predicate());
    }
}
