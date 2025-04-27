use std::sync::{Arc, Mutex};

use crate::{
    query::{constant::Constant, expression::Expression, predicate::Predicate, term::Term},
    record::schema::Schema,
};

use super::{
    create_index_data::CreateIndexData, create_table_data::CreateTableData,
    create_view_data::CreateViewData, delete_data::DeleteData, insert_data::InsertData,
    lexer::Lexer, modify_data::ModifyData, query_data::QueryData,
};

#[derive(Debug)]
pub enum UpdateCommand {
    Insert(InsertData),
    Delete(DeleteData),
    Modify(ModifyData),
    CreateTable(CreateTableData),
    CreateView(CreateViewData),
    CreateIndex(CreateIndexData),
}

#[derive(Debug)]
pub struct Parser<'a> {
    lex: Lexer<'a>,
}

impl<'a> Parser<'a> {
    pub fn new(s: &'a str) -> Self {
        Parser { lex: Lexer::new(s) }
    }

    pub fn field(&mut self) -> Result<String, super::lexer::BadSyntaxException> {
        self.lex.eat_id()
    }

    pub fn constant(&mut self) -> Result<Constant, super::lexer::BadSyntaxException> {
        if self.lex.match_string_constant() {
            return Ok(Constant::new_from_string(self.lex.eat_string_constant()?));
        } else if self.lex.match_int_constant() {
            return Ok(Constant::new_from_i32(self.lex.eat_int_constant()?));
        } else {
            panic!("unreachable!!")
        }
    }

    pub fn expression(&mut self) -> Result<Expression, super::lexer::BadSyntaxException> {
        if self.lex.match_id() {
            return Ok(Expression::new_from_fldname(self.field()?));
        } else {
            return Ok(Expression::new_from_val(self.constant()?));
        }
    }

    pub fn term(&mut self) -> Result<Term, super::lexer::BadSyntaxException> {
        let lhs = self.expression()?;
        self.lex.eat_delim('=')?;
        let rhs = self.expression()?;

        Ok(Term::new(lhs, rhs))
    }

    pub fn predicate(&mut self) -> Result<Predicate, super::lexer::BadSyntaxException> {
        let mut pred = Predicate::new_from_term(self.term()?);

        if self.lex.match_keyword("and") {
            self.lex.eat_keyword("and")?;
            pred.conjoin_with(&self.predicate()?);
        }
        if self.lex.match_keyword("and") {
            self.lex.eat_keyword("and")?;
            self.predicate()?;
        }

        Ok(pred)
    }

    pub fn query(&mut self) -> Result<QueryData, super::lexer::BadSyntaxException> {
        self.lex.eat_keyword("select")?;
        let fields = self.select_list()?;
        self.lex.eat_keyword("from")?;
        let tables = self.table_list()?;
        let mut pred = Predicate::new();
        if self.lex.match_keyword("where") {
            self.lex.eat_keyword("where")?;
            pred = self.predicate()?;
        }

        Ok(QueryData::new(fields, tables, pred))
    }

    fn select_list(&mut self) -> Result<Vec<String>, super::lexer::BadSyntaxException> {
        let mut ret = vec![self.field()?];
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            ret.extend(self.select_list()?);
        }

        Ok(ret)
    }

    fn table_list(&mut self) -> Result<Vec<String>, super::lexer::BadSyntaxException> {
        let mut ret = vec![self.lex.eat_id()?];
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            ret.extend(self.table_list()?);
        }

        Ok(ret)
    }

    pub fn update_cmd(&mut self) -> Result<UpdateCommand, super::lexer::BadSyntaxException> {
        if self.lex.match_keyword("insert") {
            Ok(UpdateCommand::Insert(self.insert()?))
        } else if self.lex.match_keyword("delete") {
            Ok(UpdateCommand::Delete(self.delete()?))
        } else if self.lex.match_keyword("update") {
            Ok(UpdateCommand::Modify(self.modify()?))
        } else if self.lex.match_keyword("create") {
            self.create()
        } else {
            panic!("unreachable!!")
        }
    }

    fn create(&mut self) -> Result<UpdateCommand, super::lexer::BadSyntaxException> {
        self.lex.eat_keyword("create")?;
        if self.lex.match_keyword("table") {
            Ok(UpdateCommand::CreateTable(self.create_table()?))
        } else if self.lex.match_keyword("view") {
            Ok(UpdateCommand::CreateView(self.create_view()?))
        } else if self.lex.match_keyword("index") {
            Ok(UpdateCommand::CreateIndex(self.create_index()?))
        } else {
            panic!("unreachable!!")
        }
    }

    fn delete(&mut self) -> Result<DeleteData, super::lexer::BadSyntaxException> {
        self.lex.eat_keyword("delete")?;
        self.lex.eat_keyword("from")?;
        let tblname = self.lex.eat_id()?;
        let mut pred = Predicate::new();
        if self.lex.match_keyword("where") {
            self.lex.eat_keyword("where")?;
            pred = self.predicate()?;
        }

        Ok(DeleteData::new(tblname, pred))
    }

    pub fn insert(&mut self) -> Result<InsertData, super::lexer::BadSyntaxException> {
        self.lex.eat_keyword("insert")?;
        self.lex.eat_keyword("into")?;
        let tblname = self.lex.eat_id()?;
        self.lex.eat_delim('(')?;
        let flds = self.field_list()?;
        self.lex.eat_delim(')')?;
        self.lex.eat_keyword("values")?;
        self.lex.eat_delim('(')?;
        let vals = self.const_list()?;
        self.lex.eat_delim(')')?;

        Ok(InsertData::new(tblname, flds, vals))
    }

    fn field_list(&mut self) -> Result<Vec<String>, super::lexer::BadSyntaxException> {
        let mut ret = vec![self.field()?];
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            ret.extend(self.field_list()?);
        }

        Ok(ret)
    }

    fn const_list(&mut self) -> Result<Vec<Constant>, super::lexer::BadSyntaxException> {
        let mut ret = vec![self.constant()?];
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            ret.extend(self.const_list()?);
        }

        Ok(ret)
    }

    pub fn modify(&mut self) -> Result<ModifyData, super::lexer::BadSyntaxException> {
        self.lex.eat_keyword("update")?;
        let tblname = self.lex.eat_id()?;
        self.lex.eat_keyword("set")?;
        let fldname = self.field()?;
        self.lex.eat_delim('=')?;
        let newval = self.expression()?;
        let mut pred = Predicate::new();
        if self.lex.match_keyword("where") {
            self.lex.eat_keyword("where")?;
            pred = self.predicate()?;
        }

        Ok(ModifyData::new(tblname, fldname, newval, pred))
    }

    pub fn create_table(&mut self) -> Result<CreateTableData, super::lexer::BadSyntaxException> {
        self.lex.eat_keyword("table")?;
        let tblname = self.lex.eat_id()?;
        self.lex.eat_delim('(')?;
        let sch = self.field_defs()?;
        self.lex.eat_delim(')')?;

        Ok(CreateTableData::new(tblname, sch))
    }

    fn field_defs(&mut self) -> Result<Schema, super::lexer::BadSyntaxException> {
        let mut schema = self.field_def()?;
        if self.lex.match_delim(',') {
            self.lex.eat_delim(',')?;
            let schema2 = self.field_defs()?;
            schema
                .add_all(Arc::new(Mutex::new(schema2)))
                .map_err(|_| super::lexer::BadSyntaxException)?;
        }
        return Ok(schema);
    }

    fn field_def(&mut self) -> Result<Schema, super::lexer::BadSyntaxException> {
        let fldname = self.field()?;
        return self.field_type(fldname);
    }

    fn field_type(&mut self, fldname: String) -> Result<Schema, super::lexer::BadSyntaxException> {
        let mut schema = Schema::new();
        if self.lex.match_keyword("int") {
            self.lex.eat_keyword("int")?;
            schema
                .add_int_field(&fldname)
                .map_err(|_| super::lexer::BadSyntaxException)?;
        } else {
            self.lex.eat_keyword("varchar")?;
            self.lex.eat_delim('(')?;
            let str_len = self.lex.eat_int_constant()?;
            self.lex.eat_delim(')')?;
            schema
                .add_string_field(&fldname, str_len)
                .map_err(|_| super::lexer::BadSyntaxException)?;
        }

        return Ok(schema);
    }

    fn create_view(&mut self) -> Result<CreateViewData, super::lexer::BadSyntaxException> {
        self.lex.eat_keyword("view")?;
        let viewname = self.lex.eat_id()?;
        self.lex.eat_keyword("as")?;
        let qd = self.query()?;

        Ok(CreateViewData::new(viewname, qd))
    }

    fn create_index(&mut self) -> Result<CreateIndexData, super::lexer::BadSyntaxException> {
        self.lex.eat_keyword("index")?;
        let idxname = self.lex.eat_id()?;
        self.lex.eat_keyword("on")?;
        let tblname = self.lex.eat_id()?;
        self.lex.eat_delim('(')?;
        let fldname = self.field()?;
        self.lex.eat_delim(')')?;
        Ok(CreateIndexData::new(idxname, tblname, fldname))
    }
}

#[cfg(test)]
mod tests {

    use crate::{
        parse::parser::{Parser, UpdateCommand},
        query::{constant::Constant, expression::Expression, predicate::Predicate, term::Term},
    };

    #[test]
    fn test_pred_parser_select() {
        let s = "select col_a from tab_a where col_b = 1";
        let mut p = Parser::new(s);
        assert_eq!(s, p.query().unwrap().to_string());
    }

    #[test]
    fn test_pred_parser_insert() {
        let s = "insert into tab_a (col_b, col_c) values ('a', 2)";
        let mut p = Parser::new(s);
        let UpdateCommand::Insert(uc) = p.update_cmd().unwrap() else {
            assert!(false);
            panic!("unreachable!!")
        };
        assert_eq!("tab_a", uc.table_name());
        assert_eq!(vec!["col_b", "col_c"], uc.fields());
        assert_eq!(
            vec![
                Constant::new_from_string("a".to_string()),
                Constant::new_from_i32(2)
            ],
            uc.vals()
        );
    }

    #[test]
    fn test_pred_parser_delete() {
        let s = "delete from tab_a where col_b = 'a' and col_c = 1";
        let mut p = Parser::new(s);
        let UpdateCommand::Delete(uc) = p.update_cmd().unwrap() else {
            assert!(false);
            panic!("unreachable!!")
        };
        let t1 = Term::new(
            Expression::new_from_fldname("col_b".to_string()),
            Expression::new_from_val(Constant::new_from_string("a".to_string())),
        );
        let t2 = Term::new(
            Expression::new_from_fldname("col_c".to_string()),
            Expression::new_from_val(Constant::new_from_i32(1)),
        );
        let mut expected = Predicate::new_from_term(t1);
        expected.conjoin_with(&Predicate::new_from_term(t2));

        assert_eq!("tab_a", uc.table_name());
        assert_eq!(expected, uc.pred());
        assert_eq!("col_b = a and col_c = 1".to_string(), uc.pred().to_string())
    }

    #[test]
    fn test_pred_parser_update() {
        let s = "update tab_a set col_a = 1 where col_c = 2";
        let mut p = Parser::new(s);
        let UpdateCommand::Modify(uc) = p.update_cmd().unwrap() else {
            assert!(false);
            panic!("unreachable!!")
        };
        assert_eq!("tab_a", uc.table_name());
        assert_eq!("col_a", uc.target_field());
        assert_eq!(
            Expression::new_from_val(Constant::new_from_i32(1)),
            uc.new_val()
        );
    }

    #[test]
    fn test_pred_parser_create_table() {
        let s = "create table tab_a (col_a int, col_b varchar(8))";
        let mut p = Parser::new(s);
        let UpdateCommand::CreateTable(uc) = p.update_cmd().unwrap() else {
            assert!(false);
            panic!("unreachable!!")
        };
        assert_eq!("tab_a", uc.table_name());
    }

    #[test]
    fn test_pred_parser_create_view() {
        let s = "create view view_a as select col_a, col_b from tab_a";
        let mut p = Parser::new(s);
        let UpdateCommand::CreateView(uc) = p.update_cmd().unwrap() else {
            assert!(false);
            panic!("unreachable!!")
        };
        assert_eq!("select col_a, col_b from tab_a", uc.view_def());
    }

    #[test]
    fn test_pred_parser_create_index() {
        let s = "create index idx_a on tab_a (col_a)";
        let mut p = Parser::new(s);
        let UpdateCommand::CreateIndex(uc) = p.update_cmd().unwrap() else {
            assert!(false);
            panic!("unreachable!!")
        };
        assert_eq!("tab_a", uc.table_name());
        assert_eq!("col_a", uc.field_name());
        assert_eq!("idx_a", uc.idx_name());
    }
}
