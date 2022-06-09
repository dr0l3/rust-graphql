mod postgres_introspect;

extern crate core;
extern crate dotenv;
extern crate futures_util;

use crate::futures_util::StreamExt;
use crate::postgres_introspect::{postgres_data_type_to_input, IntrospectionResult};
use crate::SelectableStuff::{Function, Table};
use crate::SqlQuery::FunctionSelection;
use futures::future::*;
use graphql_parser::query::{
    Definition, Document, Field, OperationDefinition, Query, Selection, SelectionSet, Type,
};
use graphql_parser::schema::Value;
use itertools::{concat, Itertools};
use num_traits::cast::ToPrimitive;
use postgres_introspect::{convert_introspect_data, fetch_introspection_data};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::cell::{Cell, Ref, RefCell};
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{format, Display, Formatter};
use std::ops::{Add, Deref, DerefMut};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

#[derive(Debug)]
struct BindCounter {
    count: Cell<u32>,
}

impl BindCounter {
    fn get_current_and_advance(&self) -> u32 {
        let current = self.count.get();
        self.count.set(current + 1);
        current
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseTable {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_keys: Vec<PrimaryKey>,
    pub toplevel_ops: Vec<Operation>,
}

impl DatabaseTable {
    fn to_graphql_object(
        &self,
        relationships: &Vec<DatabaseRelationship>,
    ) -> graphql_parser::schema::ObjectType<String> {
        let column_fields = self
            .get_all_columns()
            .map(|col| col.to_graphql_field())
            .collect_vec();
        let outbound_relationship_fields = relationships
            .iter()
            .filter(|relationship| relationship.table_name.eq(&self.name))
            .map(|relationship| {
                let field_type: Type<String> = match relationship.return_type {
                    ReturnType::Object => {
                        Type::NamedType(String::from(relationship.table_name.to_owned()))
                    }
                    ReturnType::Array => Type::ListType(Box::new(Type::NonNullType(Box::new(
                        Type::NamedType(String::from(relationship.table_name.to_owned())),
                    )))),
                };

                graphql_parser::schema::Field {
                    position: Default::default(),
                    description: None,
                    name: relationship.field_name.to_owned(),
                    arguments: vec![],
                    field_type: if relationship.column_optional {
                        Type::NonNullType(Box::new(field_type))
                    } else {
                        field_type
                    },
                    directives: vec![],
                }
            })
            .collect_vec();

        let fields = [column_fields, outbound_relationship_fields].concat();

        graphql_parser::schema::ObjectType {
            position: Default::default(),
            description: None,
            name: self.name.to_owned(),
            implements_interfaces: vec![],
            directives: vec![],
            fields,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatabaseFunction {
    pub name: String,
    pub table: DatabaseTable,
    pub args: Vec<FunctionArg>,
}

#[derive(Debug, Clone)]
pub struct FunctionArg {
    pub name: String,
    pub arg_type: InputType,
}

#[derive(Debug, Clone)]
pub enum OperationType {
    Table,
    Function,
}

#[derive(Debug, Clone)]
pub struct Operation {
    pub name: String,
    pub return_type: ReturnType,
    pub args: Vec<Arg>,
    pub operation_type: OperationType,
    pub return_type_optional: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum InputType {
    GraphQLInteger { default: Option<i32> },
    GraphQLString { default: Option<String> },
    GraphQLBoolean { default: Option<bool> },
    GraphQLID { default: Option<String> },
}

impl InputType {
    fn is_quoted(&self) -> bool {
        match self {
            InputType::GraphQLInteger { .. } => false,
            InputType::GraphQLString { .. } => true,
            InputType::GraphQLBoolean { .. } => false,
            InputType::GraphQLID { .. } => true,
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum ArgType {
    ColumnName,
    BuiltIn,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Arg {
    name: String,
    tpe: InputType,
    arg_type: ArgType,
}

impl DatabaseTable {
    fn get_all_columns(&self) -> impl Iterator<Item = &Column> {
        self.columns
            .iter()
            .chain(self.primary_keys.iter().map(|pk| &pk.0))
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub datatype: String,
    pub required: bool,
    pub unique: bool,
}

impl Column {
    fn to_graphql_field(&self) -> graphql_parser::schema::Field<String> {
        let field_type = match postgres_data_type_to_input(&self.datatype) {
            InputType::GraphQLInteger { .. } => Type::NamedType(String::from("Integer")),
            InputType::GraphQLString { .. } => Type::NamedType(String::from("String")),
            InputType::GraphQLBoolean { .. } => Type::NamedType(String::from("Boolean")),
            InputType::GraphQLID { .. } => Type::NamedType(String::from("ID")),
        };

        let type_with_required = if self.required {
            Type::NonNullType(Box::new(field_type))
        } else {
            field_type
        };

        graphql_parser::schema::Field {
            position: Default::default(),
            description: None,
            name: self.name.to_owned(),
            arguments: vec![],
            field_type: type_with_required,
            directives: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct PrimaryKey(Column);

// cases
// - column is optional -> GraphQL type is wrapped optional
// - column is not optional -> GraphQL type is not wrapped in optional
// - target_column is unique -> GraphQL type is an object
// - target_column is not unique -> GraphQL type is an array

#[derive(Debug, Clone)]
pub struct DatabaseRelationship {
    table_name: String,
    column_name: String,
    target_table_name: String,
    target_column_name: String,
    field_name: String,
    return_type: ReturnType,
    column_optional: bool,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct PostgresTableRow {
    schema: String,
    name: String,
    table_type: String,
    owner: String,
    size: String,
    comment: Option<String>,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct PostgresTableColummRow {
    column_name: String,
    table_name: String,
    is_nullable: String,
    data_type: String,
    comment: Option<String>,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct PostgresTableReferencesRow {
    table_schema: String,
    table_name: String,
    column_name: String,
    foreign_table_schema: String,
    foreign_table_name: String,
    foreign_column_name: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct PostgresTableViewRow {
    table_schema: String,
    table_name: String,
    is_updatable: String,
    is_insertable_into: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct PostgresIndexedColumns {
    table_name: String,
    index_name: String,
    column_names: Vec<String>,
    schema_name: String,
    index_def: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
struct TableUniqueConstraint {
    schema_name: String,
    table_name: String,
    column_names: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://postgres:postgres@0.0.0.0:5439/postgres")
        .map_err(|err| err.to_string())
        .await?;
    let (tables, constraints, columns, references, indexes, functions) =
        fetch_introspection_data(&pool)
            .map_err(|err| err.to_string())
            .await?;

    // sqlx::query("select * from hello where a = ?").bind(sqlx::types::Uuid::from(Uuid::new_v4()));

    println!("{:#?}", functions);

    let results =
        convert_introspect_data(tables, constraints, columns, references, indexes, functions);

    Ok(())
}
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct LimitOffsetBind {
    number: u32,
    value: u32,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct TablePagination {
    limit: LimitOffsetBind,
    offset: LimitOffsetBind,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct FunctionSelectionArgument {
    value: String,
    quoted: bool,
}
#[derive(Debug, Eq, PartialOrd, PartialEq, Ord)]
struct SqlQueryExecution {
    query: SqlQuery,
    binds: Vec<String>,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
enum SqlQuery {
    TableSelection {
        table_name: String,
        column_names: Vec<String>,
        left_joins: Vec<LeftJoin>,
        where_clauses: Vec<WhereClause>,
        pagination: Option<TablePagination>,
        return_type: ReturnType,
        field_name: String,
        path: String,
    },
    FunctionSelection {
        function_name: String,
        column_names: Vec<String>,
        left_joins: Vec<LeftJoin>,
        where_clauses: Vec<WhereClause>,
        pagination: Option<TablePagination>,
        return_type: ReturnType,
        field_name: String,
        path: String,
        arguments: Vec<FunctionSelectionArgument>,
    },
}

trait Selectable {
    fn name() -> String;
    fn column_name() -> Vec<String>;
    fn left_joins() -> Vec<LeftJoin>;
    fn return_type() -> ReturnType;
    fn path() -> String;
    fn field_name() -> String;
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct LeftJoin {
    right_table: SqlQuery,
    join_conditions: Vec<JoinCondition>,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct JoinCondition {
    left_col: String,
    right_col: String,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum BindValue {
    Text(String),
    Number(i64),
    Bool(bool),
    Uuid(sqlx::types::Uuid),
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum WhereClauseRightSide {
    ColumnName(String),
    Bind(u32, BindValue),
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
struct WhereClause {
    path: Option<String>,
    column_name: String,
    expr: WhereClauseRightSide,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Copy)]
pub enum ReturnType {
    Object,
    Array,
}

impl SqlQuery {
    fn get_where_clauses(&self) -> &Vec<WhereClause> {
        match self {
            SqlQuery::TableSelection { where_clauses, .. } => where_clauses,
            SqlQuery::FunctionSelection { where_clauses, .. } => where_clauses,
        }
    }

    fn get_pagination(&self) -> &Option<TablePagination> {
        match self {
            SqlQuery::TableSelection { pagination, .. } => pagination,
            SqlQuery::FunctionSelection { pagination, .. } => pagination,
        }
    }

    fn get_column_names(&self) -> &Vec<String> {
        match self {
            SqlQuery::TableSelection { column_names, .. } => column_names,
            SqlQuery::FunctionSelection { column_names, .. } => column_names,
        }
    }

    fn get_return_type(&self) -> &ReturnType {
        match self {
            SqlQuery::TableSelection { return_type, .. } => return_type,
            SqlQuery::FunctionSelection { return_type, .. } => return_type,
        }
    }

    fn get_left_joins(&self) -> &Vec<LeftJoin> {
        match self {
            SqlQuery::TableSelection { left_joins, .. } => left_joins,
            SqlQuery::FunctionSelection { left_joins, .. } => left_joins,
        }
    }

    fn get_field_name(&self) -> &String {
        match self {
            SqlQuery::TableSelection { field_name, .. } => field_name,
            SqlQuery::FunctionSelection { field_name, .. } => field_name,
        }
    }

    fn get_path(&self) -> &String {
        match self {
            SqlQuery::TableSelection { path, .. } => path,
            SqlQuery::FunctionSelection { path, .. } => path,
        }
    }

    fn to_data_select_sql(&self) -> String {
        let where_clause = self
            .get_where_clauses()
            .iter()
            .map(
                |WhereClause {
                     path,
                     column_name,
                     expr,
                 }| {
                    let path_string = path.to_owned().map_or("".to_string(), |path| {
                        format!(r#""{}_data"."#, path).to_string()
                    });
                    let left_side = format!(r#"{}"{}""#, path_string, column_name);
                    let right_side = match expr {
                        WhereClauseRightSide::ColumnName(name) => name.to_string(),
                        WhereClauseRightSide::Bind(number, _) => {
                            format!("${}", number)
                        }
                    };

                    format!(r#"{} = {}"#, left_side, right_side)
                },
            )
            .join(" AND ");

        let where_string = if !where_clause.is_empty() {
            format!("where {}", where_clause)
        } else {
            "".to_string()
        };

        let pagination = self
            .get_pagination()
            .as_ref()
            .map(|pag| {
                let dollar = "$";
                format!(r"limit ${} offset ${}", pag.limit.number, pag.offset.number)
            })
            .unwrap_or(String::from(""));

        let from = match self {
            SqlQuery::TableSelection { table_name, .. } => table_name.to_owned(),
            SqlQuery::FunctionSelection {
                function_name,
                arguments,
                ..
            } => {
                let arg_string = arguments
                    .iter()
                    .map(|arg| {
                        if arg.quoted {
                            format!("'{}'", arg.value.trim_matches('"'))
                        } else {
                            arg.value.to_owned()
                        }
                    })
                    .collect_vec()
                    .join(",");
                format!("{}({})", function_name, arg_string)
            }
        };
        format!("select * from {} {} {}", from, where_string, pagination)
    }

    /*
    ```sql
    select row_to_json(
        select "json_spec"
        from (
            select "data_id"."col1" as "outname1",
            select "data_id"."col2" as "outname2"
        ) as "json_spec"
    )
    from (
        <data select> as "data_id"
    )
    ```
     */

    // Shared for both
    fn to_json_sql(&self) -> String {
        let cols = match self {
            SqlQuery::TableSelection { column_names, .. } => column_names,
            SqlQuery::FunctionSelection { column_names, .. } => column_names,
        }
        .iter()
        .map(|col_name| {
            let path = self.get_path();
            format!(r#""{}_data"."{}" as "{}""#, path, col_name, col_name)
        });
        let data_select = self.to_data_select_sql();

        let default = match self.get_return_type() {
            ReturnType::Object => "'null'",
            ReturnType::Array => "'[]'",
        };

        let joins = self
            .get_left_joins()
            .iter()
            .map(|LeftJoin { right_table, .. }| {
                let nested = right_table.to_json_sql();
                let right_path = match right_table {
                    SqlQuery::TableSelection { path, .. } => path,
                    SqlQuery::FunctionSelection { path, .. } => path,
                };
                format!(
                    r#"
            LEFT OUTER JOIN LATERAL (
                {nested}
            ) as "{path}" on ('true')
            "#,
                    nested = nested,
                    path = right_path
                )
            })
            .join("\n");

        let join_cols = self
            .get_left_joins()
            .iter()
            .map(|LeftJoin { right_table, .. }| {
                format!(
                    r#""{path}"."{col_name}" as "{out_name}""#,
                    path = right_table.get_path(),
                    col_name = right_table.get_field_name(),
                    out_name = right_table.get_field_name()
                )
            });

        let all_cols = cols.chain(join_cols).collect_vec().join(",\n");
        let selector = match self.get_return_type() {
            ReturnType::Object => {
                format!(r#"(json_agg("{path}") -> 0)"#, path = self.get_path())
            }
            ReturnType::Array => {
                format!(r#"(json_agg("{path}"))"#, path = self.get_path())
            }
        };

        println!("{:?} -> {}", self.get_return_type(), selector);

        format!(
            r#"
        select coalesce({selector}, {default}) as "{field_name}" 
        from (select row_to_json(
                            (select "json_spec"
                             from (select {cols}) as "json_spec")) as "{path}"
              from ({data_select}) as "{path}_data"
                {joins}

        ) as "{path}"
        "#,
            path = self.get_path(),
            default = default,
            cols = all_cols,
            data_select = data_select,
            joins = joins,
            field_name = self.get_field_name(),
            selector = selector
        )
    }

    fn extract_binds_sorted(&self) -> Vec<BindValue> {
        self.extract_binds()
            .into_iter()
            .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
            .map(|(_, v)| v.to_owned())
            .collect_vec()
    }

    fn extract_binds(&self) -> Vec<(u32, BindValue)> {
        match self {
            SqlQuery::TableSelection {
                where_clauses,
                pagination,
                left_joins,
                ..
            } => {
                let where_binds = where_clauses
                    .iter()
                    .filter_map(|clause| match &clause.expr {
                        WhereClauseRightSide::ColumnName(_) => None,
                        WhereClauseRightSide::Bind(n, v) => Some((n.to_owned(), v.to_owned())),
                    })
                    .collect_vec();

                let pagination_binds: Vec<(u32, BindValue)> = match pagination {
                    None => {
                        vec![]
                    }
                    Some(pag) => {
                        vec![
                            (
                                pag.offset.number,
                                BindValue::Number(pag.offset.value as i64),
                            ),
                            (pag.limit.number, BindValue::Number(pag.limit.value as i64)),
                        ]
                    }
                };

                let left_join_binds = left_joins
                    .iter()
                    .flat_map(|left_join| left_join.right_table.extract_binds())
                    .collect_vec();

                [where_binds, pagination_binds, left_join_binds].concat()
            }
            SqlQuery::FunctionSelection {
                where_clauses,
                pagination,
                left_joins,
                ..
            } => {
                let where_binds = where_clauses
                    .iter()
                    .filter_map(|clause| match &clause.expr {
                        WhereClauseRightSide::ColumnName(_) => None,
                        WhereClauseRightSide::Bind(n, v) => Some((n.to_owned(), v.to_owned())),
                    })
                    .collect_vec();

                let pagination_binds: Vec<(u32, BindValue)> = match pagination {
                    None => {
                        vec![]
                    }
                    Some(pag) => {
                        vec![
                            (
                                pag.offset.number,
                                BindValue::Number(pag.offset.value as i64),
                            ),
                            (pag.limit.number, BindValue::Number(pag.limit.value as i64)),
                        ]
                    }
                };

                let left_join_binds = left_joins
                    .iter()
                    .flat_map(|left_join| left_join.right_table.extract_binds())
                    .collect_vec();

                [where_binds, pagination_binds, left_join_binds].concat()
            }
        }
    }

    fn count_binds_used(&self) -> u32 {
        match self {
            SqlQuery::TableSelection {
                where_clauses,
                pagination,
                left_joins,
                ..
            } => {
                let where_binds = where_clauses.len().to_u32().unwrap();
                let pagination_binds = match pagination {
                    None => 0,
                    Some(_) => 2,
                }
                .to_u32()
                .unwrap();

                let left_join_binds: u32 = left_joins
                    .iter()
                    .map(|left_join| left_join.right_table.count_binds_used())
                    .sum();

                where_binds + pagination_binds + left_join_binds
            }
            SqlQuery::FunctionSelection {
                where_clauses,
                pagination,
                left_joins,
                ..
            } => {
                let where_binds = where_clauses.len().to_u32().unwrap();
                let pagination_binds = match pagination {
                    None => 0,
                    Some(_) => 2,
                }
                .to_u32()
                .unwrap();

                let left_join_binds: u32 = left_joins
                    .iter()
                    .map(|left_join| left_join.right_table.count_binds_used())
                    .sum();

                where_binds + pagination_binds + left_join_binds
            }
        }
    }
}

// fn inner(selection: &Selection) -> Field {
//     match selection {
//         Selection::Field(Positioned { node, .. }) => node.to_owned(),
//         Selection::FragmentSpread(_) => todo!(),
//         Selection::InlineFragment(_) => todo!(),
//     }
// }

enum SearchPlace {
    TopLevel,
    Relationship {
        relationship: DatabaseRelationship,
        path: String,
    },
}

#[derive(Debug, Clone)]
pub enum SelectableStuff {
    Table { table: DatabaseTable },
    Function { function: DatabaseFunction },
}

impl SelectableStuff {
    fn get_all_columns(&self) -> impl Iterator<Item = &Column> {
        match self {
            SelectableStuff::Table { table } => table.get_all_columns(),
            SelectableStuff::Function { function } => function.table.get_all_columns(),
        }
    }
    fn get_table_name(&self) -> String {
        match self {
            SelectableStuff::Table { table, .. } => table.name.to_owned(),
            SelectableStuff::Function { function, .. } => function.table.name.to_owned(),
        }
    }
}

fn field_to_table_selection(
    field: &Field<String>,
    introspection: &IntrospectionResult,
    search_place: &SearchPlace,
    path: String,
    counter: &BindCounter,
) -> Result<SqlQuery, String> {
    let field_name = field.name.to_owned();
    let selectable = introspection
        .selectable_by_operation_name
        .get(&field_name)
        .ok_or("Unable to find Selectable");

    let table = match search_place {
        SearchPlace::TopLevel => match selectable? {
            Table { table } => table,
            Function { function } => &function.table,
        },

        SearchPlace::Relationship { relationship, .. } => introspection
            .tables
            .iter()
            .find(|table| table.name.contains(&relationship.target_table_name))
            .expect("Unable to find relationship"),
    };

    let col_fields = field
        .selection_set
        .items
        .iter()
        .filter_map(|sel| match sel {
            Selection::Field(f) => {
                if f.selection_set.items.len() == 0 {
                    Some(f)
                } else {
                    None
                }
            }
            Selection::FragmentSpread(_) => {
                todo!()
            }
            Selection::InlineFragment(_) => {
                todo!()
            }
        });
    let join_fields = field
        .selection_set
        .items
        .iter()
        .filter_map(|sel| match sel {
            Selection::Field(f) => {
                if f.selection_set.items.len() > 0 {
                    Some(f)
                } else {
                    None
                }
            }
            Selection::FragmentSpread(_) => {
                todo!()
            }
            Selection::InlineFragment(_) => {
                todo!()
            }
        });

    let columns: Result<Vec<String>, String> = col_fields
        .map(|field| {
            let col = table
                .get_all_columns()
                .find(|col| col.name.eq(&field.name))
                .ok_or("Unable to find column");
            Ok(col?.name.to_string())
        })
        .collect();

    let joins = join_fields
        .map(|f| {
            println!(
                "field: {:#?} relationsips: {:#?}, tablename: {}",
                f, introspection.relationships2, table.name
            );
            let relationship = introspection
                .relationships2
                .iter()
                .find(|relationship| {
                    relationship.table_name.eq(&table.name) && relationship.field_name.eq(&f.name)
                })
                .expect("Unable to find relationshp")
                .to_owned();

            (f, relationship)
        })
        .collect_vec();

    println!("{:#?}", joins);

    let where_clauses = match search_place {
        SearchPlace::TopLevel => {
            let op = table
                .toplevel_ops
                .iter()
                .find(|op| op.name.eq(&field_name))
                .unwrap();

            op.args
                .iter()
                .filter(|op_arg| op_arg.arg_type.eq(&ArgType::ColumnName)) //TODO: Should fix the data structure
                .map(|op_arg| {
                    let field_arg = field
                        .arguments
                        .iter()
                        .find(|(name, _)| name.eq(&op_arg.name))
                        .map(|(_, value)| value.to_string());

                    WhereClause {
                        path: None,
                        column_name: op_arg.name.to_owned(),
                        expr: match &op_arg.tpe {
                            InputType::GraphQLInteger { default } => WhereClauseRightSide::Bind(
                                counter.get_current_and_advance(),
                                BindValue::Number(
                                    field_arg
                                        .or(default.map(|num| num.to_string()))
                                        .expect("Unable to extract value for arg")
                                        .parse()
                                        .unwrap(),
                                ),
                            ),
                            InputType::GraphQLString { default } => WhereClauseRightSide::Bind(
                                counter.get_current_and_advance(),
                                BindValue::Text(
                                    field_arg
                                        .or(default.to_owned())
                                        .map(|field_value| {
                                            format!(r#"{}"#, field_value.trim_matches('"'))
                                        })
                                        .expect("Unable to extract value for arg"),
                                ),
                            ),
                            InputType::GraphQLBoolean { default } => WhereClauseRightSide::Bind(
                                counter.get_current_and_advance(),
                                BindValue::Bool(
                                    field_arg
                                        .map(|v| v.parse::<bool>().unwrap())
                                        .or(*default)
                                        .expect("Unable to extract value for arg"),
                                ),
                            ),
                            InputType::GraphQLID { default } => WhereClauseRightSide::Bind(
                                counter.get_current_and_advance(),
                                BindValue::Uuid(
                                    field_arg
                                        .or(default.to_owned())
                                        .map(|field_value| {
                                            let wat =
                                                format!(r#"{}"#, field_value.trim_matches('"'));
                                            sqlx::types::Uuid::parse_str(&wat).unwrap()
                                        })
                                        .expect("Unable to extract value for arg"),
                                ),
                            ),
                        },
                    }
                })
                .collect_vec()
        }
        SearchPlace::Relationship { relationship, path } => {
            vec![WhereClause {
                path: Some(path.to_string()),
                column_name: relationship.column_name.to_string(),
                expr: WhereClauseRightSide::ColumnName(format!(
                    r#""{}""#,
                    relationship.target_column_name.to_string()
                )),
            }]
        }
    };

    let pagination = match search_place {
        SearchPlace::TopLevel => {
            let op = table
                .toplevel_ops
                .iter()
                .find(|op| op.name.eq(&field.name))
                .unwrap();

            let limit_arg = field
                .arguments
                .iter()
                .find(|(name, _)| name.eq("limit"))
                .map(|(_, value)| value.to_string().parse::<u32>().unwrap())
                .or(op
                    .args
                    .iter()
                    .find(|op_arg| op_arg.name.eq("limit"))
                    .and_then(|op_arg| match &op_arg.tpe {
                        InputType::GraphQLInteger { default } => {
                            default.to_owned().map(|num| num as u32)
                        }
                        InputType::GraphQLString { default } => {
                            default.to_owned().map(|str| str.parse::<u32>().unwrap())
                        }
                        InputType::GraphQLBoolean { .. } => None,
                        InputType::GraphQLID { .. } => None,
                    }));

            let offset_arg = field
                .arguments
                .iter()
                .find(|(name, _)| name.eq("offset"))
                .map(|(_, value)| value.to_string().parse::<u32>().unwrap())
                .or(op
                    .args
                    .iter()
                    .find(|op_arg| op_arg.name.eq("offset"))
                    .and_then(|op_arg| match &op_arg.tpe {
                        InputType::GraphQLInteger { default } => {
                            default.to_owned().map(|num| num as u32)
                        }
                        InputType::GraphQLString { default } => {
                            default.to_owned().map(|str| str.parse::<u32>().unwrap())
                        }
                        InputType::GraphQLBoolean { .. } => None,
                        InputType::GraphQLID { .. } => None,
                    }));

            Some(TablePagination {
                limit: LimitOffsetBind {
                    number: counter.get_current_and_advance(),
                    value: limit_arg.unwrap_or(25u32),
                },
                offset: LimitOffsetBind {
                    number: counter.get_current_and_advance(),
                    value: offset_arg.unwrap_or(0),
                },
            })
        }
        SearchPlace::Relationship { relationship, .. } => match relationship.return_type {
            ReturnType::Object => Some(TablePagination {
                limit: LimitOffsetBind {
                    number: counter.get_current_and_advance(),
                    value: 1,
                },
                offset: LimitOffsetBind {
                    number: counter.get_current_and_advance(),
                    value: 0,
                },
            }),
            ReturnType::Array => {
                let limit_arg = field
                    .arguments
                    .iter()
                    .find(|(name, _)| name.eq("limit"))
                    .map(|(_, value)| value.to_string().parse::<u32>().unwrap());

                let offset_arg = field
                    .arguments
                    .iter()
                    .find(|(name, _)| name.eq("offset"))
                    .map(|(_, value)| value.to_string().parse::<u32>().unwrap());

                Some(TablePagination {
                    limit: LimitOffsetBind {
                        number: counter.get_current_and_advance(),
                        value: limit_arg.unwrap_or(25u32),
                    },
                    offset: LimitOffsetBind {
                        number: counter.get_current_and_advance(),
                        value: offset_arg.unwrap_or(0),
                    },
                })
            }
        },
    };

    let return_type = match search_place {
        SearchPlace::TopLevel => {
            table
                .toplevel_ops
                .iter()
                .find(|op| op.name.eq(&field.name))
                .expect("Unable to find top level ops")
                .return_type
        }
        SearchPlace::Relationship { relationship, .. } => relationship.return_type,
    };

    // I do know which joins have been selected
    // I do know which table "this" field corresponds to
    // I need to produce a left join
    // - right table (get from recursive call)
    // - left col (have that from relationship)
    // - right col (have tha from relationship)
    let selected_joins: Result<Vec<LeftJoin>, String> = joins
        .iter()
        .map(|(field, relationship)| {
            let relationship_param = relationship.to_owned();
            let updated_path = format!("{}.{}", path, field.name);

            let wat = field_to_table_selection(
                &field,
                introspection,
                &SearchPlace::Relationship {
                    relationship: relationship_param,
                    path: path.to_owned(),
                },
                updated_path,
                counter,
            )?;

            Ok((wat, relationship))
        })
        .map_ok(|(joined_table, relationship)| LeftJoin {
            right_table: joined_table,
            join_conditions: vec![JoinCondition {
                left_col: relationship.field_name.to_owned(),
                right_col: relationship.target_column_name.to_owned(),
            }],
        })
        .collect();

    Ok(match search_place {
        SearchPlace::TopLevel => match selectable.expect("Unable to find selectable") {
            Table { table } => SqlQuery::TableSelection {
                left_joins: selected_joins?,
                where_clauses,
                column_names: columns?.to_owned(),
                table_name: table.name.to_owned(),
                return_type,
                pagination,
                field_name: field.name.to_string(),
                path,
            },
            Function { function } => {
                let arguments = function
                    .args
                    .iter()
                    .map(|arg| {
                        println!("{:#?}, {:#?}", arg, field.arguments);
                        let arg_value = field
                            .arguments
                            .iter()
                            .find(|(name, _)| name.eq(&arg.name))
                            .map(|(_, value)| value.to_string())
                            .expect("Unable to find argument");
                        FunctionSelectionArgument {
                            value: arg_value,
                            quoted: arg.arg_type.is_quoted(),
                        }
                    })
                    .collect_vec();

                FunctionSelection {
                    function_name: function.name.to_string(),
                    column_names: columns?.to_owned(),
                    left_joins: selected_joins?,
                    where_clauses,
                    pagination,
                    return_type,
                    field_name: field.name.to_string(),
                    path,
                    arguments,
                }
            }
        },
        SearchPlace::Relationship { .. } => SqlQuery::TableSelection {
            left_joins: selected_joins?,
            where_clauses,
            column_names: columns?.to_owned(),
            table_name: table.name.to_owned(),
            return_type,
            pagination,
            field_name: field.name.to_string(),
            path,
        },
    })
}

fn to_intermediate(
    query: &Document<String>,
    introspection: &IntrospectionResult,
) -> Result<Vec<SqlQuery>, String> {
    let ops = query.definitions.to_owned();

    let counter = BindCounter {
        count: Cell::new(1),
    };

    ops.iter()
        .flat_map(|operation_definition| match operation_definition {
            Definition::Operation(op) => match op {
                OperationDefinition::Query(q) => {
                    q.selection_set.items.iter().map(|wat| match wat {
                        Selection::Field(f) => field_to_table_selection(
                            f,
                            introspection,
                            &SearchPlace::TopLevel,
                            "root".to_string(),
                            &counter,
                        ),
                        Selection::FragmentSpread(_) => {
                            todo!()
                        }
                        Selection::InlineFragment(_) => {
                            todo!()
                        }
                    })
                }
                OperationDefinition::SelectionSet(_) => {
                    todo!()
                }
                OperationDefinition::Mutation(_) => {
                    todo!()
                }
                OperationDefinition::Subscription(_) => {
                    todo!()
                }
            },
            Definition::Fragment(_) => {
                todo!()
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::{
        convert_introspect_data, fetch_introspection_data, select, to_intermediate, Arg, BindValue,
        DatabaseRelationship, InputType, JoinCondition, LeftJoin, Operation, ReturnType,
        SearchPlace, SqlQuery, TryFutureExt, WhereClause,
    };
    use derivative::*;
    use futures::future::{join_all, try_join_all};
    use futures::{join, select, try_join};
    use graphql_parser::{parse_query, Style};
    use sqlformat::{format, FormatOptions, QueryParams};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use testcontainers::*;

    use crate::Column;
    use crate::DatabaseTable;
    use crate::PrimaryKey;
    use dockertest::waitfor::{MessageSource, MessageWait};
    use dockertest::{Composition, DockerTest, Source};
    use graphql_parser::schema::Document;
    use itertools::Itertools;
    use sqlx::query::Query;
    use sqlx::{postgres::types::*, postgres::PgPoolOptions, Error, Pool, Postgres, Row};
    use testcontainers::core::WaitFor;

    #[derive(Debug, Derivative)]
    #[derivative(Default)]
    struct DockerPostgres {
        env_vars: HashMap<String, String>,
        #[derivative(Default(value = "14.to_string()"))]
        tag: String,
    }

    impl Image for DockerPostgres {
        type Args = ();

        fn name(&self) -> String {
            "postgres".to_owned()
        }

        fn tag(&self) -> String {
            self.tag.to_owned()
        }

        fn ready_conditions(&self) -> Vec<WaitFor> {
            vec![WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            )]
        }

        fn env_vars(&self) -> Box<dyn Iterator<Item = (&String, &String)> + '_> {
            Box::new(self.env_vars.iter())
        }
    }

    #[derive(Debug, sqlx::FromRow)]
    struct Whatever {
        root: serde_json::Value,
    }

    async fn base_test(sql: &Vec<String>, query: String) -> Result<serde_json::Value, String> {
        let docker = clients::Cli::default();
        let pg = docker.run(DockerPostgres {
            env_vars: HashMap::from([("POSTGRES_PASSWORD".to_owned(), "postgres".to_owned())]),
            tag: "14".to_owned(),
        });

        let port = pg.get_host_port(5432);

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&format!(
                "postgres://postgres:postgres@0.0.0.0:{}/postgres",
                port
            ))
            .map_err(|err| err.to_string())
            .await?;

        let init = sql
            .iter()
            .map(|init_sql| sqlx::query(init_sql).execute(&pool))
            .collect_vec();

        for future in init {
            future.map_err(|err| err.to_string()).await?;
        }

        //let init_res = try_join_all(init).await?;
        //println!("{:#?}", init_res);

        let (tables, constraints, cols, references, indexes, functions) =
            fetch_introspection_data(&pool)
                .map_err(|err| err.to_string())
                .await?;
        let introspection =
            convert_introspect_data(tables, constraints, cols, references, indexes, functions);

        println!("{:#?}", introspection);
        let query = parse_query(&query).unwrap();
        let intermediate = to_intermediate(&query, &introspection);

        println!("{:#?}", intermediate);

        // let executable_sql = intermediate
        //     .iter()
        //     .map(|inter| inter.to_json_sql().to_owned())
        //     .collect_vec()
        //     .first()
        //     .unwrap()
        //     .to_owned();

        let sqlquery = &intermediate?[0];
        let sql = sqlquery.to_json_sql();
        let binds = sqlquery.extract_binds_sorted();

        let formatted_binds = binds
            .iter()
            .map(|bind| match bind {
                BindValue::Text(t) => t.to_string(),
                BindValue::Number(b) => b.to_string(),
                BindValue::Bool(b) => b.to_string(),
                BindValue::Uuid(b) => b.to_string(),
            })
            .collect_vec();

        println!(
            "{}",
            format(
                &sql,
                &QueryParams::Indexed(formatted_binds),
                Default::default()
            )
        );
        println!("");
        println!("{:#?}", sql);
        println!("{:#?}", binds);

        let base_query = sqlx::query(&sql);
        let hmm = binds.into_iter().fold(base_query, |q, n| match n {
            BindValue::Text(t) => q.bind(t),
            BindValue::Number(n) => q.bind(n),
            BindValue::Bool(b) => q.bind(b),
            BindValue::Uuid(b) => q.bind(b),
        });

        let res2 = hmm.fetch_one(&pool).await;

        //println!("{:?}", res2);

        //let res: (serde_json::Value,) = sqlx::query_as(&executable_sql).fetch_one(&pool).await?;
        pg.stop();
        res2.map(|omg| omg.get(0)).map_err(|err| err.to_string())
    }

    async fn base_test_schema(sql: Vec<String>) -> Result<Document<'static, String>, Error> {
        let docker = clients::Cli::default();
        let pg = docker.run(DockerPostgres {
            env_vars: HashMap::from([("POSTGRES_PASSWORD".to_owned(), "postgres".to_owned())]),
            tag: "14".to_owned(),
        });

        let port = pg.get_host_port(5432);

        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&format!(
                "postgres://postgres:postgres@0.0.0.0:{}/postgres",
                port
            ))
            .await?;

        let init = sql
            .iter()
            .map(|init_sql| sqlx::query(init_sql).execute(&pool))
            .collect_vec();

        for future in init {
            future.await?;
        }

        //let init_res = try_join_all(init).await?;
        //println!("{:#?}", init_res);

        let (tables, constraints, cols, references, indexes, functions) =
            fetch_introspection_data(&pool).await?;
        let introspection =
            convert_introspect_data(tables, constraints, cols, references, indexes, functions);

        let schema = introspection.to_schema().into_static();

        pg.stop();
        Ok(schema)
    }

    #[tokio::test]
    async fn simple_table() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text)"),
            String::from("insert into test values(1, 'rune')"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                get_test_by_id(a: 1) {
                    a
                    b
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!({"a": 1, "b": "rune"});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn simple_table_uuid_pk() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a uuid primary key, b text)"),
            String::from("insert into test values('1ea0f505-b0e6-4a97-881e-a105fd580998', 'rune')"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                get_test_by_id(a: "1ea0f505-b0e6-4a97-881e-a105fd580998") {
                    a
                    b
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected =
            serde_json::json!({"a": "1ea0f505-b0e6-4a97-881e-a105fd580998", "b": "rune"});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn simple_table_subset_select() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text)"),
            String::from("insert into test values(1, 'rune')"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                get_test_by_id(a: 1) {
                    b
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!({"b": "rune"});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn join_docker() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text);"),
            String::from("insert into test values(1, 'rune');"),
            String::from("create table test2(c int primary key, d int references test(a));"),
            String::from("insert into test2 values(1, 1);"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                get_test2_by_id(c: 1) {
                    c
                    d
                    test {
                        a
                        b
                    }
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!({"c": 1, "d": 1,"test":{"a": 1, "b": "rune"}});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn simple_table_get_all() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a uuid primary key, b text)"),
            String::from("insert into test values('1ea0f505-b0e6-4a97-881e-a105fd580998', 'rune')"),
            String::from(
                "insert into test values('78a17f38-91fd-48be-a985-3342ab5f65c5', 'rune2')",
            ),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                list_test{
                    a
                    b
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"a": "1ea0f505-b0e6-4a97-881e-a105fd580998", "b": "rune"}, {"a": "78a17f38-91fd-48be-a985-3342ab5f65c5", "b": "rune2"}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn join_docker_list_all() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text);"),
            String::from("insert into test values(1, 'rune');"),
            String::from("insert into test values(2, 'rune2')"),
            String::from("create table test2(c int primary key, d int references test(a));"),
            String::from("insert into test2 values(1, 1);"),
            String::from("insert into test2 values(2, 2);"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                list_test2 {
                    c
                    d
                    test {
                        a
                        b
                    }
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"c": 1, "d": 1,"test":{"a": 1, "b": "rune"}}, {"c": 2, "d": 2,"test":{"a": 2, "b": "rune2"}}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn simple_table_get_some_with_pagination() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a uuid primary key, b text)"),
            String::from("insert into test values('1ea0f505-b0e6-4a97-881e-a105fd580998', 'rune')"),
            String::from(
                "insert into test values('78a17f38-91fd-48be-a985-3342ab5f65c5', 'rune2')",
            ),
            String::from(
                "insert into test values('2b9b475b-95dc-416c-a6cf-5e3dbdba3cd5', 'rune3')",
            ),
            String::from(
                "insert into test values('9401419b-da9e-4359-8bd5-dd405d48642b', 'rune4')",
            ),
            String::from(
                "insert into test values('72a99d4a-0e63-44f0-8c2a-c4a9f9ae1d8e', 'rune5')",
            ),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                list_test(limit: 4){
                    b
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected =
            serde_json::json!([{"b": "rune"}, {"b": "rune2"}, {"b": "rune3"}, {"b": "rune4"}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn simple_table_get_all_with_pagination_and_offset() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a uuid primary key, b text)"),
            String::from("insert into test values('1ea0f505-b0e6-4a97-881e-a105fd580998', 'rune')"),
            String::from(
                "insert into test values('78a17f38-91fd-48be-a985-3342ab5f65c5', 'rune2')",
            ),
            String::from(
                "insert into test values('2b9b475b-95dc-416c-a6cf-5e3dbdba3cd5', 'rune3')",
            ),
            String::from(
                "insert into test values('9401419b-da9e-4359-8bd5-dd405d48642b', 'rune4')",
            ),
            String::from(
                "insert into test values('72a99d4a-0e63-44f0-8c2a-c4a9f9ae1d8e', 'rune5')",
            ),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                list_test(limit: 4, offset: 1){
                    b
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected =
            serde_json::json!([{"b": "rune2"}, {"b": "rune3"}, {"b": "rune4"},{"b": "rune5"}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn nested_pagination() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text);"),
            String::from("insert into test values(1, 'rune');"),
            String::from("insert into test values(2, 'rune2')"),
            String::from("create table test2(c int primary key, d int references test(a));"),
            String::from("insert into test2 values(1, 1);"),
            String::from("insert into test2 values(2, 1);"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                get_test_by_id(a: 1) {
                    a
                    b
                    test2(limit: 1) {
                        c
                        d
                    }
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!({"a": 1, "b": "rune", "test2": [{"c": 1, "d": 1}]});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn nested_pagination_with_offset() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text);"),
            String::from("insert into test values(1, 'rune');"),
            String::from("insert into test values(2, 'rune2')"),
            String::from("create table test2(c int primary key, d int references test(a));"),
            String::from("insert into test2 values(1, 1);"),
            String::from("insert into test2 values(2, 1);"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                get_test_by_id(a: 1) {
                    a
                    b
                    test2(limit: 1, offset: 1) {
                        c
                        d
                    }
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!({"a": 1, "b": "rune", "test2": [{"c": 2, "d": 1}]});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text)"),
            String::from("insert into test values(1, 'rune')"),
            String::from("create index test_index on test(b)"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                search_test_by_b(b: "rune") {
                    a
                    b
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"a": 1, "b": "rune"}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk_negative() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text)"),
            String::from("insert into test values(1, 'rune')"),
            String::from("create index test_index on test(b)"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                search_test_by_b(b: "hello") {
                    a
                    b
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!([]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk_compound() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text, c text)"),
            String::from("insert into test values(1, 'rune', 'drole')"),
            String::from("create index test_index on test(b, c)"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                search_test_by_b_c(b: "rune", c: "drole") {
                    a
                    b
                    c
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"a": 1, "b": "rune", "c": "drole"}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk_compound_negative() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text, c text)"),
            String::from("insert into test values(1, 'rune', 'drole')"),
            String::from("create index test_index on test(b, c)"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                search_test_by_b_c(b: "rune", c: "non-existing") {
                    a
                    b
                    c
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!([]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk_compound_unique() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text, c text, unique(b,c))"),
            String::from("insert into test values(1, 'rune', 'drole')"),
            String::from("create index test_index on test(b, c)"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                search_test_by_b_c(b: "rune", c: "drole") {
                    a
                    b
                    c
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!({"a": 1, "b": "rune", "c": "drole"});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk_unique() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text unique)"),
            String::from("insert into test values(1, 'rune')"),
            String::from("create index test_index on test(b)"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                search_test_by_b(b: "rune") {
                    a
                    b
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!({"a": 1, "b": "rune"});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk_with_pagination() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text)"),
            String::from("insert into test values(1, 'rune')"),
            String::from("insert into test values(2, 'rune')"),
            String::from("insert into test values(3, 'rune')"),
            String::from("insert into test values(4, 'rune')"),
            String::from("insert into test values(5, 'rune')"),
            String::from("create index test_index on test(b)"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                search_test_by_b(limit: 4, b: "rune") {
                    a
                    b
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"a": 1, "b": "rune"}, {"a": 2, "b": "rune"}, {"a": 3, "b": "rune"}, {"a": 4, "b": "rune"}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn simple_function_get_all() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a uuid primary key, b text)"),
            String::from("create function test_function() returns setof test as $$ select * from test $$ language sql stable"),
            String::from("insert into test values('1ea0f505-b0e6-4a97-881e-a105fd580998', 'rune')"),
            String::from(
                "insert into test values('78a17f38-91fd-48be-a985-3342ab5f65c5', 'rune2')",
            ),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                test_function{
                    a
                    b
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"a": "1ea0f505-b0e6-4a97-881e-a105fd580998", "b": "rune"}, {"a": "78a17f38-91fd-48be-a985-3342ab5f65c5", "b": "rune2"}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn join_function_source() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text);"),
            String::from("create function test_function() returns setof test as $$ select * from test $$ language sql stable"),
            String::from("insert into test values(1, 'rune');"),
            String::from("insert into test values(2, 'rune2')"),
            String::from("create table test2(c int primary key, d int references test(a));"),
            String::from("insert into test2 values(1, 1);"),
            String::from("insert into test2 values(2, 2);"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                test_function {
                    a
                    b
                    test2 {
                        c
                        d
                    }
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"a": 1, "b": "rune","test2":[{"c": 1, "d": 1}]}, {"a": 2, "b": "rune2","test2":[{"c": 2, "d": 2}]}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn join_function_source_with_string_argument() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text);"),
            String::from("create function test_function(b_arg text) returns setof test as $$ select * from test where b = b_arg $$ language sql stable"),
            String::from("insert into test values(1, 'rune');"),
            String::from("insert into test values(2, 'rune')"),
            String::from("insert into test values(3, 'rune3')"),
            String::from("create table test2(c int primary key, d int references test(a));"),
            String::from("insert into test2 values(1, 1);"),
            String::from("insert into test2 values(2, 2);"),
            String::from("insert into test2 values(3, 3);"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                test_function(b_arg: "rune") {
                    a
                    b
                    test2 {
                        c
                        d
                    }
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"a": 1, "b": "rune","test2":[{"c": 1, "d": 1}]}, {"a": 2, "b": "rune","test2":[{"c": 2, "d": 2}]}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn join_function_source_with_numeric_argument() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text);"),
            String::from("create function test_function(l_arg integer) returns setof test as $$ select * from test limit l_arg $$ language sql stable"),
            String::from("insert into test values(1, 'rune');"),
            String::from("insert into test values(2, 'rune')"),
            String::from("insert into test values(3, 'rune3')"),
            String::from("create table test2(c int primary key, d int references test(a));"),
            String::from("insert into test2 values(1, 1);"),
            String::from("insert into test2 values(2, 2);"),
            String::from("insert into test2 values(3, 3);"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                test_function(l_arg: 2) {
                    a
                    b
                    test2 {
                        c
                        d
                    }
                }
            }
            "#,
        );

        let actual = base_test(&init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"a": 1, "b": "rune","test2":[{"c": 1, "d": 1}]}, {"a": 2, "b": "rune","test2":[{"c": 2, "d": 2}]}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn hidden_table() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text);"),
            String::from("comment on table test is '@ignore';"),
            String::from("insert into test values(1, 'rune');"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                test {
                    a
                    b
                }
            }
            "#,
        );

        let res = base_test(&init_sql, graphql_query).await;
        println!("{:#?}", res);
        assert_eq!(true, res.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn hidden_column() -> Result<(), String> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text);"),
            String::from("comment on column test.b is '@ignore';"),
            String::from("insert into test values(1, 'rune');"),
        ];

        let graphql_query = String::from(
            r#"
            query test {
                get_test_by_id(a: 1) {
                    a
                    b
                }
            }
            "#,
        );

        let res = base_test(&init_sql, graphql_query).await;
        println!("{:#?}", res);
        assert_eq!(true, res.is_err());
        println!("FIST ONE SUCCESSED");

        let graphql_query2 = String::from(
            r#"
            query test {
                get_test_by_id(a: 1) {
                    a
                }
            }
            "#,
        );
        let res2 = base_test(&init_sql, graphql_query2).await?;
        let expected = serde_json::json!({"a": 1});

        assert_eq!(res2, expected);

        Ok(())
    }

    #[tokio::test]
    async fn simple_table_schema() -> Result<(), Error> {
        let init_sql = vec![String::from(
            "create table test(a int primary key, b text);",
        )];

        let doc = base_test_schema(init_sql).await?;
        let actual = doc.format(&Style::default());

        let expected = "type test {\n  b: String\n  a: Integer!\n}\n\ntype query {\n  get_test_by_id(a: Integer): test\n  list_test(limit: Integer, offset: Integer): [test!]\n}\n\nschema {\n  query: query\n}\n";

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn join_table_schema() -> Result<(), Error> {
        let init_sql = vec![
            String::from("create table test(a int primary key, b text);"),
            String::from("create table test2(c int primary key, d int references test(a));"),
        ];

        let doc = base_test_schema(init_sql).await?;
        let actual = doc.format(&Style::default());

        let expected = "type test {\n  b: String\n  a: Integer!\n  test2: [test!]\n}\n\ntype test2 {\n  d: Integer\n  c: Integer!\n  test: test2!\n}\n\ntype query {\n  get_test_by_id(a: Integer): test\n  list_test(limit: Integer, offset: Integer): [test!]\n  get_test2_by_id(c: Integer): test2\n  list_test2(limit: Integer, offset: Integer): [test2!]\n}\n\nschema {\n  query: query\n}\n";

        assert_eq!(expected, actual);

        Ok(())
    }
}
