mod postgres_introspect;

extern crate core;
extern crate dotenv;

use crate::futures_util::StreamExt;
use async_graphql::{
    parser::parse_query,
    parser::types::{ExecutableDocument, Field, OperationDefinition, Selection},
    *,
};
use futures::future::*;
use itertools::Itertools;
use postgres_introspect::{convert_introspect_data, fetch_introspection_data};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::fmt::{format, Display, Formatter};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone)]
pub struct DatabaseTable {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_keys: Vec<PrimaryKey>,
    pub toplevel_ops: Vec<Operation>,
}

#[derive(Debug, Clone)]
pub struct Operation {
    pub name: String,
    pub return_type: ReturnType,
    pub args: Vec<Arg>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum InputType {
    GraphQLInteger { default: Option<i32> },
    GraphQLString { default: Option<String> },
    GraphQLBoolean { default: Option<bool> },
    GraphQLID { default: Option<String> },
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
    description: Option<String>,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct PostgresTableColumm {
    column_name: String,
    table_name: String,
    is_nullable: String,
    data_type: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct PostgresTableReferences {
    table_schema: String,
    table_name: String,
    column_name: String,
    foreign_table_schema: String,
    foreign_table_name: String,
    foreign_column_name: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct PostgresTableView {
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
        .connect("postgres://postgres:postgres@0.0.0.0:5439/dixa")
        .map_err(|err| err.to_string())
        .await?;
    let (tables, constraints, columns, references, indexes) = fetch_introspection_data(&pool)
        .map_err(|err| err.to_string())
        .await?;
    let results = convert_introspect_data(tables, constraints, columns, references, indexes);

    println!("{:#?}", results);

    Ok(())
}
#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct TablePagination {
    limit: u32,
    offset: u32,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct TableSelection {
    table_name: String,
    column_names: Vec<String>,
    left_joins: Vec<LeftJoin>,
    where_clauses: Vec<WhereClause>,
    pagination: Option<TablePagination>,
    return_type: ReturnType,
    field_name: String,
    path: String,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct LeftJoin {
    right_table: TableSelection,
    join_conditions: Vec<JoinCondition>,
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct JoinCondition {
    left_col: String,
    right_col: String,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
struct WhereClause {
    path: Option<String>,
    column_name: String,
    expr: String,
}

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Copy)]
pub enum ReturnType {
    Object,
    Array,
}

impl TableSelection {
    fn to_data_select_sql(&self) -> String {
        let where_clause = self
            .where_clauses
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

                    format!(r#"{} = {}"#, left_side, expr)
                },
            )
            .join(" AND ");

        let where_string = if !where_clause.is_empty() {
            format!("where {}", where_clause)
        } else {
            "".to_string()
        };

        let pagination = self
            .pagination
            .as_ref()
            .map(|pag| format!("limit {} offset {}", pag.limit, pag.offset))
            .unwrap_or(String::from(""));

        format!(
            "select * from {} {} {}",
            self.table_name, where_string, pagination
        )
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
        let cols = self
            .column_names
            .iter()
            .map(|col_name| format!(r#""{}_data"."{}" as "{}""#, self.path, col_name, col_name));
        let data_select = self.to_data_select_sql();

        let default = match self.return_type {
            ReturnType::Object => "'null'",
            ReturnType::Array => "'[]'",
        };

        let joins = self
            .left_joins
            .iter()
            .map(|LeftJoin { right_table, .. }| {
                let nested = right_table.to_json_sql();
                format!(
                    r#"
            LEFT OUTER JOIN LATERAL (
                {nested}
            ) as "{path}" on ('true')
            "#,
                    nested = nested,
                    path = right_table.path
                )
            })
            .join("\n");

        let join_cols = self.left_joins.iter().map(|LeftJoin { right_table, .. }| {
            format!(
                r#""{path}"."{col_name}" as "{out_name}""#,
                path = right_table.path,
                col_name = right_table.field_name,
                out_name = right_table.field_name
            )
        });

        let all_cols = cols.chain(join_cols).collect_vec().join(",\n");
        let selector = match self.return_type {
            ReturnType::Object => {
                format!(r#"(json_agg("{path}") -> 0)"#, path = self.path)
            }
            ReturnType::Array => {
                format!(r#"(json_agg("{path}"))"#, path = self.path)
            }
        };

        println!("{:?} -> {}", self.return_type, selector);

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
            path = self.path,
            default = default,
            cols = all_cols,
            data_select = data_select,
            joins = joins,
            field_name = self.field_name,
            selector = selector
        )
    }
}

fn inner(selection: &Selection) -> Field {
    match selection {
        Selection::Field(Positioned { node, .. }) => node.to_owned(),
        Selection::FragmentSpread(_) => todo!(),
        Selection::InlineFragment(_) => todo!(),
    }
}

enum SearchPlace {
    TopLevel,
    Relationship {
        relationship: DatabaseRelationship,
        path: String,
    },
}

fn field_to_table_selection(
    field: &Field,
    tables: &Vec<DatabaseTable>,
    search_place: &SearchPlace,
    relationships2: &Vec<DatabaseRelationship>,
    path: String,
) -> TableSelection {
    let table = match search_place {
        SearchPlace::TopLevel => tables.iter().find(|table| {
            table
                .toplevel_ops
                .iter()
                .map(|op| &op.name)
                .contains(&field.response_key().node.to_string())
        }),
        SearchPlace::Relationship { relationship, .. } => tables
            .iter()
            .find(|table| table.name.contains(&relationship.target_table_name)),
    }
    .expect("No table found");

    let columns: Vec<String> = field
        .selection_set
        .node
        .items
        .iter()
        .map(|Positioned { node, .. }| inner(node))
        .map(|f| f.response_key().node.to_string())
        .filter(|col_name| {
            //TODO: Should probably explode instead of silently ignoring fields
            table
                .get_all_columns()
                .find(|col| col.name.eq(col_name))
                .is_some()
        })
        .collect();

    let joins = field
        .selection_set
        .node
        .items
        .iter()
        .map(|Positioned { node, .. }| inner(node))
        .filter(|field| field.selection_set.node.items.len() > 0)
        .map(|field| {
            println!("wutwutwut {} {}", table.name, field.name.node.to_string());

            let relationship = relationships2
                .iter()
                .find(|relationship| {
                    relationship.table_name.eq(&table.name)
                        && relationship
                            .field_name
                            .contains(&field.name.node.to_string())
                })
                .expect("Unable to find relationship")
                .to_owned();
            (field, relationship)
        })
        .collect::<Vec<(Field, DatabaseRelationship)>>();

    // I do know which joins have been selected
    // I do know which table "this" field corresponds to
    // I need to produce a left join
    // - right table (get from recursive call)
    // - left col (have that from relationship)
    // - right col (have tha from relationship)
    let selected_joins = joins
        .iter()
        .map(|(field, relationship)| {
            let relationship_param = relationship.to_owned();
            let updated_path = format!("{}.{}", path, field.name.node);

            (
                field_to_table_selection(
                    field,
                    tables,
                    &SearchPlace::Relationship {
                        relationship: relationship_param,
                        path: path.to_owned(),
                    },
                    relationships2,
                    updated_path,
                ),
                relationship,
            )
        })
        .map(|(joined_table, relationship)| LeftJoin {
            right_table: joined_table,
            join_conditions: vec![JoinCondition {
                left_col: relationship.field_name.to_owned(),
                right_col: relationship.target_column_name.to_owned(),
            }],
        })
        .collect_vec();

    let where_clauses = match search_place {
        SearchPlace::TopLevel => {
            let op = table
                .toplevel_ops
                .iter()
                .find(|op| op.name.eq(&field.name.node))
                .unwrap();

            op.args
                .iter()
                .filter(|op_arg| op_arg.arg_type.eq(&ArgType::ColumnName)) //TODO: Should fix the data structure
                .map(|op_arg| {
                    let field_arg = field
                        .arguments
                        .iter()
                        .find(|(field_arg, _)| op_arg.name.eq(&field_arg.node))
                        .map(|(_, value)| value.node.to_string());

                    WhereClause {
                        path: None,
                        column_name: op_arg.name.to_owned(),
                        expr: match &op_arg.tpe {
                            InputType::GraphQLInteger { default } => field_arg
                                .or(default.map(|num| num.to_string()))
                                .expect("Unable to extract value for arg"),
                            InputType::GraphQLString { default } => field_arg
                                .or(default.to_owned())
                                .map(|field_value| {
                                    format!(r#"'{}'"#, field_value.trim_matches('"'))
                                })
                                .expect("Unable to extract value for arg"),
                            InputType::GraphQLBoolean { default } => field_arg
                                .or(default.map(|v| v.to_string()))
                                .expect("Unable to extract value for arg"),
                            InputType::GraphQLID { default } => field_arg
                                .or(default.to_owned())
                                .map(|field_value| {
                                    format!(r#"'{}'"#, field_value.trim_matches('"'))
                                })
                                .expect("Unable to extract value for arg"),
                        },
                    }
                })
                .collect_vec()
        }
        SearchPlace::Relationship { relationship, path } => {
            vec![WhereClause {
                path: Some(path.to_string()),
                column_name: relationship.column_name.to_string(),
                expr: format!(r#""{}""#, relationship.target_column_name.to_string()),
            }]
        }
    };

    let pagination = match search_place {
        SearchPlace::TopLevel => {
            let op = table
                .toplevel_ops
                .iter()
                .find(|op| op.name.eq(&field.name.node))
                .unwrap();

            let limit_arg = field
                .arguments
                .iter()
                .find(|(field_arg, _)| field_arg.node.eq("limit"))
                .map(|(_, value)| value.node.to_string().parse::<u32>().unwrap())
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
                .find(|(field_arg, _)| field_arg.node.eq("offset"))
                .map(|(_, value)| value.node.to_string().parse::<u32>().unwrap())
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
                limit: limit_arg.unwrap_or(25u32),
                offset: offset_arg.unwrap_or(0),
            })
        }
        SearchPlace::Relationship { relationship, .. } => match relationship.return_type {
            ReturnType::Object => Some(TablePagination {
                limit: 1,
                offset: 0,
            }),
            ReturnType::Array => {
                let limit_arg = field
                    .arguments
                    .iter()
                    .find(|(field_arg, _)| field_arg.node.eq("limit"))
                    .map(|(_, value)| value.node.to_string().parse::<u32>().unwrap());

                let offset_arg = field
                    .arguments
                    .iter()
                    .find(|(field_arg, _)| field_arg.node.eq("offset"))
                    .map(|(_, value)| value.node.to_string().parse::<u32>().unwrap());

                Some(TablePagination {
                    limit: limit_arg.unwrap_or(25u32),
                    offset: offset_arg.unwrap_or(0),
                })
            }
        },
    };

    let return_type = match search_place {
        SearchPlace::TopLevel => {
            table
                .toplevel_ops
                .iter()
                .find(|op| op.name.eq(&field.name.node.to_string()))
                .expect("Unable to find top level ops")
                .return_type
        }
        SearchPlace::Relationship { relationship, .. } => relationship.return_type,
    };

    TableSelection {
        left_joins: selected_joins,
        where_clauses,
        column_names: columns,
        table_name: table.name.to_owned(),
        return_type,
        pagination,
        field_name: field.name.node.to_string(),
        path,
    }
}

fn to_intermediate(
    query: &ExecutableDocument,
    tables: &Vec<DatabaseTable>,
    relationships2: &Vec<DatabaseRelationship>,
) -> Vec<TableSelection> {
    let ops = query
        .operations
        .iter()
        .map(|(_, Positioned { node, .. })| node.to_owned())
        .collect::<Vec<OperationDefinition>>();

    ops.iter()
        .flat_map(|operation_definition| {
            operation_definition.selection_set.node.items.iter().map(
                |Positioned { node, .. }| match node {
                    Selection::Field(Positioned { node, .. }) => field_to_table_selection(
                        &node,
                        tables,
                        &SearchPlace::TopLevel,
                        relationships2,
                        "root".to_string(),
                    ),
                    Selection::FragmentSpread(_) => todo!(),
                    Selection::InlineFragment(_) => todo!(),
                },
            )
        })
        .collect_vec()
}

#[cfg(test)]
mod tests {
    use crate::{
        convert_introspect_data, fetch_introspection_data, select, to_intermediate, Arg,
        DatabaseRelationship, InputType, JoinCondition, LeftJoin, Operation, ReturnType,
        SearchPlace, TableSelection, TryFutureExt, WhereClause,
    };
    use derivative::*;
    use futures::future::{join_all, try_join_all};
    use futures::{join, select, try_join};
    use sqlformat::{format, FormatOptions, QueryParams};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use testcontainers::*;

    use crate::Column;
    use crate::DatabaseTable;
    use crate::PrimaryKey;
    use async_graphql::parser::parse_query;
    use dockertest::waitfor::{MessageSource, MessageWait};
    use dockertest::{Composition, DockerTest, Source};
    use itertools::Itertools;
    use sqlx::query::Query;
    use sqlx::{postgres::PgPoolOptions, Error, Pool, Postgres, Row};
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

    async fn base_test(sql: Vec<String>, query: String) -> Result<serde_json::Value, Error> {
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

        let (tables, constraints, cols, references, indexes) =
            fetch_introspection_data(&pool).await?;
        let introspection = convert_introspect_data(tables, constraints, cols, references, indexes);

        println!("{:#?}", introspection);
        let query = parse_query(query).unwrap();
        let intermediate =
            to_intermediate(&query, &introspection.tables, &introspection.relationships2);

        println!("{:#?}", intermediate);

        let executable_sql = intermediate
            .iter()
            .map(|inter| inter.to_json_sql().to_owned())
            .collect_vec()
            .first()
            .unwrap()
            .to_owned();

        println!(
            "{}",
            format(&executable_sql, &QueryParams::None, Default::default())
        );

        let res: (serde_json::Value,) = sqlx::query_as(&executable_sql).fetch_one(&pool).await?;
        pg.stop();
        Ok(res.0)
    }

    #[tokio::test]
    async fn simple_table() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!({"a": 1, "b": "rune"});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn simple_table_uuid_pk() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected =
            serde_json::json!({"a": "1ea0f505-b0e6-4a97-881e-a105fd580998", "b": "rune"});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn simple_table_subset_select() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!({"b": "rune"});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn join_docker() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!({"c": 1, "d": 1,"test":{"a": 1, "b": "rune"}});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn simple_table_get_all() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"a": "1ea0f505-b0e6-4a97-881e-a105fd580998", "b": "rune"}, {"a": "78a17f38-91fd-48be-a985-3342ab5f65c5", "b": "rune2"}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn join_docker_list_all() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"c": 1, "d": 1,"test":{"a": 1, "b": "rune"}}, {"c": 2, "d": 2,"test":{"a": 2, "b": "rune2"}}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn simple_table_get_some_with_pagination() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected =
            serde_json::json!([{"b": "rune"}, {"b": "rune2"}, {"b": "rune3"}, {"b": "rune4"}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn simple_table_get_all_with_pagination_and_offset() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected =
            serde_json::json!([{"b": "rune2"}, {"b": "rune3"}, {"b": "rune4"},{"b": "rune5"}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn nested_pagination() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!({"a": 1, "b": "rune", "test2": [{"c": 1, "d": 1}]});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn nested_pagination_with_offset() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!({"a": 1, "b": "rune", "test2": [{"c": 2, "d": 1}]});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"a": 1, "b": "rune"}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk_negative() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!([]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk_compound() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"a": 1, "b": "rune", "c": "drole"}]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk_compound_negative() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!([]);

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk_compound_unique() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!({"a": 1, "b": "rune", "c": "drole"});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk_unique() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!({"a": 1, "b": "rune"});

        assert_eq!(expected, actual);

        Ok(())
    }

    #[tokio::test]
    async fn search_by_non_pk_with_pagination() -> Result<(), Error> {
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

        let actual = base_test(init_sql, graphql_query).await?;
        let expected = serde_json::json!([{"a": 1, "b": "rune"}, {"a": 2, "b": "rune"}, {"a": 3, "b": "rune"}, {"a": 4, "b": "rune"}]);

        assert_eq!(expected, actual);

        Ok(())
    }
}
