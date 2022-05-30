mod postgres_introspect;

extern crate dotenv;

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
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub primary_keys: Vec<PrimaryKey>,
    pub toplevel_ops: Vec<Op>,
}

#[derive(Debug, Clone)]
pub struct Op {
    pub name: String,
    pub return_type: ReturnType,
    pub args: Vec<Arg>,
}

#[derive(Debug, Clone)]
pub enum InputType {
    GraphQLInteger,
    GraphQLString,
    GraphQLBoolean,
    GraphQLID,
}

#[derive(Debug, Clone)]
pub struct Arg {
    name: String,
    tpe: InputType,
}

impl Table {
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
pub struct Relationship2 {
    table_name: String,
    column_name: String,
    target_table_name: String,
    target_column_name: String,
    field_name: String,
    return_type: ReturnType,
    column_optional: bool,
}

// really a two way relationship
#[derive(Debug, Clone)]
struct TableRelationship {
    table: Table,
    column: Column,
    foreign_table: Table,
    foreign_column: Column,
    constraint_name: String,
    field_name: String,
    //table_to_foreign_field_name
    //foreign_to
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct TableRow {
    schema: String,
    name: String,
    table_type: String,
    owner: String,
    size: String,
    description: Option<String>,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct TableColumm {
    column_name: String,
    table_name: String,
    is_nullable: String,
    data_type: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct TableReferences {
    table_schema: String,
    table_name: String,
    column_name: String,
    foreign_table_schema: String,
    foreign_table_name: String,
    foreign_column_name: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct TableView {
    table_schema: String,
    table_name: String,
    is_updatable: String,
    is_insertable_into: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
struct TableUniqueConstraint {
    schema_name: String,
    table_name: String,
    column_name: String,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://postgres:postgres@0.0.0.0:5439/dixa")
        .map_err(|err| err.to_string())
        .await?;
    let (tables, constraints, columns, references) = fetch_introspection_data(&pool)
        .map_err(|err| err.to_string())
        .await?;
    let results = convert_introspect_data(tables, constraints, columns, references);

    println!("{:#?}", results);

    Ok(())
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct TableSelection {
    table_name: String,
    column_names: Vec<String>,
    left_joins: Vec<LeftJoin>,
    where_clauses: Vec<WhereClause>,
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
    left_path: Option<String>,
    left_column: String,
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
                     left_path,
                     left_column,
                     expr,
                 }| {
                    let path_string = left_path.to_owned().map_or("".to_string(), |path| {
                        format!(r#""{}_data"."#, path).to_string()
                    });
                    format!(r#"{}"{}" = {}"#, path_string, left_column, expr)
                },
            )
            .join(" AND ");

        let where_string = if !where_clause.is_empty() {
            format!("where {}", where_clause)
        } else {
            "".to_string()
        };

        format!("select * from {} {}", self.table_name, where_string)
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
        relationship: Relationship2,
        path: String,
    },
}

fn field_to_table_selection(
    field: &Field,
    tables: &Vec<Table>,
    search_place: &SearchPlace,
    relationships2: &Vec<Relationship2>,
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
            let relationship = relationships2
                .iter()
                .find(|relationship| {
                    relationship.table_name.contains(&table.name)
                        && relationship
                            .field_name
                            .contains(&field.name.node.to_string())
                })
                .expect("Unable to find relationship")
                .to_owned();
            (field, relationship)
        })
        .collect::<Vec<(Field, Relationship2)>>();

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
                .map(|op_arg| {
                    let (_, field_arg_value) = field
                        .arguments
                        .iter()
                        .find(|(field_arg, _)| op_arg.name.eq(&field_arg.node))
                        .unwrap();

                    println!("{:?}", op_arg);

                    WhereClause {
                        left_path: None,
                        left_column: op_arg.name.to_owned(),
                        expr: match op_arg.tpe {
                            InputType::GraphQLInteger => field_arg_value.node.to_string(),
                            InputType::GraphQLString => {
                                format!(r#"{}"#, field_arg_value.node.to_string())
                            }
                            InputType::GraphQLBoolean => field_arg_value.node.to_string(),
                            InputType::GraphQLID => {
                                format!(r#"{}"#, field_arg_value.node.to_string())
                            }
                        },
                    }
                })
                .collect_vec()
        }
        SearchPlace::Relationship { relationship, path } => {
            vec![WhereClause {
                left_path: Some(path.to_string()),
                left_column: relationship.column_name.to_string(),
                expr: format!(r#""{}""#, relationship.target_column_name.to_string()),
            }]
        }
    };

    let return_type = match search_place {
        SearchPlace::TopLevel => {
            table
                .toplevel_ops
                .iter()
                .find(|op| op.name.contains(&field.name.node.to_string()))
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
        field_name: field.name.node.to_string(),
        path,
    }
}

fn to_intermediate(
    query: &ExecutableDocument,
    tables: &Vec<Table>,
    relationships2: &Vec<Relationship2>,
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
        convert_introspect_data, fetch_introspection_data, select, to_intermediate, Arg, InputType,
        JoinCondition, LeftJoin, Op, Relationship2, ReturnType, SearchPlace, TableRelationship,
        TableSelection, TryFutureExt, WhereClause,
    };
    use derivative::*;
    use futures::future::{join_all, try_join_all};
    use futures::{join, select, try_join};
    use sqlformat::{format, FormatOptions, QueryParams};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use testcontainers::*;

    use crate::Column;
    use crate::PrimaryKey;
    use crate::Table;
    use async_graphql::parser::parse_query;
    use dockertest::waitfor::{MessageSource, MessageWait};
    use dockertest::{Composition, DockerTest, Source};
    use itertools::Itertools;
    use sqlx::query::Query;
    use sqlx::{postgres::PgPoolOptions, Error, Pool, Postgres, Row};
    use testcontainers::core::WaitFor;

    fn single_table() -> Vec<Table> {
        vec![Table {
            columns: vec![
                Column {
                    name: "number".to_string(),
                    datatype: "integer".to_string(),
                    required: false,
                    unique: false,
                },
                Column {
                    name: "txt".to_string(),
                    datatype: "text".to_string(),
                    required: false,
                    unique: false,
                },
            ],
            primary_keys: vec![PrimaryKey(Column {
                name: "id".to_string(),
                datatype: "uuid".to_string(),
                required: true,
                unique: true,
            })],
            name: "example".to_string(),
            toplevel_ops: vec![Op {
                name: "get_example_by_id".to_string(),
                return_type: ReturnType::Object,
                args: vec![Arg {
                    name: "id".to_string(),
                    tpe: InputType::GraphQLID,
                }],
            }],
        }]
    }

    fn two_tables() -> (Vec<Table>, Vec<Relationship2>) {
        let author_col = Column {
            name: "author".to_string(),
            datatype: "uuid".to_string(),
            required: true,
            unique: false,
        };

        let user_id_column = Column {
            name: "id".to_string(),
            datatype: "uuid".to_string(),
            required: true,
            unique: true,
        };
        let tables = vec![
            Table {
                columns: vec![
                    Column {
                        name: "content".to_string(),
                        datatype: "text".to_string(),
                        required: false,
                        unique: false,
                    },
                    author_col.to_owned(),
                ],
                primary_keys: vec![PrimaryKey(Column {
                    name: "id".to_string(),
                    datatype: "uuid".to_string(),
                    required: true,
                    unique: true,
                })],
                name: "posts".to_string(),
                toplevel_ops: vec![Op {
                    name: "get_post_by_id".to_string(),
                    return_type: ReturnType::Object,
                    args: vec![Arg {
                        name: "id".to_string(),
                        tpe: InputType::GraphQLID,
                    }],
                }],
            },
            Table {
                columns: vec![
                    Column {
                        name: "age".to_string(),
                        datatype: "integer".to_string(),
                        required: false,
                        unique: false,
                    },
                    Column {
                        name: "name".to_string(),
                        datatype: "text".to_string(),
                        required: false,
                        unique: false,
                    },
                ],
                primary_keys: vec![PrimaryKey(user_id_column.to_owned())],
                name: "users".to_string(),
                toplevel_ops: vec![Op {
                    name: "get_user_by_id".to_string(),
                    return_type: ReturnType::Object,
                    args: vec![Arg {
                        name: "id".to_string(),
                        tpe: InputType::GraphQLID,
                    }],
                }],
            },
        ];

        let relationships2 = vec![
            Relationship2 {
                table_name: "posts".to_string(),
                column_name: author_col.name.to_owned(),
                target_table_name: "users".to_string(),
                target_column_name: user_id_column.name.to_string(),
                field_name: "users".to_string(),
                return_type: ReturnType::Object,
                column_optional: false,
            },
            Relationship2 {
                table_name: "users".to_string(),
                column_name: user_id_column.name.to_string(),
                target_table_name: "posts".to_string(),
                target_column_name: author_col.name.to_string(),
                field_name: "posts".to_string(),
                return_type: ReturnType::Array,
                column_optional: false,
            },
        ];

        (tables, relationships2)
    }

    #[test]
    fn simple_table_not_all_columns_json() {
        let query = r#"
        query get_example {
            get_example_by_id(id: "hello") {
              id
              number
            }
          }

        "#;

        let parsed_query = parse_query(query).unwrap();

        let intermediate = to_intermediate(&parsed_query, &single_table(), &vec![]);

        let expected = vec![TableSelection {
            table_name: "example".to_string(),
            column_names: vec!["id".to_string(), "number".to_string()],
            left_joins: vec![],

            where_clauses: vec![WhereClause {
                left_path: None,
                left_column: "id".to_string(),
                expr: r#""hello""#.to_string(),
            }],
            return_type: ReturnType::Object,
            field_name: "get_example_by_id".to_string(),
            path: "root".to_string(),
        }];

        println!("{:#?}", intermediate);

        assert_eq!(intermediate, expected)
    }

    #[test]
    fn join_table_json() {
        let query = r#"
        query get_post_with_user {
            get_post_by_id(id: "hello") {
                id
                content
                user {
                    id
                    name
                }
            }
        }
        "#;

        let parsed_query = parse_query(query).unwrap();
        let (tables, relationships2) = two_tables();
        let intermediate = to_intermediate(&parsed_query, &tables, &relationships2);

        let expected = vec![TableSelection {
            table_name: "posts".to_string(),
            column_names: vec!["id".to_string(), "content".to_string()],
            left_joins: vec![LeftJoin {
                right_table: TableSelection {
                    table_name: "users".to_string(),
                    column_names: vec!["id".to_string(), "name".to_string()],
                    left_joins: vec![],
                    where_clauses: vec![{
                        WhereClause {
                            left_path: Some("root".to_string()),
                            left_column: "author".to_string(),
                            expr: r#""id""#.to_string(),
                        }
                    }],
                    return_type: ReturnType::Object,
                    field_name: "user".to_string(),

                    path: "root.user".to_string(),
                },
                join_conditions: vec![JoinCondition {
                    left_col: "users".to_string(),
                    right_col: "id".to_string(),
                }],
            }],
            where_clauses: vec![WhereClause {
                left_path: None,
                left_column: "id".to_string(),
                expr: r#""hello""#.to_string(),
            }],
            return_type: ReturnType::Object,
            field_name: "get_post_by_id".to_string(),
            path: "root".to_string(),
        }];

        assert_eq!(intermediate, expected);
    }

    #[test]
    fn join_sql() {
        let intermediate = TableSelection {
            table_name: "posts".to_string(),
            column_names: vec!["id".to_string(), "content".to_string()],
            left_joins: vec![LeftJoin {
                right_table: TableSelection {
                    table_name: "users".to_string(),
                    column_names: vec!["id".to_string(), "name".to_string()],
                    left_joins: vec![],
                    where_clauses: vec![{
                        WhereClause {
                            left_path: Some("root".to_string()),
                            left_column: "author".to_string(),
                            expr: r#""id""#.to_string(),
                        }
                    }],
                    return_type: ReturnType::Object,
                    field_name: "user".to_string(),

                    path: "root.user".to_string(),
                },
                join_conditions: vec![JoinCondition {
                    left_col: "users".to_string(),
                    right_col: "id".to_string(),
                }],
            }],
            where_clauses: vec![WhereClause {
                left_path: None,
                left_column: "id".to_string(),
                expr: r#""04510aac-ad26-48ee-b081-47ce2f5ee3f2""#.to_string(),
            }],
            return_type: ReturnType::Object,
            field_name: "get_post_by_id".to_string(),
            path: "root".to_string(),
        };

        let sql = intermediate.to_json_sql();

        let expected = r#"select
  coalesce((json_agg("root") -> 0), 'null') as "get_post_by_id"
from
  (
    select
      row_to_json(
        (
          select
            "json_spec"
          from
            (
              select
                "root_data"."id" as "id",
                "root_data"."content" as "content",
                "root.user"."user" as "user"
            ) as "json_spec"
        )
      ) as "root"
    from
      (
        select
          *
        from
          posts
        where
          "id" = "04510aac-ad26-48ee-b081-47ce2f5ee3f2"
      ) as "root_data"
      LEFT OUTER JOIN LATERAL (
        select
          coalesce((json_agg("root.user") -> 0), 'null') as "user"
        from
          (
            select
              row_to_json(
                (
                  select
                    "json_spec"
                  from
                    (
                      select
                        "root.user_data"."id" as "id",
                        "root.user_data"."name" as "name"
                    ) as "json_spec"
                )
              ) as "root.user"
            from
              (
                select
                  *
                from
                  users
                where
                  "root_data"."author" = "id"
              ) as "root.user_data"
          ) as "root.user"
      ) as "root.user" on ('true')
  ) as "root""#;

        assert_eq!(
            format(&sql, &QueryParams::None, FormatOptions::default()),
            expected
        )
    }

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

        let (tables, constraints, cols, references) = fetch_introspection_data(&pool).await?;
        let introspection = convert_introspect_data(tables, constraints, cols, references);

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

        println!("{}", executable_sql);

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
}
