mod postgres_introspect;

extern crate dotenv;

use async_graphql::{
    parser::parse_query,
    parser::types::{ExecutableDocument, Field, OperationDefinition, Selection},
    registry::MetaField,
    *,
};
use dotenv::dotenv;
use futures::future::*;
use itertools::Itertools;
use postgres_introspect::{convert_introspect_data, fetch_introspection_data};
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::borrow::Borrow;
use std::env;
use std::fmt::{format, Display, Formatter};
use std::ops::Add;
use uuid::*;

#[derive(Debug, Clone)]
struct Table {
    name: String,
    columns: Vec<Column>,
    primary_keys: Vec<PrimaryKey>,
    toplevel_ops: Vec<Op>,
}

#[derive(Debug, Clone)]
pub struct Op {
    name: String,
    return_type: ReturnType,
}

impl Table {
    fn get_all_columns(&self) -> impl Iterator<Item = &Column> {
        self.columns
            .iter()
            .chain(self.primary_keys.iter().map(|pk| &pk.0))
    }
}

#[derive(Debug, Clone)]
struct Column {
    name: String,
    datatype: String,
    required: bool,
    unique: bool,
}

#[derive(Debug, Clone)]
struct PrimaryKey(Column);

struct TableName(String);

struct ColumnName(String);

struct RelationshipName(String);

enum Relationship {
    OneToOne {
        table_name: String,
        column_name: String,
        foreign_table_name: String,
        foreign_column_name: String,
        relationship_name: String,
    },
    OneToMany {
        table_name: String,
        column_name: String,
        foreign_table_name: String,
        foreign_column_name: String,
        relationship_name: String,
    },
    ManyToOne {
        foreign_table_name: String,
        foreign_column_name: String,
        relationship_name: String,
        table_name: String,
        column_name: String,
    },
}

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

impl Relationship {
    fn get_relationship_name(&self) -> String {
        match self {
            Relationship::OneToOne {
                relationship_name, ..
            } => relationship_name.to_string(),
            Relationship::OneToMany {
                relationship_name, ..
            } => relationship_name.to_string(),
            Relationship::ManyToOne {
                relationship_name, ..
            } => relationship_name.to_string(),
        }
    }
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

fn test_something() -> Result<(), String> {
    let wut = parse_query(
        std::fs::read_to_string("./example_getby_id.graphql").map_err(|err| err.to_string())?,
    )
    .map_err(|err| err.to_string())?;

    println!("{:#?}", wut);

    let tables = vec![Table {
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
        }],
    }];
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), String> {
    //test_something();

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://postgres:postgres@0.0.0.0:5439/dixa")
        .map_err(|err| err.to_string())
        .await?;
    let (tables, constraints, columns, references) = fetch_introspection_data(&pool)
        .map_err(|err| err.to_string())
        .await?;
    let results =
        convert_introspect_data(tables, constraints, columns, references).map_err(|err| "Wut")?;

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
enum ReturnType {
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
                    format!(r#"{}"{}" = "{}""#, path_string, left_column, expr)
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
            vec![]
        }
        SearchPlace::Relationship { relationship, path } => {
            vec![WhereClause {
                left_path: Some(path.to_string()),
                left_column: relationship.column_name.to_string(),
                expr: relationship.target_column_name.to_string(),
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
        to_intermediate, JoinCondition, LeftJoin, Op, Relationship, Relationship2, ReturnType,
        SearchPlace, TableRelationship, TableSelection, WhereClause,
    };

    use crate::Column;
    use crate::PrimaryKey;
    use crate::Table;
    use async_graphql::parser::parse_query;

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
        query get_example($id: Uuid!) {
            get_example_by_id(id: $id) {
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

            where_clauses: vec![],
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
        query get_post_with_user($id: Uuid!) {
            get_post_by_id(id: $id) {
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
                            expr: "id".to_string(),
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
            where_clauses: vec![],
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
                            expr: "id".to_string(),
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
                expr: "04510aac-ad26-48ee-b081-47ce2f5ee3f2".to_string(),
            }],
            return_type: ReturnType::Object,
            field_name: "get_post_by_id".to_string(),
            path: "root".to_string(),
        };

        let sql = intermediate.to_json_sql();

        let expected = r#"
        select coalesce(json_agg("root", 'null') as "root"
        from (
            select row_to_json(
                select "json_spec"
                from (
                    select "get_post_by_id"."id" as"id" ,
select "get_post_by_id"."content" as"content"
                ) as "json_spec"
            )
            from (
                select id,content from posts where  as "root"
                
            LEFT OUTER JOIN LATERAL (

        select coalesce(json_agg("root.user", 'null') as "root.user"
        from (
            select row_to_json(
                select "json_spec"
                from (
                    select "user"."id" as"id" ,
select "user"."name" as"name"
                ) as "json_spec"
            )
            from (
                select id,name from users where "author" = id as "root.user"

            )
        ) as "root.user"

            ) as user on ('true')

            )
        ) as "root""#;

        println!("{}", sql);
        println!("{}", expected);
    }
}
