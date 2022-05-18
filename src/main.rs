extern crate dotenv;

use async_graphql::{
    parser::parse_query,
    parser::types::{ExecutableDocument, Field, OperationDefinition, Selection},
    registry::MetaField,
    *,
};
use dotenv::dotenv;
use futures::future::*;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::borrow::Borrow;
use std::env;
use std::fmt::format;
use uuid::*;
use itertools::Itertools;

#[derive(Debug, Clone)]
struct Table {
    name: String,
    columns: Vec<Column>,
    primary_keys: Vec<PrimaryKey>,
    toplevel_ops: Vec<String>,
}

impl Table {
    fn get_all_columns(&self) -> impl Iterator<Item = &Column> {
        self.columns.iter().chain(self.primary_keys.iter().map(|pk|&pk.0))
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
    OneToOne { foreign_table_name: String, foreign_column_name: String, relationship_name: String },
    OneToMany { foreign_table_name: String, foreign_column_name: String, relationship_name: String },
    ManyToOne { foreign_table_name: String, foreign_column_name: String, relationship_name: String },
}

struct TableRelationship {
    table: Table,
    column: Column,
    foreign_table: Table,
    foreign_column: Column,
    constraint_name: String,
    field_name: String,
}

impl Relationship {
    fn get_relationship_name(&self) -> String {
        match self {
            Relationship::OneToOne { relationship_name, .. } => { relationship_name.to_string() }
            Relationship::OneToMany { relationship_name, .. } => { relationship_name.to_string() }
            Relationship::ManyToOne { relationship_name, .. } => { relationship_name.to_string() }
        }
    }
}

#[derive(sqlx::FromRow, Debug, Clone)]
struct TableRow {
    schema: String,
    name: String,
    table_type: String,
    owner: String,
    size: String,
    description: Option<String>,
}

#[derive(sqlx::FromRow, Debug, Clone)]
struct TableColumm {
    column_name: String,
    table_name: String,
    is_nullable: String,
    data_type: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
struct TableReferences {
    table_schema: String,
    table_name: String,
    column_name: String,
    foreign_table_schema: String,
    foreign_table_name: String,
    foreign_column_name: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
struct TableView {
    table_schema: String,
    table_name: String,
    is_updatable: String,
    is_insertable_into: String,
}

#[derive(Debug)]
struct User {
    id: Uuid,
    name: Option<String>,
    email: String,
}

struct Conversation {
    id: Uuid,
    requester: Uuid,
    state: String,
}

struct Message {
    id: Uuid,
    conversation_id: Uuid,
    content: String,
    author: Uuid,
}

struct Queue {
    id: Uuid,
    name: String,
}

struct QueuedConversation {
    csid: Uuid,
    queue_id: Uuid,
}

struct Test {
    something: String,
    something_else: String,
}

#[Object]
impl Test {
    async fn something(&self) -> String {
        println!("Fetching something");
        self.something.to_string()
    }

    async fn something_else(&self) -> String {
        println!("Fetching something else");
        self.something_else.to_string()
    }
}

async fn get_tables(pool: &Pool<Postgres>) -> Result<Vec<TableRow>, sqlx::Error> {
    let sql = r#"SELECT n.nspname as "schema",
    c.relname as "name",
    CASE c.relkind 
      WHEN 'r' THEN 'table' 
      WHEN 'v' THEN 'view' 
      WHEN 'm' THEN 'materialized view' 
      WHEN 'i' THEN 'index' 
      WHEN 'S' THEN 'sequence' 
      WHEN 's' THEN 'special' 
      WHEN 'f' THEN 'foreign table' 
      WHEN 'p' THEN 'partitioned table' 
      WHEN 'I' THEN 'partitioned index' 
    END as "table_type",
    pg_catalog.pg_get_userbyid(c.relowner) as "owner",
    pg_catalog.pg_size_pretty(pg_catalog.pg_table_size(c.oid)) as "size",
    pg_catalog.obj_description(c.oid, 'pg_class') as "description"
  FROM pg_catalog.pg_class c
       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r','p','s','')
        AND n.nspname = 'public'
  ORDER BY 1,2;"#;
    Ok(sqlx::query_as::<_, TableRow>(sql).fetch_all(pool).await?)
}

async fn get_columns(
    pool: &Pool<Postgres>,
    table: String,
) -> Result<Vec<TableColumm>, sqlx::Error> {
    let sql = "SELECT *
    FROM information_schema.columns
   WHERE table_schema = 'public'
     AND table_name   = $1
       ;";
    println!("fetching columns for table {:#?}", table);

    Ok(sqlx::query_as::<_, TableColumm>(sql)
        .bind(table)
        .fetch_all(pool)
        .await?)
}

async fn get_references(
    pool: &Pool<Postgres>,
    table: String,
) -> Result<Vec<TableReferences>, sqlx::Error> {
    let sql = "SELECT
    tc.table_schema, 
    tc.constraint_name, 
    tc.table_name, 
    kcu.column_name, 
    ccu.table_schema AS foreign_table_schema,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name 
FROM 
    information_schema.table_constraints AS tc 
    JOIN information_schema.key_column_usage AS kcu
      ON tc.constraint_name = kcu.constraint_name
      AND tc.table_schema = kcu.table_schema
    JOIN information_schema.constraint_column_usage AS ccu
      ON ccu.constraint_name = tc.constraint_name
      AND ccu.table_schema = tc.table_schema
WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name=$1;";

    Ok(sqlx::query_as(sql).bind(table).fetch_all(pool).await?)
}

async fn get_views(pool: &Pool<Postgres>) -> Result<Vec<TableView>, sqlx::Error> {
    let sql = "select * from information_schema.views where table_schema = 'public';";

    Ok(sqlx::query_as(sql).fetch_all(pool).await?)
}

struct Query;

struct JsonValue(serde_json::Value);

struct JsonResponse {
    value: JsonValue,
    type_name: String,
    type_info: String,
}

async fn compute<'life1, 'life2, 'life3>(
    ctx: &'life1 ContextSelectionSet<'life2>,
    field: &'life3 Positioned<parser::types::Field>,
) -> Result<Value, ServerError> {
    println!("{:#?}", "hello");
    println!("{:#?}", field);

    Ok(async_graphql::Value::String("".to_string()))
}

struct ConversationValue(serde_json::Value);

/*impl OutputType for ConversationValue {
    fn type_name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Conversation")
    }

    fn create_type_info(registry: &mut registry::Registry) -> String {
        let fields: indexmap::IndexMap<String, async_graphql::registry::MetaField> =
            indexmap::IndexMap::new();


        registry.create_output_type::<ConversationValue, _>(registry::MetaTypeId::Object, |_| {
            registry::MetaType::Object {
                name: "Converation".to_string(),
                description: Some("A Conversation"),
                fields: fields,
                cache_control: CacheControl {
                    public: false,
                    max_age: 0,
                },
                extends: false,
                keys: None,
                visible: None,
                is_subscription: false,
                rust_typename: "ConversationValue",
            }
        });
        "Conversation".to_string()
    }

    fn resolve<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        ctx: &'life1 ContextSelectionSet<'life2>,
        field: &'life3 Positioned<parser::types::Field>,
    ) -> core::pin::Pin<
        Box<
            dyn core::future::Future<Output = ServerResult<Value>>
                + core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        todo!()
    }
}

impl OutputType for JsonValue {
    fn type_name() -> std::borrow::Cow<'static, str> {
        Self::type_name()
        //std::borrow::Cow::Borrowed("hehe")
    }

    fn qualified_type_name() -> String {
        format!("{}!", Self::type_name())
    }

    fn introspection_type_name(&self) -> std::borrow::Cow<'static, str> {
        Self::type_name()
    }

    fn create_type_info(registry: &mut registry::Registry) -> String {
        "todo!()".to_string()
    }

    fn resolve<'life0, 'life1, 'life2, 'life3, 'async_trait>(
        &'life0 self,
        ctx: &'life1 ContextSelectionSet<'life2>,
        field: &'life3 Positioned<parser::types::Field>,
    ) -> core::pin::Pin<
        Box<
            dyn core::future::Future<Output = ServerResult<Value>>
                + core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        'life3: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(compute(ctx, field))
    }
}*/

#[Object]
impl Query {
    async fn add(&self, ctx: &Context<'_>, a: i32, b: i32) -> i32 {
        let meh = ctx.field();
        println!("{:#?}", meh);
        a + b
    }
}

#[derive(Debug)]
enum Whatever {
    Hello,
    World,
    This,
    Is,
    Dog,
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let schema = Schema::new(Query, EmptyMutation, EmptySubscription);

    let res = schema.execute("{ add(a: 10, b: 20)}").await;

    println!("{:#?}", res);

    println!("{}", &schema.sdl());

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
        toplevel_ops: vec!["get_example_by_id".to_string()],
    }];

    let sql = to_sql(wut, tables);

    let something: Whatever = todo!();

    println!("{:#?}", something);

    match something {
        Whatever::Hello => {}
        Whatever::World => {}
        Whatever::This => {}
        Whatever::Is => {}
        Whatever::Dog => {}
    }

    println!("{:#?}", sql);

    Ok(())
}

#[derive(Debug, Eq, PartialEq, Ord, PartialOrd)]
struct TableSelection {
    table_name: String,
    column_names: Vec<String>,
    left_joins: Vec<LeftJoin>,
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

impl TableSelection {
    fn to_simple_sql(&self) -> String {
        format!(
            "select {} from {}",
            self.column_names.join(","),
            self.table_name
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

    fn to_json_sql(&self) -> String {
        let data_id = "hehe".to_string();
        let data_select = self.to_simple_sql();
        let column_lines = self
            .column_names
            .iter()
            .map(|col_name| format!(r#"select "{}"."{}" as "{}" "#, data_id, col_name, col_name).to_string())
            .join(",\n");

        format!(
            r#"
         select row_to_json(
            {column_lines}
         )
         from (
           {data_select} as {data_id}
         )
        "#,
            data_select = data_select,
            data_id = data_id,
            column_lines = column_lines
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
    Relationship,
}


fn field_to_table_selection(field: &Field, tables: &Vec<Table>, search_place: &SearchPlace, relationships: &Vec<TableRelationship>) -> TableSelection {
    let table =match search_place {
        SearchPlace::TopLevel => {
            tables.iter().find(|table| table.toplevel_ops.contains(&field.response_key().node.to_string()))
        }
        SearchPlace::Relationship => {
            relationships
                .iter()
                .find(|relationship| relationship.field_name.eq(&field.name.node.to_string()))
                .map(|rel| &rel.foreign_table)
        }
    }.expect("No table found");

    let columns: Vec<String> = field
        .selection_set
        .node
        .items
        .iter()
        .map(|Positioned { node, .. }| inner(node))
        .map(|f| f.response_key().node.to_string())
        .filter(|col_name| table.get_all_columns().find(|col| col.name.eq(col_name)).is_some())
        .collect();

    let joins = field
        .selection_set
        .node
        .items
        .iter()
        .map(|Positioned { node, .. }| inner(node))
        .filter(|field| field.selection_set.node.items.len() > 0)
        .collect::<Vec<Field>>();

    let selected_joins = joins
        .iter()
        .map(|field| {
            (field_to_table_selection(field, tables, &SearchPlace::Relationship, relationships), field)
        }).map(|(joined_table, field)| {
        let join_relationship = relationships.iter().find(|relationship| {
            relationship.table.name == table.name &&
                relationship.foreign_table.name == joined_table.table_name &&
                relationship.field_name == field.name.node.to_string()
        }).expect("Unable to find relationship");

        LeftJoin {
            right_table: joined_table,
            join_conditions: vec![JoinCondition {
                left_col: join_relationship.column.name.to_owned(),
                right_col: join_relationship.foreign_column.name.to_owned(),
            }],
        }
    }).collect_vec();
    TableSelection {
        left_joins: selected_joins,
        column_names: columns,
        table_name: table.name.to_owned(),
    }
}

fn to_intermediate(query: &ExecutableDocument, tables: &Vec<Table>, relationships: &Vec<TableRelationship>) -> Vec<TableSelection> {
    let ops = query
        .operations
        .iter()
        .map(|(_, Positioned { node, .. })| node.to_owned())
        .collect::<Vec<OperationDefinition>>();

    ops.iter()
        .flat_map(|operation_definition| {
            operation_definition.selection_set.node.items.iter().map(
                |Positioned { node, .. }| match node {
                    Selection::Field(Positioned { node, .. }) => {
                        let table = &tables
                            .iter()
                            .find(|table| {
                                table
                                    .toplevel_ops
                                    .contains(&node.response_key().node.to_string())
                            })
                            .expect("No table matching top level ops");

                        let columns: Vec<String> = node
                            .selection_set
                            .node
                            .items
                            .iter()
                            .map(|Positioned { node, .. }| inner(node))
                            .map(|f| f.response_key().node.to_string())
                            .filter(|col_name| {
                                table.columns
                                    .iter()
                                    .chain(table.primary_keys
                                        .iter()
                                        .map(|pk| &pk.0))
                                    .find(|col| col.name.eq(col_name))
                                    .is_some()
                            })
                            .collect();

                        let joins = node
                            .selection_set
                            .node
                            .items
                            .iter()
                            .map(|Positioned { node, .. }| inner(node))
                            .filter(|field| field.selection_set.node.items.len() > 0)
                            .map(|field| (field_to_table_selection(&field, tables, &SearchPlace::Relationship, relationships), field))
                            .map(|(tableselection, field)| {
                                let join_relationship = relationships.iter().find(|relationship| {
                                    relationship.table.name == table.name &&
                                        relationship.foreign_table.name == tableselection.table_name &&
                                        relationship.field_name == field.name.node.to_string()
                                }).expect("Unable to find relationship");

                                LeftJoin {
                                    right_table: tableselection,
                                    join_conditions: vec![JoinCondition {
                                        right_col: join_relationship.foreign_column.name.to_string(),
                                        left_col: join_relationship.column.name.to_string(),
                                    }],
                                }
                            })
                            .collect_vec();

                        TableSelection {
                            table_name: table.name.to_string(),
                            column_names: columns,
                            left_joins: joins,
                        }
                    }
                    Selection::FragmentSpread(_) => todo!(),
                    Selection::InlineFragment(_) => todo!(),
                },
            )
        }).collect_vec()
}


#[cfg(test)]
mod tests {
    use crate::{to_sql, Relationship, to_intermediate, SearchPlace, TableRelationship, TableSelection, LeftJoin, JoinCondition};

    use crate::Column;
    use crate::PrimaryKey;
    use crate::Table;
    use async_graphql::parser::parse_query;
    use serde_json::to_string;

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
            toplevel_ops: vec!["get_example_by_id".to_string()],
        }]
    }

    fn two_tables() -> (Vec<Table>, Vec<TableRelationship>) {
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
                columns: vec![Column {
                    name: "content".to_string(),
                    datatype: "text".to_string(),
                    required: false,
                    unique: false,
                }, author_col.to_owned()],
                primary_keys: vec![PrimaryKey(Column {
                    name: "id".to_string(),
                    datatype: "uuid".to_string(),
                    required: true,
                    unique: true,
                })],
                name: "posts".to_string(),
                toplevel_ops: vec!["get_post_by_id".to_string()],
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
                toplevel_ops: vec!["get_user_by_id".to_string()],
            },
        ];

        let relationships = vec![TableRelationship {
            table: tables.first().unwrap().to_owned(),
            column: author_col,
            foreign_table: tables.last().unwrap().to_owned(),
            foreign_column: user_id_column.to_owned(),
            constraint_name: "".to_string(),
            field_name: "user".to_string(),
        }];

        (tables, relationships)
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
            column_names: vec![
                "id".to_string(),
                "number".to_string(),
            ],
            left_joins: vec![],
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
        let (tables, relationships) = two_tables();
        let intermediate = to_intermediate(&parsed_query, &tables, &relationships);

        let expected = vec![
            TableSelection {
                table_name: "posts".to_string(),
                column_names: vec![
                    "id".to_string(),
                    "content".to_string(),
                ],
                left_joins: vec![
                    LeftJoin {
                        right_table: TableSelection {
                            table_name: "users".to_string(),
                            column_names: vec![
                                "id".to_string(),
                                "name".to_string(),
                            ],
                            left_joins: vec![],
                        },
                        join_conditions: vec![
                            JoinCondition {
                                left_col: "author".to_string(),
                                right_col: "id".to_string(),
                            },
                        ],
                    },
                ],
            },
        ];

        assert_eq!(intermediate, expected);
    }
}
