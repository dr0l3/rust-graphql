use crate::postgres_introspect::PostgresFunctionReturn::{SetOfObject, SingleObject};
use crate::InputType::{GraphQLID, GraphQLInteger, GraphQLString};
use crate::{
    Arg, ArgType, Column, DatabaseFunction, DatabaseRelationship, DatabaseTable, Function,
    FunctionArg, InputType, Operation, OperationType, PostgresIndexedColumns,
    PostgresTableColummRow, PostgresTableReferencesRow, PostgresTableRow, PostgresTableViewRow,
    PrimaryKey, ReturnType, SelectableStuff, Table, TableUniqueConstraint,
};
use graphql_parser::query::Type::{ListType, NamedType, NonNullType};
use graphql_parser::schema::Definition::TypeDefinition;
use graphql_parser::schema::{
    Definition, Document, Field, InputValue, ObjectType, SchemaDefinition,
};
use itertools::Itertools;
use regex::Regex;
use sqlx::{Pool, Postgres};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct IntrospectionResult {
    pub tables: Vec<DatabaseTable>,
    pub relationships2: Vec<DatabaseRelationship>,
    pub functions: Vec<DatabaseFunction>,
    pub selectable_by_operation_name: HashMap<String, SelectableStuff>,
}

impl IntrospectionResult {
    pub fn to_schema(&self) -> Document<String> {
        let query_fields: Vec<Field<String>> = self
            .tables
            .iter()
            .flat_map(|table| table.toplevel_ops.iter().map(move |op| (table, op)))
            .map(|(table, op)| {
                let arguments = op
                    .args
                    .iter()
                    .map(|arg| {
                        let value_type = match arg.tpe {
                            InputType::GraphQLInteger { .. } => NamedType("Integer".to_string()),
                            InputType::GraphQLString { .. } => NamedType("String".to_string()),
                            InputType::GraphQLBoolean { .. } => NamedType("Bool".to_string()),
                            InputType::GraphQLID { .. } => NamedType("ID".to_string()),
                        };
                        InputValue {
                            position: Default::default(),
                            description: None,
                            name: arg.name.to_owned(),
                            value_type,
                            default_value: None,
                            directives: vec![],
                        }
                    })
                    .collect_vec();
                let name = op.name.to_owned();
                let field_type = match op.return_type {
                    ReturnType::Object => NamedType(table.name.to_owned()),
                    ReturnType::Array => ListType(Box::new(NonNullType(Box::new(NamedType(
                        table.name.to_owned(),
                    ))))),
                };
                Field {
                    position: Default::default(),
                    description: None,
                    name,
                    arguments,
                    field_type,
                    directives: vec![],
                }
            })
            .collect_vec();

        let table_types = self
            .tables
            .iter()
            .map(|table| table.to_graphql_object(&self.relationships2))
            .map(|tpe| {
                Definition::TypeDefinition(graphql_parser::schema::TypeDefinition::Object(tpe))
            })
            .collect_vec();

        let query_object = ObjectType {
            position: Default::default(),
            description: None,
            name: "query".to_string(),
            implements_interfaces: vec![],
            directives: vec![],
            fields: query_fields,
        };

        let query = graphql_parser::schema::TypeDefinition::Object(query_object);

        let schema: SchemaDefinition<String> = SchemaDefinition {
            position: Default::default(),
            directives: vec![],
            query: Some("query".to_string()),
            mutation: None,
            subscription: None,
        };

        let defs = [
            table_types,
            vec![
                Definition::TypeDefinition(query),
                Definition::SchemaDefinition(schema),
            ],
        ]
        .concat();

        Document { definitions: defs }
    }
}

pub async fn fetch_introspection_data(
    pool: &Pool<Postgres>,
) -> Result<
    (
        Vec<PostgresTableRow>,
        Vec<TableConstraint>,
        Vec<PostgresTableColummRow>,
        Vec<PostgresTableReferencesRow>,
        Vec<PostgresIndexedColumns>,
        Vec<PostgresFunction>,
    ),
    sqlx::Error,
> {
    let pg_tables = get_tables(pool).await?;
    let constraints = get_constraints(pool).await?;
    let columns = get_all_columns(pool).await?;
    let references = get_all_references(pool).await?;
    let indexes = get_indexes(pool).await?;
    let functions = get_functions(pool).await?;
    Ok((
        pg_tables,
        constraints,
        columns,
        references,
        indexes,
        functions,
    ))
}

pub fn postgres_data_type_to_input(data_type: &str) -> InputType {
    match data_type {
        "text" => GraphQLString { default: None },
        "int" => GraphQLInteger { default: None },
        "integer" => GraphQLInteger { default: None },
        "uuid" => GraphQLID { default: None },
        _ => panic!("fn postgres_data_type_to_input"),
    }
}

pub fn convert_introspect_data(
    pg_tables: Vec<PostgresTableRow>,
    constraints: Vec<TableConstraint>,
    columns: Vec<PostgresTableColummRow>,
    references: Vec<PostgresTableReferencesRow>,
    indexed_columns: Vec<PostgresIndexedColumns>,
    pg_functions: Vec<PostgresFunction>,
) -> IntrospectionResult {
    let introspection_results = pg_tables
        .iter()
        .filter(|table| {
            table
                .comment
                .as_ref()
                .map_or(true, |str| !str.contains("@ignore"))
        })
        .map(|table| {
            (
                table,
                columns
                    .iter()
                    .filter(|col| col.table_name.eq(&table.name))
                    .filter(|col| {
                        col.comment
                            .as_ref()
                            .map_or(true, |str| !str.contains("@ignore"))
                    })
                    .collect_vec(),
                constraints
                    .iter()
                    .filter(|constraint| constraint.table_name.eq(&table.name))
                    .collect_vec(),
            )
        })
        .collect_vec();

    let tables = introspection_results
        .iter()
        .map(|(table_row, column_rows, constraint_rows)| {
            let cols: Vec<Column> = column_rows
                .iter()
                .map(|col| Column {
                    name: col.column_name.to_string(),
                    datatype: col.data_type.to_string(),
                    required: col.is_nullable.contains("NO"),
                    unique: constraint_rows
                        .iter()
                        .filter(|contraint| {
                            contraint.constraint_type.contains("UNIQUE")
                                || contraint.constraint_type.contains("PRIMARY KEY")
                        })
                        .find(|constraint| {
                            constraint
                                .key_columns
                                .iter()
                                .contains(&col.column_name.to_string())
                        })
                        .is_some(),
                    graphql_name: col.get_graphql_name(),
                })
                .collect_vec();
            let pk_contraints = constraint_rows
                .iter()
                .filter(|constraint| constraint.constraint_type.contains("PRIMARY KEY"))
                .collect_vec();

            let primary_keys = cols
                .iter()
                .filter(|col| {
                    pk_contraints
                        .iter()
                        .find(|pk_contraint| pk_contraint.key_columns.iter().contains(&col.name))
                        .is_some()
                })
                .collect_vec();

            let regular_cols = cols
                .iter()
                .filter(|col| {
                    pk_contraints
                        .iter()
                        .find(|pk_contraint| pk_contraint.key_columns.iter().contains(&col.name))
                        .is_none()
                })
                .map(|col| col.to_owned().to_owned())
                .collect_vec();

            let non_pk_indexed_cols = indexed_columns
                .iter()
                .filter(|indexed_columns| {
                    indexed_columns.table_name == table_row.name
                        && indexed_columns.schema_name == table_row.schema
                })
                .filter(|index| match &index.column_names[..] {
                    index_col_names if index_col_names.len() == primary_keys.len() => {
                        !(index_col_names
                            == primary_keys
                                .iter()
                                .map(|cols| cols.name.to_owned())
                                .collect_vec())
                    }
                    _ => true,
                })
                .collect_vec();

            let function_ops: Vec<Operation> = pg_functions
                .iter()
                .filter(|function| match &function.return_type {
                    PostgresFunctionReturn::Scalar => false,
                    PostgresFunctionReturn::SingleObject { table_name } => {
                        table_name.eq(&table_row.name)
                    }
                    PostgresFunctionReturn::SetOfObject { table_name } => {
                        table_name.eq(&table_row.name)
                    }
                })
                .map(|function| {
                    let return_type = match function.return_type {
                        PostgresFunctionReturn::Scalar => ReturnType::Object, //TODO: Should never happne
                        PostgresFunctionReturn::SingleObject { .. } => ReturnType::Object,
                        PostgresFunctionReturn::SetOfObject { .. } => ReturnType::Array,
                    };

                    let return_type_optional = match function.return_type {
                        PostgresFunctionReturn::Scalar => true,
                        PostgresFunctionReturn::SingleObject { .. } => true, // TODO: Do we know?
                        PostgresFunctionReturn::SetOfObject { .. } => false,
                    };

                    Operation {
                        name: function.name.to_owned(),
                        return_type,
                        args: vec![],
                        operation_type: OperationType::Function,
                        return_type_optional,
                    }
                })
                .collect_vec();

            let indexed_cols_ops = non_pk_indexed_cols
                .iter()
                .map(|indexed_columns| {
                    let column_combination_unique = constraint_rows
                        .iter()
                        .filter(|constraint| constraint.constraint_type == "UNIQUE")
                        .map(|constraint| &constraint.key_columns)
                        .find(|cols| cols == &&indexed_columns.column_names)
                        .is_some();

                    let base_args = indexed_columns
                        .column_names
                        .iter()
                        .map(|col_name| {
                            let col = regular_cols
                                .iter()
                                .find(|col| col.name.eq(col_name))
                                .unwrap();
                            Arg {
                                name: col_name.to_owned(),
                                tpe: postgres_data_type_to_input(&col.datatype),
                                arg_type: ArgType::ColumnName,
                            }
                        })
                        .collect_vec();

                    let pagination_args = if column_combination_unique {
                        vec![]
                    } else {
                        vec![
                            Arg {
                                name: "limit".to_string(),
                                tpe: GraphQLInteger { default: Some(25) },
                                arg_type: ArgType::BuiltIn,
                            },
                            Arg {
                                name: "offset".to_string(),
                                tpe: GraphQLInteger { default: Some(0) },
                                arg_type: ArgType::BuiltIn,
                            },
                        ]
                    };

                    Operation {
                        name: format!(
                            "search_{}_by_{}",
                            indexed_columns.table_name,
                            indexed_columns.column_names.join("_")
                        ), //TODO: Improve naming
                        return_type: if column_combination_unique {
                            ReturnType::Object
                        } else {
                            ReturnType::Array
                        },
                        args: [base_args, pagination_args].concat(),
                        operation_type: OperationType::Table,
                        return_type_optional: true,
                    }
                })
                .collect_vec();

            let toplevel_ops: Vec<Operation> = if !primary_keys.is_empty() {
                let base = vec![
                    Operation {
                        name: format!("get_{}_by_id", table_row.get_name()),
                        return_type: ReturnType::Object,
                        args: primary_keys
                            .iter()
                            .map(|pk| {
                                println!("{:?}", pk);

                                Arg {
                                    name: pk.name.to_owned(),
                                    tpe: postgres_data_type_to_input(&pk.datatype),
                                    arg_type: ArgType::ColumnName,
                                }
                            })
                            .collect_vec(),
                        operation_type: OperationType::Table,
                        return_type_optional: true,
                    },
                    Operation {
                        name: format!("list_{}", table_row.get_name()),
                        return_type: ReturnType::Array,
                        args: vec![
                            Arg {
                                name: "limit".to_string(),
                                tpe: GraphQLInteger { default: Some(25) },
                                arg_type: ArgType::BuiltIn,
                            },
                            Arg {
                                name: "offset".to_string(),
                                tpe: GraphQLInteger { default: Some(0) },
                                arg_type: ArgType::BuiltIn,
                            },
                        ],
                        operation_type: OperationType::Table,
                        return_type_optional: false,
                    },
                    // Operation {
                    //     name: format!("search_{}", table.name),
                    //     return_type: ReturnType::Array,
                    //     args: vec![],
                    //     operation_type: OperationType::Table,
                    //     return_type_optional: false,
                    // },
                ];

                [base, indexed_cols_ops, function_ops].concat()
            } else {
                vec![]
            };

            DatabaseTable {
                name: table_row.name.to_string(),
                columns: regular_cols,
                primary_keys: primary_keys
                    .iter()
                    .map(|col| PrimaryKey(col.to_owned().to_owned()))
                    .collect_vec(),
                toplevel_ops,
                graphql_name: None,
            }
        })
        .collect_vec();

    let functions: Vec<DatabaseFunction> = pg_functions
        .iter()
        .map(|pg_function| {
            let table = tables
                .iter()
                .find(|table| match &pg_function.return_type {
                    PostgresFunctionReturn::Scalar => false,
                    PostgresFunctionReturn::SingleObject { table_name } => {
                        table.name.eq(table_name)
                    }
                    PostgresFunctionReturn::SetOfObject { table_name } => table.name.eq(table_name),
                })
                .expect("Unable to find matching table");

            let args = pg_function
                .args
                .iter()
                .map(|pg_arg| {
                    let arg_type = pg_arg.to_graphql_arg().tpe;
                    FunctionArg {
                        name: pg_arg.arg_name.to_string(),
                        arg_type,
                    }
                })
                .collect_vec();

            DatabaseFunction {
                name: pg_function.name.to_owned(),
                table: table.to_owned(),
                args,
            }
        })
        .collect_vec();

    let relationships2 = references
        .iter()
        .flat_map(|reference| {
            let table = tables
                .iter()
                .find(|table| table.name.contains(&reference.table_name))
                .expect("Unable to find table")
                .to_owned();
            let column = table
                .get_all_columns()
                .find(|col| col.name.contains(&reference.column_name))
                .expect("Unable to find column")
                .to_owned();
            let foreign_table = tables
                .iter()
                .find(|table| table.name.contains(&reference.foreign_table_name))
                .expect("Unable to find foreign table")
                .to_owned();
            let foreign_column = foreign_table
                .get_all_columns()
                .find(|col| col.name.contains(&reference.foreign_column_name))
                .expect("Unable to find column")
                .to_owned();

            let field_name = format!("{}", table.name); // TODO: Better
            let return_type = if foreign_column.unique {
                ReturnType::Object
            } else {
                ReturnType::Array
            };

            let second_return_type = if column.unique {
                ReturnType::Object
            } else {
                ReturnType::Array
            };

            let table_relationships = vec![
                DatabaseRelationship {
                    table_name: table.name.to_string(),
                    column_name: column.name.to_string(),
                    target_table_name: foreign_table.name.to_string(),
                    target_column_name: foreign_column.name.to_string(),
                    field_name: format!(
                        "{}",
                        foreign_table
                            .graphql_name
                            .as_ref()
                            .unwrap_or(&foreign_table.name)
                            .to_string()
                    ),
                    return_type,
                    column_optional: !column.required,
                },
                DatabaseRelationship {
                    table_name: foreign_table.name.to_string(),
                    column_name: foreign_column.name.to_string(),
                    target_table_name: table.name.to_string(),
                    target_column_name: column.name.to_string(),
                    field_name: format!("{}", table.graphql_name.as_ref().unwrap_or(&table.name)), // TODO: Better
                    return_type: second_return_type,
                    column_optional: !foreign_column.required,
                },
            ];

            table_relationships
        })
        .collect_vec();

    println!("{:#?}", functions);

    let table_iter = tables.iter().flat_map(|table| {
        table.toplevel_ops.iter().map(|op| {
            println!("{}", op.name);
            match op.operation_type {
                OperationType::Table => (
                    op.name.to_owned(),
                    Table {
                        table: table.to_owned(),
                    },
                ),
                OperationType::Function => (
                    op.name.to_owned(),
                    Function {
                        function: functions
                            .iter()
                            .find(|function| function.name.eq(&op.name))
                            .expect("unable to find referenced function")
                            .to_owned(),
                    },
                ),
            }
        })
    });

    let something: HashMap<String, SelectableStuff> = table_iter.collect();

    IntrospectionResult {
        tables,
        relationships2,
        functions,
        selectable_by_operation_name: something,
    }
}

async fn get_tables(pool: &Pool<Postgres>) -> Result<Vec<PostgresTableRow>, sqlx::Error> {
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
    pg_catalog.obj_description(c.oid, 'pg_class') as "comment"
  FROM pg_catalog.pg_class c
       LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r','p','s','')
        AND n.nspname = 'public'
  ORDER BY 1,2;"#;
    Ok(sqlx::query_as::<_, PostgresTableRow>(sql)
        .fetch_all(pool)
        .await?)
}

async fn get_all_columns(
    pool: &Pool<Postgres>,
) -> Result<Vec<PostgresTableColummRow>, sqlx::Error> {
    let sql = r#"
    select 
    *, 
    pg_catalog.col_description(format('%s.%s',cols.table_schema,cols.table_name)::regclass::oid,cols.ordinal_position) as comment
    FROM information_schema.columns cols
    WHERE table_schema = 'public'
       ;"#;

    Ok(sqlx::query_as::<_, PostgresTableColummRow>(sql)
        .fetch_all(pool)
        .await?)
}
async fn get_indexes(pool: &Pool<Postgres>) -> Result<Vec<PostgresIndexedColumns>, sqlx::Error> {
    let sql = "select
    t.relname as table_name,
    i.relname as index_name,
    ns.nspname as schema_name,
    array_agg(a.attname) as column_names,
    pg_get_indexdef(i.oid) as index_def
from
    pg_class t,
    pg_class i,
    pg_index ix,
    pg_attribute a,
    pg_namespace ns
where
    t.oid = ix.indrelid
    and i.oid = ix.indexrelid
    and a.attrelid = t.oid
    and a.attnum = ANY(ix.indkey)
    and t.relkind = 'r'
    and i.relnamespace = ns.oid
    and ns.nspname = 'public'
group by
    ns.nspname,
    t.relname,
    i.relname,
    i.oid   
order by
    t.relname,
    i.relname;
    ";

    let indexes = sqlx::query_as::<_, PostgresIndexedColumns>(&sql)
        .fetch_all(pool)
        .await?;

    Ok(indexes)
}

async fn get_columns(
    pool: &Pool<Postgres>,
    table: &String,
) -> Result<Vec<PostgresTableColummRow>, sqlx::Error> {
    let sql = "SELECT *
    FROM information_schema.columns
   WHERE table_schema = 'public'
     AND table_name   = $1
       ;";
    println!("fetching columns for table {:#?}", table);

    Ok(sqlx::query_as::<_, PostgresTableColummRow>(sql)
        .bind(table)
        .fetch_all(pool)
        .await?)
}

async fn get_references(
    pool: &Pool<Postgres>,
    table: &String,
) -> Result<Vec<PostgresTableReferencesRow>, sqlx::Error> {
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

async fn get_all_references(
    pool: &Pool<Postgres>,
) -> Result<Vec<PostgresTableReferencesRow>, sqlx::Error> {
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
WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema='public';";

    Ok(sqlx::query_as(sql).fetch_all(pool).await?)
}

async fn get_views(pool: &Pool<Postgres>) -> Result<Vec<PostgresTableViewRow>, sqlx::Error> {
    let sql = "select * from information_schema.views where table_schema = 'public';";

    Ok(sqlx::query_as(sql).fetch_all(pool).await?)
}

async fn get_unique_constraints(
    pool: &Pool<Postgres>,
) -> Result<Vec<TableUniqueConstraint>, sqlx::Error> {
    let sql = "SELECT
    n.nspname,
    c.relname,
    a.attname,
    (i.indisunique IS TRUE) AS part_of_unique_index
FROM pg_class c
    INNER JOIN pg_namespace n ON n.oid = c.relnamespace
    INNER JOIN pg_attribute a ON a.attrelid = c.oid
    LEFT JOIN pg_index i 
        ON i.indrelid = c.oid 
            AND a.attnum = ANY (i.indkey[0:(i.indnkeyatts - 1)])
WHERE a.attnum > 0 AND n.nspname = 'public' AND i.indisunique IS TRUE;";

    Ok(sqlx::query_as(sql).fetch_all(pool).await?)
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct TableConstraint {
    table_name: String,
    key_columns: Vec<String>,
    constraint_type: String,
}

async fn get_constraints(pool: &Pool<Postgres>) -> Result<Vec<TableConstraint>, sqlx::Error> {
    let sql = "select kcu.table_schema,
       kcu.table_name,
       tco.constraint_name,
       tco.constraint_type,
       array_agg(kcu.column_name::text) as key_columns
from information_schema.table_constraints tco
join information_schema.key_column_usage kcu
     on kcu.constraint_name = tco.constraint_name
     and kcu.constraint_schema = tco.constraint_schema
     and kcu.constraint_name = tco.constraint_name
where kcu.table_schema= 'public'
group by
     kcu.table_schema,
     kcu.table_name,
     tco.constraint_name,
     tco.constraint_type
order by kcu.table_schema,
         kcu.table_name;";

    Ok(sqlx::query_as(sql).fetch_all(pool).await?)
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum PostgresFunctionArgType {
    IN,
    OUT,
    INOUT,
}
#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct PostgresFunctionArg {
    arg_name: String,
    type_name: String,
    arg_type: PostgresFunctionArgType,
}

impl PostgresFunctionArg {
    fn to_graphql_arg(&self) -> Arg {
        let tpe = postgres_data_type_to_input(&self.type_name);
        Arg {
            name: self.arg_name.to_owned(),
            tpe,
            arg_type: ArgType::BuiltIn,
        }
    }
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub enum PostgresFunctionReturn {
    Scalar,
    SingleObject { table_name: String },
    SetOfObject { table_name: String },
}

#[derive(Ord, PartialOrd, Eq, PartialEq, Debug, Clone)]
pub struct PostgresFunction {
    name: String,
    args: Vec<PostgresFunctionArg>,
    return_type: PostgresFunctionReturn,
}

#[derive(sqlx::FromRow)]
struct PostgresFunctionRow {
    function_name: String,
    strict: bool,
    prokind: String,
    provolatile: String,
    proretset: bool,
    typename: String,
    args: String,
}

impl PostgresFunctionRow {
    fn extract_args(&self) -> Vec<PostgresFunctionArg> {
        let re = Regex::new(r"(?P<INOUT>(INOUT|OUT))? ?(?P<NAME>\w*) (?P<TYPE>\w*)").unwrap();
        self.args
            .split(",")
            .into_iter()
            .filter(|str| !str.is_empty())
            .map(|str| {
                let captures = re.captures(str).unwrap();
                let arg_type = match captures
                    .name("INOUT")
                    .map(|v| v.as_str().to_owned())
                    .unwrap_or("IN".parse().unwrap())
                    .as_str()
                {
                    "IN" => PostgresFunctionArgType::IN,
                    "OUT" => PostgresFunctionArgType::OUT,
                    "INOUT" => PostgresFunctionArgType::INOUT,
                    _ => {
                        panic!("Unable to parse argype")
                    }
                };
                let arg_name = captures.name("NAME").unwrap().as_str().to_owned();
                let type_name = captures.name("TYPE").unwrap().as_str().to_owned();
                PostgresFunctionArg {
                    arg_name,
                    type_name,
                    arg_type,
                }
            })
            .collect_vec()
    }

    fn to_postgres_function(&self) -> PostgresFunction {
        let return_type = if self.proretset {
            SetOfObject {
                table_name: self.typename.to_owned(),
            }
        } else {
            //TODO: check if builtin type
            SingleObject {
                table_name: self.typename.to_owned(),
            }
        };

        let args = self.extract_args();

        PostgresFunction {
            name: self.function_name.to_owned(),
            args,
            return_type,
        }
    }
}

async fn get_functions(pool: &Pool<Postgres>) -> Result<Vec<PostgresFunction>, sqlx::Error> {
    let sql = "select
    pg_proc.oid as function_oid,
    proname as function_name,
    proisstrict as strict,
    prokind::text,
    provolatile::text,
    pg_proc.proretset,
    pg_type.typname as typename,
    pg_get_function_arguments(pg_proc.oid) as args
from pg_proc
         left join pg_namespace on pg_proc.pronamespace = pg_namespace.oid
left join pg_type on pg_proc.prorettype = pg_type.oid

where proname not like 'pgp_%'
  and proname NOT IN
      ('armor', 'crypt', 'dearmor', 'decrypt', 'decrypt_iv', 'digest', 'encrypt', 'encrypt_iv',
       'gen_random_bytes', 'gen_random_uuid', 'gen_salt', 'hmac'
          )
  AND pg_namespace.nspname NOT LIKE 'pg_%'
  AND pg_namespace.nspname NOT IN ('information_schema', 'hdb_catalog')
  AND (NOT EXISTS(
        SELECT 1
        FROM pg_aggregate
        WHERE ((pg_aggregate.aggfnoid) :: oid = pg_namespace.oid)
    )
    );";

    let rows = sqlx::query_as::<_, PostgresFunctionRow>(sql)
        .fetch_all(pool)
        .await?;

    Ok(rows
        .iter()
        .map(|row| row.to_postgres_function())
        .collect_vec())
}

#[derive(sqlx::FromRow, Debug)]
pub struct PostgresBuiltInTypeRow {
    schema: String,
    name: String,
}

async fn get_built_in_types(
    pool: &Pool<Postgres>,
) -> Result<Vec<PostgresBuiltInTypeRow>, sqlx::Error> {
    let sql = "SELECT      n.nspname as schema, t.typname as name
FROM        pg_type t
                LEFT JOIN   pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE       (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
  AND     NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid);";

    sqlx::query_as::<_, PostgresBuiltInTypeRow>(sql)
        .fetch_all(pool)
        .await
}
