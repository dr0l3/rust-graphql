use crate::InputType::{GraphQLID, GraphQLInteger, GraphQLString};
use crate::{
    ok, Arg, ArgType, Column, Op, PrimaryKey, Relationship2, ReturnType, Table, TableColumm,
    TableReferences, TableRelationship, TableRow, TableUniqueConstraint, TableView,
};
use itertools::Itertools;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

#[derive(Debug, Clone)]
pub struct IntrospectionResult {
    pub tables: Vec<Table>,
    pub relationships2: Vec<Relationship2>,
}

pub async fn fetch_introspection_data(
    pool: &Pool<Postgres>,
) -> Result<
    (
        Vec<TableRow>,
        Vec<TableConstraint>,
        Vec<TableColumm>,
        Vec<TableReferences>,
    ),
    sqlx::Error,
> {
    let pg_tables = get_tables(pool).await?;
    let constraints = get_constraints(pool).await?;
    let columns = get_all_columns(pool).await?;
    let references = get_all_references(pool).await?;
    Ok((pg_tables, constraints, columns, references))
}

pub fn convert_introspect_data(
    pg_tables: Vec<TableRow>,
    constraints: Vec<TableConstraint>,
    columns: Vec<TableColumm>,
    references: Vec<TableReferences>,
) -> IntrospectionResult {
    let introspection_results = pg_tables
        .iter()
        .map(|table| {
            (
                table,
                columns
                    .iter()
                    .filter(|col| col.table_name.eq(&table.name))
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
        .map(|(table, columns, constraints)| {
            let cols: Vec<Column> = columns
                .iter()
                .map(|col| Column {
                    name: col.column_name.to_string(),
                    datatype: col.data_type.to_string(),
                    required: col.is_nullable.contains("NO"),
                    unique: constraints
                        .iter()
                        .filter(|contraint| {
                            contraint.constraint_type.contains("UNIQUE")
                                || contraint.constraint_type.contains("PRIMARY KEY")
                        })
                        .find(|constraint| {
                            constraint.key_column.contains(&col.column_name.to_string())
                        })
                        .is_some(),
                })
                .collect_vec();
            let pk_contraints = constraints
                .iter()
                .filter(|constraint| constraint.constraint_type.contains("PRIMARY KEY"))
                .collect_vec();

            let primary_keys = cols
                .iter()
                .filter(|col| {
                    pk_contraints
                        .iter()
                        .find(|pk_contraint| pk_contraint.key_column.contains(&col.name))
                        .is_some()
                })
                .collect_vec();

            let regular_cols = cols
                .iter()
                .filter(|col| {
                    pk_contraints
                        .iter()
                        .find(|pk_contraint| pk_contraint.key_column.contains(&col.name))
                        .is_none()
                })
                .map(|col| col.to_owned().to_owned())
                .collect_vec();

            let toplevel_ops = if !primary_keys.is_empty() {
                vec![
                    Op {
                        name: format!("get_{}_by_id", table.name),
                        return_type: ReturnType::Object,
                        args: primary_keys
                            .iter()
                            .map(|pk| {
                                println!("{:?}", pk);

                                Arg {
                                    name: pk.name.to_owned(),
                                    tpe: match pk.datatype.as_str() {
                                        "text" => GraphQLString { default: None },
                                        "int" => GraphQLInteger { default: None },
                                        "integer" => GraphQLInteger { default: None },
                                        "uuid" => GraphQLID { default: None },
                                        _ => GraphQLString { default: None },
                                    },
                                    arg_type: ArgType::ColumnName,
                                }
                            })
                            .collect_vec(),
                    },
                    Op {
                        name: format!("list_{}", table.name),
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
                    },
                ]
            } else {
                vec![]
            };

            Table {
                name: table.name.to_string(),
                columns: regular_cols,
                primary_keys: primary_keys
                    .iter()
                    .map(|col| PrimaryKey(col.to_owned().to_owned()))
                    .collect_vec(),
                toplevel_ops,
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

            vec![
                Relationship2 {
                    table_name: table.name.to_string(),
                    column_name: column.name.to_string(),
                    target_table_name: foreign_table.name.to_string(),
                    target_column_name: foreign_column.name.to_string(),
                    field_name: format!("{}", foreign_table.name.to_string()),
                    return_type,
                    column_optional: !column.required,
                },
                Relationship2 {
                    table_name: foreign_table.name.to_string(),
                    column_name: foreign_column.name.to_string(),
                    target_table_name: table.name.to_string(),
                    target_column_name: column.name.to_string(),
                    field_name: format!("{}", table.name), // TODO: Better
                    return_type: second_return_type,
                    column_optional: !foreign_column.required,
                },
            ]
        })
        .collect_vec();

    IntrospectionResult {
        tables,
        relationships2,
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

async fn get_all_columns(pool: &Pool<Postgres>) -> Result<Vec<TableColumm>, sqlx::Error> {
    let sql = "SELECT *
    FROM information_schema.columns
   WHERE table_schema = 'public'
       ;";

    Ok(sqlx::query_as::<_, TableColumm>(sql)
        .fetch_all(pool)
        .await?)
}

async fn get_columns(
    pool: &Pool<Postgres>,
    table: &String,
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
    table: &String,
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

async fn get_all_references(pool: &Pool<Postgres>) -> Result<Vec<TableReferences>, sqlx::Error> {
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

async fn get_views(pool: &Pool<Postgres>) -> Result<Vec<TableView>, sqlx::Error> {
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
    key_column: String,
    constraint_type: String,
}

async fn get_constraints(pool: &Pool<Postgres>) -> Result<Vec<TableConstraint>, sqlx::Error> {
    let sql = "select kcu.table_schema,
       kcu.table_name,
       tco.constraint_name,
       kcu.ordinal_position as position,
       kcu.column_name as key_column,
       tco.constraint_type
from information_schema.table_constraints tco
join information_schema.key_column_usage kcu
     on kcu.constraint_name = tco.constraint_name
     and kcu.constraint_schema = tco.constraint_schema
     and kcu.constraint_name = tco.constraint_name
where kcu.table_schema= 'public'
order by kcu.table_schema,
         kcu.table_name,
         position;";

    Ok(sqlx::query_as(sql).fetch_all(pool).await?)
}
