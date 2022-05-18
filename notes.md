# Getting all tables

```sql
SELECT n.nspname as "schema",
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
      AND n.nspname !~ '^pg_toast'
  AND c.relname OPERATOR(pg_catalog.~) '^(pg_class)$'
  AND n.nspname OPERATOR(pg_catalog.~) '^(pg_catalog)$'
ORDER BY 1,2;
```

# Getting all views

```sql
select * from information_schema.views;
```

Contains information about whether the view is updateable


# Getting column information

```sql
SELECT *
  FROM information_schema.columns
 WHERE table_schema = 'your_schema'
   AND table_name   = 'your_table'
     ;
```

# Getting foreign key information

```sql
SELECT
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
WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name='mytable';
```