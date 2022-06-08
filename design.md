# General idea

Three set of entities

- Structs representing rows returned from Postgres
- Structs representing the API found in postgres
- Structs representing the result of transpiling a query to sql
  - Rendering this struct generates the sql

Important functions

- `fn introspect_postgres(...) -> IntrospectionResult`
- `fn to_intermediate(api: IntrospectionResult, request: GraphQLRequest) -> SqlQuery`

# Single table

A table of the form 

```sql
create table example(
    id uuid primary key,
    txt text not null,
    number integer
)
```

This table should generate the following operations

- get by id
- get by <any unique column>
- list all with pagination
- search by any remaining columns

In GraphQL this could look like this

```graphql
type Example {
    id: ID!
    txt: String!
    number: Int
}

type Query {
    get_example_by_id(id: ID!): Example
}
```
# Listing

A table of the form

```sql
create table example(
    id uuid primary key,
    txt text not null,
    number integer
)
```

## Pagination

We are going to use key set pagination. So the following simple interface will be generated.

```graphql
type Example {
    id: ID!
    txt: String!
    number: Int
}

type Query {
    list_examples(limit: Int, offset: Int): [Example!]!
}
```

## Ordering

Additionally we would like to support custom ordering. Given the (lack of) indexes in the above the table the below interface is created. Note the ordering.

```graphql
type Example {
    id: ID!
    txt: String!
    number: Int
}

enum OrderBy {
    ASC,
    ASC_NULLS_FIRST,
    ASC_NULLS_LAST,
    ASC,
    ASC_NULLS_FIRST,
    ASC_NULLS_LAST
}

type ExampleOrderBy {
    id: OrderBy
}

type Query {
    list_examples(limit: Int, offset: Int, order_by: ExampleOrderBy): [Example!]!
}
```

If we extended the sql to the following

```sql
create table example(
    id uuid primary key,
    txt text not null,
    number integer
);

create index example_by_number on example(number);
```

We can extend the generated interface to the following

```graphql
type Example {
    id: ID!
    txt: String!
    number: Int
}

enum OrderBy {
    ASC,
    ASC_NULLS_FIRST,
    ASC_NULLS_LAST,
    ASC,
    ASC_NULLS_FIRST,
    ASC_NULLS_LAST
}

type ExampleOrderBy {
    id: OrderBy
    number: OrderBy
}

type Query {
    list_examples(limit: Int, offset: Int, order_by: ExampleOrderBy): [Example!]!
}
```

## Filtering

Should this be a separate endpoint or just the listing one?

# Indexes

So far we have just added the ability to search by value when there is an index backing the operation.

It would be nice to generate special operations if the index backing the column is s special index like
- pg_trgm

# Functions

In essence we only care about the following things 

- Parameters
  - type (inout vs in)
  - type (string vs int)
- output
  - type (string vs int)
- stability classifier (immutable, stable and volatile)

It is not possible to do predicate pushdown into functions. 

## Features

1. Support only functions with
    - Exclusively in parameters
    - immutable and stable classifiers
    - returns setof or stuff

