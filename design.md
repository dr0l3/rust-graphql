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
    list_example(limit: Int!, offset: Int!): [Example!]!
}
```
