# Working stuff

- Select
  - Get by id
  - List
    - Pagination
  - Search
    - By indexed fields
    - By non-indexed fields
    - Pagination
- Without using fragments
- And only using inline variable
- Functions
  - As top level operations
- GraphQL schema introspection
- Efficiency
  - Binds in SQL
- configuration
  - Table
    - Ignore
  - Column
    - Ignore

# Non working stuff
- Configuration
  - Table
    - Hide
    - Rename
  - Colum
    - Hide
    - Rename
    - Read only
- Views
- Faster tests
  - Shared postgres (should be possible with use of either lazy static or sync::Once)
- Fragments
- Other operations
  - Update
  - Insert
  - Delete
- Other databases
  - SQLite

- Federation
- fuzzy search operations
  - if index covers field
- Functions as joins targets