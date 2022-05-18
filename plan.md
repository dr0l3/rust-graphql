# Figure out how to execute SQL queries

It seems the sqlx library is quite neat

# Figure out how to do codegen

It seems https://github.com/carllerche/codegen can do the job

# Figure out if any graphql libraries support "generic"

It seems there are two libraries

- Juniper
- async-graphql


Async-graphql definately supports fetting fields

# Wait for Lazy to make it into async-graphql

It seems that none of the two libraries support neither a generic response (just return something that we promise is JSON). I have to return a struct corresponding to the type.

This makes things considerably more difficult. I need the types to be accurate representations for the GraphQL schema that is generated to make sense. This means I must instantiate a struct in each response.

To make matters worse there is no "lazy" fields (its in nightly, but not in either of the libraries). This also unfortunately makes it really difficult to return json from the database. I could I guess return json and then map that into a struct and use a default value when the json is empty for a particular field. The code that needs to be generated for all of this to work seems really difficult.

All of this might be easier to just do as a Postgres extension.

