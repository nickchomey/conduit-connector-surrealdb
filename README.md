# Conduit Connector for <resource>

[Conduit](https://conduit.io) connector for [SurrealDB](https://surrealdb.com/).

## How to build?

Run `make build` to build the connector, or `make build-debug` if you want to step debug it.

## Testing

**Note, currently there are no tests**

Run `make test` to run all the unit tests. Run `make test-integration` to run the integration tests.

The Docker compose file at `test/docker-compose.yml` can be used to run the required resource locally.

## Configuration

| name                  | description                           | required | default value |
|-----------------------|---------------------------------------|----------|---------------|
| `URL` | URL is the connection string for the SurrealDB server. | true     | ""     |
| `Username` | Username is the username for the SurrealDB server. | true     | ""        |
| `Password` | Password is the password for the SurrealDB server. | true     | ""          |
| `Namespace` | Namespace is the namespace for the SurrealDB server. | true     | ""          |
| `Database` | Database is the database name for the SurrealDB server. | true     | ""          |
| `Scope` | Scope is the scope for the SurrealDB server. | true     | ""          |

### Source

There is currently no Source connector



### Destination

A destination connector pushes data from upstream resources to an external resource via Conduit.


| name                       | description                                | required | default value |
|----------------------------|--------------------------------------------|----------|---------------|
| `DeleteOldKey` | Primary key will be set to "id". Specify whether you want to keep the source Primary Key column as well. | false     | false          |

## Known Issues & Limitations

- Known issue A
- Limitation A

## Planned work

- [ ] Item A
- [ ] Item B
