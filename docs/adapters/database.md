---
title: Database adapters
nav_order: 1
layout: default
parent: Adapters
---

# Database adapters
{: .no_toc }

1. TOC
{:toc}

The database adapter is responsible for persisting document contents and ops.

## Available adapters

### MemoryDB

ShareDB ships with an in-memory, non-persistent database. This is useful for testing. It has no query support.

{: .warn }
`MemoryDB` does not persist its data between app restarts, and is **not** suitable for use in a Production environment.

### ShareDBMongo

[`sharedb-mongo`](https://github.com/share/sharedb-mongo) is backed by MongoDB, with full query support.

### ShareDBMingoMemory

[`sharedb-mingo-memory`](https://github.com/share/sharedb-mingo-memory) is an in-memory database that implements a subset of Mongo operations, including queries. This can be useful for testing against a MongoDB-like ShareDB instance.

### ShareDBPostgres

[`sharedb-postgres`](https://github.com/plotdb/sharedb-postgres) is backed by PostgreSQL, and has no query support.

## Usage

An instance of a database adapter should be provided to the [`Backend()` constructor]({{ site.baseurl }}{% link api/backend.md %}#backend-constructor)'s `db` option:

```js
const backend = new Backend({
  db: new MemoryDB(),
})
```
