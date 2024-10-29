# ShareDB

  [![NPM Version](https://img.shields.io/npm/v/sharedb.svg)](https://npmjs.org/package/sharedb)
  ![Test](https://github.com/share/sharedb/workflows/Test/badge.svg)
  [![Coverage Status](https://coveralls.io/repos/github/share/sharedb/badge.svg?branch=master)](https://coveralls.io/github/share/sharedb?branch=master)

ShareDB is a realtime database backend based on [Operational Transformation
(OT)](https://en.wikipedia.org/wiki/Operational_transformation) of JSON
documents. It is the realtime backend for the [DerbyJS web application
framework](http://derbyjs.com/).

For help, questions, discussion and announcements, join the [ShareJS mailing
list](https://groups.google.com/forum/?fromgroups#!forum/sharejs) or [read the documentation](https://share.github.io/sharedb/
).

Please report any bugs you find to the [issue
tracker](https://github.com/share/sharedb/issues).

## Documentation

[Chinese](https://rain120.github.io/sharedb-zh/sharedb/sharedb)

## Features

 - Realtime synchronization of any JSON document
 - Concurrent multi-user collaboration
 - Synchronous editing API with asynchronous eventual consistency
 - Realtime query subscriptions
 - Simple integration with any database
 - Horizontally scalable with pub/sub integration
 - Projections to select desired fields from documents and operations
 - Middleware for implementing access control and custom extensions
 - Ideal for use in browsers or on the server
 - Offline change syncing upon reconnection
 - In-memory implementations of database and pub/sub for unit testing
 - Access to historic document versions
 - Realtime user presence syncing

## Documentation

https://share.github.io/sharedb/

## Examples

### Counter

[<img src="examples/counter/demo.gif" height="300">](examples/counter)

### Leaderboard

[<img src="examples/leaderboard/demo.gif" height="436">](examples/leaderboard)

## Development

### Documentation

The documentation is stored as Markdown files, but sometimes it can be useful to run these locally. The docs are served using [Jekyll](https://jekyllrb.com/), and require Ruby >2.4.0 and [Bundler](https://bundler.io/):

```bash
gem install jekyll bundler
```

The docs can be built locally and served with live reload:

```bash
npm run docs:install
npm run docs:start
```
