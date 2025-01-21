var DB = require('./index');
var Snapshot = require('../snapshot');
var util = require('../util');
var clone = util.clone;

// In-memory ShareDB database
//
// This adapter is not appropriate for production use. It is intended for
// testing and as an API example for people implementing database adaptors. It
// is fully functional, except it stores all documents & operations forever in
// memory. As such, memory usage will grow without bound, it doesn't scale
// across multiple node processes and you'll lose all your data if the server
// restarts. Query APIs are adapter specific. Use with care.

function MemoryDB(options) {
  if (!(this instanceof MemoryDB)) return new MemoryDB(options);
  DB.call(this, options);

  // Map from collection name -> doc id -> doc snapshot ({v:, type:, data:})
  this.docs = Object.create(null);

  // Map from collection name -> doc id -> list of operations. Operations
  // don't store their version - instead their version is simply the index in
  // the list.
  this.ops = Object.create(null);

  this.closed = false;

  this._transactions = Object.create(null);
};
module.exports = MemoryDB;

MemoryDB.prototype = Object.create(DB.prototype);

MemoryDB.prototype.close = function(callback) {
  this.closed = true;
  if (callback) callback();
};

// Persists an op and snapshot if it is for the next version. Calls back with
// callback(err, succeeded)
MemoryDB.prototype.commit = function(collection, id, op, snapshot, options, callback) {
  var db = this;
  options = options || {};
  if (typeof callback !== 'function') throw new Error('Callback required');
  util.nextTick(function() {
    var version = db._getVersionSync(collection, id);

    var transaction;
    if (options.transaction) {
      transaction = db._transactions[options.transaction];
      if (!transaction) return callback(null, false);
      version = version + transaction.commits.length;
    }

    if (snapshot.v !== version + 1) {
      var succeeded = false;
      return callback(null, succeeded);
    }

    var commit = function() {
      var err = db._writeOpSync(collection, id, op);
      if (err) return err;
      err = db._writeSnapshotSync(collection, id, snapshot);
      if (err) return err;
    };

    var error;
    if (!transaction) error = commit();
    callback(error, !error);

    if (transaction) transaction.commits.push(commit);
  });
};

// Get the named document from the database. The callback is called with (err,
// snapshot). A snapshot with a version of zero is returned if the docuemnt
// has never been created in the database.
MemoryDB.prototype.getSnapshot = function(collection, id, fields, options, callback) {
  var includeMetadata = (fields && fields.$submit) || (options && options.metadata);
  var db = this;
  if (typeof callback !== 'function') throw new Error('Callback required');
  util.nextTick(function() {
    var snapshot = db._getSnapshotSync(collection, id, includeMetadata);
    callback(null, snapshot);
  });
};

// Get operations between [from, to) noninclusively. (Ie, the range should
// contain start but not end).
//
// If end is null, this function should return all operations from start onwards.
//
// The operations that getOps returns don't need to have a version: field.
// The version will be inferred from the parameters if it is missing.
//
// Callback should be called as callback(error, [list of ops]);
MemoryDB.prototype.getOps = function(collection, id, from, to, options, callback) {
  var includeMetadata = options && options.metadata;
  var db = this;
  if (typeof callback !== 'function') throw new Error('Callback required');
  util.nextTick(function() {
    var opLog = db._getOpLogSync(collection, id);
    if (!from) from = 0;
    if (to == null) to = opLog.length;
    var ops = clone(opLog.slice(from, to).filter(Boolean));
    if (ops.length < to - from) {
      return callback(new Error('Missing ops'));
    }
    if (!includeMetadata) {
      for (var i = 0; i < ops.length; i++) {
        delete ops[i].m;
      }
    }
    callback(null, ops);
  });
};

MemoryDB.prototype.deleteOps = function(collection, id, from, to, options, callback) {
  if (typeof callback !== 'function') throw new Error('Callback required');
  var db = this;
  util.nextTick(function() {
    var opLog = db._getOpLogSync(collection, id);
    if (!from) from = 0;
    if (to == null) to = opLog.length;
    for (var i = from; i < to; i++) opLog[i] = null;
    callback(null);
  });
};

// The memory database query function returns all documents in a collection
// regardless of query by default
MemoryDB.prototype.query = function(collection, query, fields, options, callback) {
  var includeMetadata = options && options.metadata;
  var db = this;
  if (typeof callback !== 'function') throw new Error('Callback required');
  util.nextTick(function() {
    var collectionDocs = db.docs[collection];
    var snapshots = [];
    for (var id in collectionDocs || {}) {
      var snapshot = db._getSnapshotSync(collection, id, includeMetadata);
      snapshots.push(snapshot);
    }
    try {
      var result = db._querySync(snapshots, query, options);
      callback(null, result.snapshots, result.extra);
    } catch (err) {
      callback(err);
    }
  });
};

MemoryDB.prototype.startTransaction = function(id, callback) {
  if (this._transactions[id]) callback(new Error('Transaction already started'));
  this._transactions[id] = this._transactions[id] || {};
  this._transactions[id].commits = [];
  callback();
};

MemoryDB.prototype.restartTransaction = function(id, callback) {
  delete this._transactions[id];
  this.startTransaction(id, callback);
};

MemoryDB.prototype.commitTransaction = function(id, callback) {
  var transaction = this._transactions[id];
  if (!transaction) return callback();

  delete this._transactions[id];

  // Heavy-handed but effective approach to synchronous transaction writing:
  // let's just save the DB state before and restore if the transaction fails
  var docs = JSON.stringify(this.docs);
  var ops = JSON.stringify(this.ops);

  for (var commit of transaction.commits) {
    var error = commit();

    if (error) {
      this.docs = JSON.parse(docs);
      this.ops = JSON.parse(ops);
      break;
    }
  }

  callback(error, !error);
};

MemoryDB.prototype.abortTransaction = function(id, callback) {
  delete this._transactions[id];
  callback(null);
};

// For testing, it may be useful to implement the desired query
// language by defining this function. Returns an object with
// two properties:
// - snapshots: array of query result snapshots
// - extra: (optional) other types of results, such as counts
MemoryDB.prototype._querySync = function(snapshots) {
  return {snapshots: snapshots};
};

MemoryDB.prototype._writeOpSync = function(collection, id, op) {
  var opLog = this._getOpLogSync(collection, id);
  // This will write an op in the log at its version, which should always be
  // the next item in the array under normal operation
  opLog[op.v] = clone(op);
};

// Create, update, and delete snapshots. For creates and updates, a snapshot
// object will be passed in with a type property. If there is no type property,
// it should be considered a delete
MemoryDB.prototype._writeSnapshotSync = function(collection, id, snapshot) {
  var collectionDocs = this.docs[collection] || (this.docs[collection] = Object.create(null));
  if (!snapshot.type) {
    delete collectionDocs[id];
  } else {
    collectionDocs[id] = clone(snapshot);
  }
};

MemoryDB.prototype._getSnapshotSync = function(collection, id, includeMetadata) {
  var collectionDocs = this.docs[collection];
  // We need to clone the snapshot, because ShareDB assumes each call to
  // getSnapshot returns a new object
  var doc = collectionDocs && collectionDocs[id];
  var snapshot;
  if (doc) {
    var data = clone(doc.data);
    var meta = (includeMetadata) ? clone(doc.m) : null;
    snapshot = new Snapshot(id, doc.v, doc.type, data, meta);
  } else {
    var version = this._getVersionSync(collection, id);
    snapshot = new Snapshot(id, version, null, undefined, null);
  }
  return snapshot;
};

MemoryDB.prototype._getOpLogSync = function(collection, id) {
  var collectionOps = this.ops[collection] || (this.ops[collection] = Object.create(null));
  return collectionOps[id] || (collectionOps[id] = []);
};

MemoryDB.prototype._getVersionSync = function(collection, id) {
  var collectionOps = this.ops[collection];
  return (collectionOps && collectionOps[id] && collectionOps[id].length) || 0;
};
