var Doc = require('./doc');
var Query = require('./query');
var Presence = require('./presence/presence');
var DocPresence = require('./presence/doc-presence');
var SnapshotVersionRequest = require('./snapshot-request/snapshot-version-request');
var SnapshotTimestampRequest = require('./snapshot-request/snapshot-timestamp-request');
var emitter = require('../emitter');
var ShareDBError = require('../error');
var ACTIONS = require('../message-actions').ACTIONS;
var types = require('../types');
var util = require('../util');
var logger = require('../logger');
var DocPresenceEmitter = require('./presence/doc-presence-emitter');
var protocol = require('../protocol');
var Transaction = require('./transaction');

var ERROR_CODE = ShareDBError.CODES;

function connectionState(socket) {
  if (socket.readyState === 0 || socket.readyState === 1) return 'connecting';
  return 'disconnected';
}

/**
 * Handles communication with the sharejs server and provides queries and
 * documents.
 *
 * We create a connection with a socket object
 *   connection = new sharejs.Connection(sockset)
 * The socket may be any object handling the websocket protocol. See the
 * documentation of bindToSocket() for details. We then wait for the connection
 * to connect
 *   connection.on('connected', ...)
 * and are finally able to work with shared documents
 *   connection.get('food', 'steak') // Doc
 *
 * @param socket @see bindToSocket
 */
module.exports = Connection;
function Connection(socket) {
  emitter.EventEmitter.call(this);

  // Map of collection -> id -> doc object for created documents.
  // (created documents MUST BE UNIQUE)
  this.collections = Object.create(null);

  // Each query and snapshot request is created with an id that the server uses when it sends us
  // info about the request (updates, etc)
  this.nextQueryId = 1;
  this.nextSnapshotRequestId = 1;

  // Map from query ID -> query object.
  this.queries = Object.create(null);

  // Maps from channel -> presence objects
  this._presences = Object.create(null);
  this._docPresenceEmitter = new DocPresenceEmitter();

  // Map from snapshot request ID -> snapshot request
  this._snapshotRequests = Object.create(null);

  // A unique message number for the given id
  this.seq = 1;

  // A unique message number for presence
  this._presenceSeq = 1;

  this._transactions = Object.create(null);

  // Equals agent.src on the server
  this.id = null;

  // This direct reference from connection to agent is not used internal to
  // ShareDB, but it is handy for server-side only user code that may cache
  // state on the agent and read it in middleware
  this.agent = null;

  this.debug = false;

  this.state = connectionState(socket);

  this.bindToSocket(socket);
}
emitter.mixin(Connection);


/**
 * Use socket to communicate with server
 *
 * Socket is an object that can handle the websocket protocol. This method
 * installs the onopen, onclose, onmessage and onerror handlers on the socket to
 * handle communication and sends messages by calling socket.send(message). The
 * sockets `readyState` property is used to determine the initaial state.
 *
 * @param socket Handles the websocket protocol
 * @param socket.readyState
 * @param socket.close
 * @param socket.send
 * @param socket.onopen
 * @param socket.onclose
 * @param socket.onmessage
 * @param socket.onerror
 */
Connection.prototype.bindToSocket = function(socket) {
  if (this.socket) {
    this.socket.close();
    this.socket.onmessage = null;
    this.socket.onopen = null;
    this.socket.onerror = null;
    this.socket.onclose = null;
  }

  this.socket = socket;

  // State of the connection. The corresponding events are emitted when this changes
  //
  // - 'connecting'   The connection is still being established, or we are still
  //                    waiting on the server to send us the initialization message
  // - 'connected'    The connection is open and we have connected to a server
  //                    and recieved the initialization message
  // - 'disconnected' Connection is closed, but it will reconnect automatically
  // - 'closed'       The connection was closed by the client, and will not reconnect
  // - 'stopped'      The connection was closed by the server, and will not reconnect
  var newState = connectionState(socket);
  this._setState(newState);

  // This is a helper variable the document uses to see whether we're
  // currently in a 'live' state. It is true if and only if we're connected
  this.canSend = false;

  var connection = this;

  socket.onmessage = function(event) {
    try {
      var data = (typeof event.data === 'string') ?
        JSON.parse(event.data) : event.data;
    } catch (err) {
      logger.warn('Failed to parse message', event);
      return;
    }

    if (connection.debug) logger.info('RECV', JSON.stringify(data));

    var request = {data: data};
    connection.emit('receive', request);
    if (!request.data) return;

    try {
      connection.handleMessage(request.data);
    } catch (err) {
      util.nextTick(function() {
        connection.emit('error', err);
      });
    }
  };

  // If socket is already open, do handshake immediately.
  if (socket.readyState === 1) {
    connection._initializeHandshake();
  }
  socket.onopen = function() {
    connection._setState('connecting');
    connection._initializeHandshake();
  };

  socket.onerror = function(err) {
    // This isn't the same as a regular error, because it will happen normally
    // from time to time. Your connection should probably automatically
    // reconnect anyway, but that should be triggered off onclose not onerror.
    // (onclose happens when onerror gets called anyway).
    connection.emit('connection error', err);
  };

  socket.onclose = function(reason) {
    // node-browserchannel reason values:
    //   'Closed' - The socket was manually closed by calling socket.close()
    //   'Stopped by server' - The server sent the stop message to tell the client not to try connecting
    //   'Request failed' - Server didn't respond to request (temporary, usually offline)
    //   'Unknown session ID' - Server session for client is missing (temporary, will immediately reestablish)

    if (reason === 'closed' || reason === 'Closed') {
      connection._setState('closed', reason);
    } else if (reason === 'stopped' || reason === 'Stopped by server') {
      connection._setState('stopped', reason);
    } else {
      connection._setState('disconnected', reason);
    }
  };
};

/**
 * @param {object} message
 * @param {string} message.a action
 */
Connection.prototype.handleMessage = function(message) {
  var err = null;
  if (message.error) {
    err = wrapErrorData(message.error, message);
    delete message.error;
  }
  // Switch on the message action. Most messages are for documents and are
  // handled in the doc class.
  switch (message.a) {
    case ACTIONS.initLegacy:
      // Client initialization packet
      return this._handleLegacyInit(message);
    case ACTIONS.handshake:
      return this._handleHandshake(err, message);
    case ACTIONS.queryFetch:
      var query = this.queries[message.id];
      if (query) query._handleFetch(err, message.data, message.extra);
      return;
    case ACTIONS.querySubscribe:
      var query = this.queries[message.id];
      if (query) query._handleSubscribe(err, message.data, message.extra);
      return;
    case ACTIONS.queryUnsubscribe:
      // Queries are removed immediately on calls to destroy, so we ignore
      // replies to query unsubscribes. Perhaps there should be a callback for
      // destroy, but this is currently unimplemented
      return;
    case ACTIONS.queryUpdate:
      // Query message. Pass this to the appropriate query object.
      var query = this.queries[message.id];
      if (!query) return;
      if (err) return query._handleError(err);
      if (message.diff) query._handleDiff(message.diff);
      if (util.hasOwn(message, 'extra')) query._handleExtra(message.extra);
      return;

    case ACTIONS.bulkFetch:
      return this._handleBulkMessage(err, message, '_handleFetch');
    case ACTIONS.bulkSubscribe:
    case ACTIONS.bulkUnsubscribe:
      return this._handleBulkMessage(err, message, '_handleSubscribe');

    case ACTIONS.snapshotFetch:
    case ACTIONS.snapshotFetchByTimestamp:
      return this._handleSnapshotFetch(err, message);

    case ACTIONS.fetch:
      var doc = this.getExisting(message.c, message.d);
      if (doc) doc._handleFetch(err, message.data);
      return;
    case ACTIONS.subscribe:
    case ACTIONS.unsubscribe:
      var doc = this.getExisting(message.c, message.d);
      if (doc) doc._handleSubscribe(err, message.data);
      return;
    case ACTIONS.op:
      var doc = this.getExisting(message.c, message.d);
      if (doc) doc._handleOp(err, message);
      return;
    case ACTIONS.presence:
      return this._handlePresence(err, message);
    case ACTIONS.presenceSubscribe:
      return this._handlePresenceSubscribe(err, message);
    case ACTIONS.presenceUnsubscribe:
      return this._handlePresenceUnsubscribe(err, message);
    case ACTIONS.presenceRequest:
      return this._handlePresenceRequest(err, message);
    case ACTIONS.pingPong:
      return this._handlePingPong(err);
    case ACTIONS.transactionCommit:
      return this._handleTransactionCommit(err, message);

    default:
      logger.warn('Ignoring unrecognized message', message);
  }
};

function wrapErrorData(errorData, fullMessage) {
  // wrap in Error object so can be passed through event emitters
  var err = new Error(errorData.message);
  err.code = errorData.code;
  if (fullMessage) {
    // Add the message data to the error object for more context
    err.data = fullMessage;
  }
  return err;
}

Connection.prototype._handleBulkMessage = function(err, message, method) {
  if (message.data) {
    for (var id in message.data) {
      var dataForId = message.data[id];
      var doc = this.getExisting(message.c, id);
      if (doc) {
        if (err) {
          doc[method](err);
        } else if (dataForId.error) {
          // Bulk reply snapshot-specific errorr - see agent.js getMapResult
          doc[method](wrapErrorData(dataForId.error));
        } else {
          doc[method](null, dataForId);
        }
      }
    }
  } else if (Array.isArray(message.b)) {
    for (var i = 0; i < message.b.length; i++) {
      var id = message.b[i];
      var doc = this.getExisting(message.c, id);
      if (doc) doc[method](err);
    }
  } else if (message.b) {
    for (var id in message.b) {
      var doc = this.getExisting(message.c, id);
      if (doc) doc[method](err);
    }
  } else {
    logger.error('Invalid bulk message', message);
  }
};

Connection.prototype._reset = function() {
  this.agent = null;
};

// Set the connection's state. The connection is basically a state machine.
Connection.prototype._setState = function(newState, reason) {
  if (this.state === newState) return;

  // I made a state diagram. The only invalid transitions are getting to
  // 'connecting' from anywhere other than 'disconnected' and getting to
  // 'connected' from anywhere other than 'connecting'.
  if (
    (
      newState === 'connecting' &&
      this.state !== 'disconnected' &&
      this.state !== 'stopped' &&
      this.state !== 'closed'
    ) || (
      newState === 'connected' &&
      this.state !== 'connecting'
    )
  ) {
    var err = new ShareDBError(
      ERROR_CODE.ERR_CONNECTION_STATE_TRANSITION_INVALID,
      'Cannot transition directly from ' + this.state + ' to ' + newState
    );
    return this.emit('error', err);
  }

  this.state = newState;
  this.canSend = (newState === 'connected');

  if (
    newState === 'disconnected' ||
    newState === 'stopped' ||
    newState === 'closed'
  ) {
    this._reset();
  }

  // Group subscribes together to help server make more efficient calls
  this.startBulk();
  // Emit the event to all queries
  for (var id in this.queries) {
    var query = this.queries[id];
    query._onConnectionStateChanged();
  }
  // Emit the event to all documents
  for (var collection in this.collections) {
    var docs = this.collections[collection];
    for (var id in docs) {
      docs[id]._onConnectionStateChanged();
    }
  }
  // Emit the event to all Presences
  for (var channel in this._presences) {
    this._presences[channel]._onConnectionStateChanged();
  }
  // Emit the event to all snapshots
  for (var id in this._snapshotRequests) {
    var snapshotRequest = this._snapshotRequests[id];
    snapshotRequest._onConnectionStateChanged();
  }
  this.endBulk();

  this.emit(newState, reason);
  this.emit('state', newState, reason);
};

Connection.prototype.startBulk = function() {
  if (!this.bulk) this.bulk = Object.create(null);
};

Connection.prototype.endBulk = function() {
  if (this.bulk) {
    for (var collection in this.bulk) {
      var actions = this.bulk[collection];
      this._sendBulk('f', collection, actions.f);
      this._sendBulk('s', collection, actions.s);
      this._sendBulk('u', collection, actions.u);
    }
  }
  this.bulk = null;
};

Connection.prototype._sendBulk = function(action, collection, values) {
  if (!values) return;
  var ids = [];
  var versions = Object.create(null);
  var versionsCount = 0;
  var versionId;
  for (var id in values) {
    var value = values[id];
    if (value == null) {
      ids.push(id);
    } else {
      versions[id] = value;
      versionId = id;
      versionsCount++;
    }
  }
  if (ids.length === 1) {
    var id = ids[0];
    this.send({a: action, c: collection, d: id});
  } else if (ids.length) {
    this.send({a: 'b' + action, c: collection, b: ids});
  }
  if (versionsCount === 1) {
    var version = versions[versionId];
    this.send({a: action, c: collection, d: versionId, v: version});
  } else if (versionsCount) {
    this.send({a: 'b' + action, c: collection, b: versions});
  }
};

Connection.prototype._sendActions = function(action, doc, version) {
  // Ensure the doc is registered so that it receives the reply message
  this._addDoc(doc);
  if (this.bulk) {
    // Bulk subscribe
    var actions = this.bulk[doc.collection] || (this.bulk[doc.collection] = Object.create(null));
    var versions = actions[action] || (actions[action] = Object.create(null));
    var isDuplicate = util.hasOwn(versions, doc.id);
    versions[doc.id] = version;
    return isDuplicate;
  } else {
    // Send single doc subscribe message
    var message = {a: action, c: doc.collection, d: doc.id, v: version};
    this.send(message);
  }
};

Connection.prototype.sendFetch = function(doc) {
  return this._sendActions(ACTIONS.fetch, doc, doc.version);
};

Connection.prototype.sendSubscribe = function(doc) {
  return this._sendActions(ACTIONS.subscribe, doc, doc.version);
};

Connection.prototype.sendUnsubscribe = function(doc) {
  return this._sendActions(ACTIONS.unsubscribe, doc);
};

Connection.prototype.sendOp = function(doc, op) {
  // Ensure the doc is registered so that it receives the reply message
  this._addDoc(doc);
  var message = {
    a: ACTIONS.op,
    c: doc.collection,
    d: doc.id,
    v: doc.version,
    src: op.src,
    seq: op.seq,
    x: {}
  };
  if ('op' in op) message.op = op.op;
  if (op.create) message.create = op.create;
  if (op.del) message.del = op.del;
  if (doc.submitSource) message.x.source = op.source;
  if (op.transaction) message.t = op.transaction;
  this.send(message);
};


/**
 * Sends a message down the socket
 */
Connection.prototype.send = function(message) {
  if (this.debug) logger.info('SEND', JSON.stringify(message));

  this.emit('send', message);
  this.socket.send(JSON.stringify(message));
};

Connection.prototype.ping = function() {
  if (!this.canSend) {
    throw new ShareDBError(
      ERROR_CODE.ERR_CANNOT_PING_OFFLINE,
      'Socket must be CONNECTED to ping'
    );
  }

  var message = {
    a: ACTIONS.pingPong
  };
  this.send(message);
};

/**
 * Closes the socket and emits 'closed'
 */
Connection.prototype.close = function() {
  this.socket.close();
};

Connection.prototype.getExisting = function(collection, id) {
  if (this.collections[collection]) return this.collections[collection][id];
};


/**
 * Get or create a document.
 *
 * @param collection
 * @param id
 * @return {Doc}
 */
Connection.prototype.get = function(collection, id) {
  var docs = this.collections[collection] ||
    (this.collections[collection] = Object.create(null));

  var doc = docs[id];
  if (!doc) {
    doc = docs[id] = new Doc(this, collection, id);
    this.emit('doc', doc);
  }

  doc._wantsDestroy = false;
  return doc;
};


/**
 * Remove document from this.collections
 *
 * @private
 */
Connection.prototype._destroyDoc = function(doc) {
  if (!doc._wantsDestroy) return;
  util.digAndRemove(this.collections, doc.collection, doc.id);
  doc.emit('destroy');
};

Connection.prototype._addDoc = function(doc) {
  var docs = this.collections[doc.collection];
  if (!docs) {
    docs = this.collections[doc.collection] = Object.create(null);
  }
  if (docs[doc.id] !== doc) {
    docs[doc.id] = doc;
  }
};

// Helper for createFetchQuery and createSubscribeQuery, below.
Connection.prototype._createQuery = function(action, collection, q, options, callback) {
  var id = this.nextQueryId++;
  var query = new Query(action, this, id, collection, q, options, callback);
  this.queries[id] = query;
  query.send();
  return query;
};

// Internal function. Use query.destroy() to remove queries.
Connection.prototype._destroyQuery = function(query) {
  delete this.queries[query.id];
};

// The query options object can contain the following fields:
//
// db: Name of the db for the query. You can attach extraDbs to ShareDB and
//   pick which one the query should hit using this parameter.

// Create a fetch query. Fetch queries are only issued once, returning the
// results directly into the callback.
//
// The callback should have the signature function(error, results, extra)
// where results is a list of Doc objects.
Connection.prototype.createFetchQuery = function(collection, q, options, callback) {
  return this._createQuery(ACTIONS.queryFetch, collection, q, options, callback);
};

// Create a subscribe query. Subscribe queries return with the initial data
// through the callback, then update themselves whenever the query result set
// changes via their own event emitter.
//
// If present, the callback should have the signature function(error, results, extra)
// where results is a list of Doc objects.
Connection.prototype.createSubscribeQuery = function(collection, q, options, callback) {
  return this._createQuery(ACTIONS.querySubscribe, collection, q, options, callback);
};

Connection.prototype.hasPending = function() {
  return !!(
    this._firstDoc(hasPending) ||
    this._firstQuery(hasPending) ||
    this._firstSnapshotRequest()
  );
};
function hasPending(object) {
  return object.hasPending();
}

Connection.prototype.hasWritePending = function() {
  return !!this._firstDoc(hasWritePending);
};
function hasWritePending(object) {
  return object.hasWritePending();
}

Connection.prototype.whenNothingPending = function(callback) {
  var doc = this._firstDoc(hasPending);
  if (doc) {
    // If a document is found with a pending operation, wait for it to emit
    // that nothing is pending anymore, and then recheck all documents again.
    // We have to recheck all documents, just in case another mutation has
    // been made in the meantime as a result of an event callback
    doc.once('nothing pending', this._nothingPendingRetry(callback));
    return;
  }
  var query = this._firstQuery(hasPending);
  if (query) {
    query.once('ready', this._nothingPendingRetry(callback));
    return;
  }
  var snapshotRequest = this._firstSnapshotRequest();
  if (snapshotRequest) {
    snapshotRequest.once('ready', this._nothingPendingRetry(callback));
    return;
  }
  // Call back when no pending operations
  util.nextTick(callback);
};
Connection.prototype._nothingPendingRetry = function(callback) {
  var connection = this;
  return function() {
    util.nextTick(function() {
      connection.whenNothingPending(callback);
    });
  };
};

Connection.prototype._firstDoc = function(fn) {
  for (var collection in this.collections) {
    var docs = this.collections[collection];
    for (var id in docs) {
      var doc = docs[id];
      if (fn(doc)) {
        return doc;
      }
    }
  }
};

Connection.prototype._firstQuery = function(fn) {
  for (var id in this.queries) {
    var query = this.queries[id];
    if (fn(query)) {
      return query;
    }
  }
};

Connection.prototype._firstSnapshotRequest = function() {
  for (var id in this._snapshotRequests) {
    return this._snapshotRequests[id];
  }
};

/**
 * Fetch a read-only snapshot at a given version
 *
 * @param collection - the collection name of the snapshot
 * @param id - the ID of the snapshot
 * @param version (optional) - the version number to fetch. If null, the latest version is fetched.
 * @param callback - (error, snapshot) => void, where snapshot takes the following schema:
 *
 * {
 *   id: string;         // ID of the snapshot
 *   v: number;          // version number of the snapshot
 *   type: string;       // the OT type of the snapshot, or null if it doesn't exist or is deleted
 *   data: any;          // the snapshot
 * }
 *
 */
Connection.prototype.fetchSnapshot = function(collection, id, version, callback) {
  if (typeof version === 'function') {
    callback = version;
    version = null;
  }

  var requestId = this.nextSnapshotRequestId++;
  var snapshotRequest = new SnapshotVersionRequest(this, requestId, collection, id, version, callback);
  this._snapshotRequests[snapshotRequest.requestId] = snapshotRequest;
  snapshotRequest.send();
};

/**
 * Fetch a read-only snapshot at a given timestamp
 *
 * @param collection - the collection name of the snapshot
 * @param id - the ID of the snapshot
 * @param timestamp (optional) - the timestamp to fetch. If null, the latest version is fetched.
 * @param callback - (error, snapshot) => void, where snapshot takes the following schema:
 *
 * {
 *   id: string;         // ID of the snapshot
 *   v: number;          // version number of the snapshot
 *   type: string;       // the OT type of the snapshot, or null if it doesn't exist or is deleted
 *   data: any;          // the snapshot
 * }
 *
 */
Connection.prototype.fetchSnapshotByTimestamp = function(collection, id, timestamp, callback) {
  if (typeof timestamp === 'function') {
    callback = timestamp;
    timestamp = null;
  }

  var requestId = this.nextSnapshotRequestId++;
  var snapshotRequest = new SnapshotTimestampRequest(this, requestId, collection, id, timestamp, callback);
  this._snapshotRequests[snapshotRequest.requestId] = snapshotRequest;
  snapshotRequest.send();
};

Connection.prototype._handleSnapshotFetch = function(error, message) {
  var snapshotRequest = this._snapshotRequests[message.id];
  if (!snapshotRequest) return;
  delete this._snapshotRequests[message.id];
  snapshotRequest._handleResponse(error, message);
};

Connection.prototype._handleLegacyInit = function(message) {
  // If the protocol is at least 1.1, we want to use the
  // new handshake protocol. Let's send a handshake initialize, because
  // we now know the server is ready. If we've already sent it, we'll
  // just ignore the response anyway.
  if (protocol.checkAtLeast(message, '1.1')) return this._initializeHandshake();
  this._initialize(message);
};

Connection.prototype._initializeHandshake = function() {
  this.send({
    a: ACTIONS.handshake,
    id: this.id,
    protocol: protocol.major,
    protocolMinor: protocol.minor
  });
};

Connection.prototype._handleHandshake = function(error, message) {
  if (error) return this.emit('error', error);
  this._initialize(message);
};

Connection.prototype._handlePingPong = function(error) {
  if (error) return this.emit('error', error);
  this.emit('pong');
};

Connection.prototype._initialize = function(message) {
  if (this.state !== 'connecting') return;

  if (message.protocol !== protocol.major) {
    return this.emit('error', new ShareDBError(
      ERROR_CODE.ERR_PROTOCOL_VERSION_NOT_SUPPORTED,
      'Unsupported protocol version: ' + message.protocol
    ));
  }
  if (types.map[message.type] !== types.defaultType) {
    return this.emit('error', new ShareDBError(
      ERROR_CODE.ERR_DEFAULT_TYPE_MISMATCH,
      message.type + ' does not match the server default type'
    ));
  }
  if (typeof message.id !== 'string') {
    return this.emit('error', new ShareDBError(
      ERROR_CODE.ERR_CLIENT_ID_BADLY_FORMED,
      'Client id must be a string'
    ));
  }
  this.id = message.id;

  this._setState('connected');
};

Connection.prototype.startTransaction = function() {
  var transaction = new Transaction(this);
  return this._transactions[transaction.id] = transaction;
};

Connection.prototype._commitTransaction = function(transaction) {
  this.send({a: ACTIONS.transactionCommit, id: transaction.id});
};

Connection.prototype._abortTransaction = function(transaction) {
  this.send({a: ACTIONS.transactionAbort, id: transaction.id});
};

Connection.prototype._handleTransactionCommit = function(error, message) {
  var transaction = this._transactions[message.id];
  if (!transaction) return;
  transaction._handleCommit(error, message);
  delete this._transactions[message.id];
};

Connection.prototype.getPresence = function(channel) {
  var connection = this;
  var presence = util.digOrCreate(this._presences, channel, function() {
    return new Presence(connection, channel);
  });
  presence._wantsDestroy = false;
  return presence;
};

Connection.prototype.getDocPresence = function(collection, id) {
  var channel = DocPresence.channel(collection, id);
  var connection = this;
  var presence = util.digOrCreate(this._presences, channel, function() {
    return new DocPresence(connection, collection, id);
  });
  presence._wantsDestroy = false;
  return presence;
};

Connection.prototype._sendPresenceAction = function(action, seq, presence) {
  // Ensure the presence is registered so that it receives the reply message
  this._addPresence(presence);
  var message = {a: action, ch: presence.channel, seq: seq};
  this.send(message);
  return message.seq;
};

Connection.prototype._addPresence = function(presence) {
  util.digOrCreate(this._presences, presence.channel, function() {
    return presence;
  });
};

Connection.prototype._requestRemotePresence = function(channel) {
  this.send({a: ACTIONS.presenceRequest, ch: channel});
};

Connection.prototype._handlePresenceSubscribe = function(error, message) {
  var presence = util.dig(this._presences, message.ch);
  if (presence) presence._handleSubscribe(error, message.seq);
};

Connection.prototype._handlePresenceUnsubscribe = function(error, message) {
  var presence = util.dig(this._presences, message.ch);
  if (presence) presence._handleUnsubscribe(error, message.seq);
};

Connection.prototype._handlePresence = function(error, message) {
  var presence = util.dig(this._presences, message.ch);
  if (presence) presence._receiveUpdate(error, message);
};

Connection.prototype._handlePresenceRequest = function(error, message) {
  var presence = util.dig(this._presences, message.ch);
  if (presence) presence._broadcastAllLocalPresence(error, message);
};
