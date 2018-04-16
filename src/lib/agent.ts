var hat = require('hat');
var util = require('./util');
var types = require('./types');

/**
 * Agent deserializes the wire protocol messages received from the stream and
 * calls the corresponding functions on its Agent. It uses the return values
 * to send responses back. Agent also handles piping the operation streams
 * provided by a Agent.
 *
 * @param {Backend} backend
 * @param {Duplex} stream connection to a client
 */
class Agent {

    public backend: any;
    public stream: any;
    public clientId: any;
    public connectTime: any;
    public subscribedDocs: any;
    public subscribedQueries: any;
    public closed: any;
    public custom: any;

    constructor(backend, stream) {
        this.backend = backend;
        this.stream = stream;

        this.clientId = hat();
        this.connectTime = Date.now();

        // We need to track which documents are subscribed by the client. This is a
        // map of collection -> id -> stream
        this.subscribedDocs = {};

        // Map from queryId -> emitter
        this.subscribedQueries = {};

        // We need to track this manually to make sure we don't reply to messages
        // after the stream was closed.
        this.closed = false;

        // For custom use in middleware. The agent is a convenient place to cache
        // session state in memory. It is in memory only as long as the session is
        // active, and it is passed to each middleware call
        this.custom = {};

        // Initialize the remote client by sending it its agent Id.
        this.send({
            a: 'init',
            protocol: 1,
            id: this.clientId,
            type: types.defaultType.uri
        });
    }

// Close the agent with the client.
    public close(err) {
        if (err) {
            console.warn('Agent closed due to error', this.clientId, err.stack || err);
        }
        if (this.closed) return;
        // This will end the writable stream and emit 'finish'
        this.stream.end();
    };

    public _cleanup() {
        this.closed = true;

        // Clean up doc subscription streams
        for (var collection in this.subscribedDocs) {
            var docs = this.subscribedDocs[collection];
            for (var id in docs) {
                var stream = docs[id];
                stream.destroy();
            }
        }
        this.subscribedDocs = {};

        // Clean up query subscription streams
        for (var id in this.subscribedQueries) {
            var emitter = this.subscribedQueries[id];
            emitter.destroy();
        }
        this.subscribedQueries = {};
    };

    /**
     * Passes operation data received on stream to the agent stream via
     * _sendOp()
     */
    public _subscribeToStream(collection, id, stream) {
        if (this.closed) return stream.destroy();

        var streams = this.subscribedDocs[collection] || (this.subscribedDocs[collection] = {});

        // If already subscribed to this document, destroy the previously subscribed stream
        var previous = streams[id];
        if (previous) previous.destroy();
        streams[id] = stream;

        var agent = this;
        stream.on('data', function (data) {
            if (data.error) {
                // Log then silently ignore errors in a subscription stream, since these
                // may not be the client's fault, and they were not the result of a
                // direct request by the client
                console.error('Doc subscription stream error', collection, id, data.error);
                return;
            }
            if (agent._isOwnOp(collection, data)) return;
            agent._sendOp(collection, id, data);
        });
        stream.on('end', function () {
            // The op stream is done sending, so release its reference
            var streams = agent.subscribedDocs[collection];
            if (!streams) return;
            delete streams[id];
            if (util.hasKeys(streams)) return;
            delete agent.subscribedDocs[collection];
        });
    };

    public _subscribeToQuery(emitter, queryId, collection, query) {
        var previous = this.subscribedQueries[queryId];
        if (previous) previous.destroy();
        this.subscribedQueries[queryId] = emitter;

        var agent = this;
        emitter.onExtra = function (extra) {
            agent.send({a: 'q', id: queryId, extra: extra});
        };

        emitter.onDiff = function (diff) {
            for (var i = 0; i < diff.length; i++) {
                var item = diff[i];
                if (item.type === 'insert') {
                    item.values = Agent.getResultsData(item.values);
                }
            }
            // Consider stripping the collection out of the data we send here
            // if it matches the query's collection.
            agent.send({a: 'q', id: queryId, diff: diff});
        };

        emitter.onError = function (err) {
            // Log then silently ignore errors in a subscription stream, since these
            // may not be the client's fault, and they were not the result of a
            // direct request by the client
            console.error('Query subscription stream error', collection, query, err);
        };

        emitter.onOp = function (op) {
            var id = op.d;
            if (agent._isOwnOp(collection, op)) return;
            agent._sendOp(collection, id, op);
        };

        emitter._open();
    };

    public _isOwnOp(collection, op) {
        // Detect ops from this client on the same projection. Since the client sent
        // these in, the submit reply will be sufficient and we can silently ignore
        // them in the streams for subscribed documents or queries
        return (this.clientId === op.src) && (collection === (op.i || op.c));
    };

    public send(message) {
        // Quietly drop replies if the stream was closed
        if (this.closed) return;

        this.backend.emit('send', this, message);
        this.stream.write(message);
    };

    public _sendOp(collection, id, op) {
        var message: any = {
            a: 'op',
            c: collection,
            d: id,
            v: op.v,
            src: op.src,
            seq: op.seq
        };
        if (op.op) message.op = op.op;
        if (op.create) message.create = op.create;
        if (op.del) message.del = true;

        this.send(message);
    };

    public _sendOps(collection, id, ops) {
        for (var i = 0; i < ops.length; i++) {
            this._sendOp(collection, id, ops[i]);
        }
    };

    public _reply(request, err, message) {
        if (err) {
            if (typeof err === 'string') {
                request.error = {
                    code: 4001,
                    message: err
                };
            } else {
                if (err.stack) {
                    console.warn(err.stack);
                }
                request.error = {
                    code: err.code,
                    message: err.message
                };
            }
            this.send(request);
            return;
        }
        if (!message) message = {};

        message.a = request.a;
        if (request.id) {
            message.id = request.id;
        } else {
            if (request.c) message.c = request.c;
            if (request.d) message.d = request.d;
            if (request.b && !message.data) message.b = request.b;
        }

        this.send(message);
    };

// Start processing events from the stream
    public _open() {
        if (this.closed) return;
        this.backend.agentsCount++;
        if (!this.stream.isServer) this.backend.remoteAgentsCount++;

        var agent = this;
        this.stream.on('data', function (chunk) {
            if (agent.closed) return;

            if (typeof chunk !== 'object') {
                var err = {code: 4000, message: 'Received non-object message'};
                return agent.close(err);
            }

            var request = {data: chunk};
            agent.backend.trigger('receive', agent, request, function (err) {
                var callback = function (err, message?) {
                    agent._reply(request.data, err, message);
                };
                if (err) return callback(err);
                agent._handleMessage(request.data, callback);
            });
        });

        this.stream.on('end', function () {
            agent.backend.agentsCount--;
            if (!agent.stream.isServer) agent.backend.remoteAgentsCount--;
            agent._cleanup();
        });
    };

// Check a request to see if its valid. Returns an error if there's a problem.
    public _checkRequest(request) {
        if (request.a === 'qf' || request.a === 'qs' || request.a === 'qu') {
            // Query messages need an ID property.
            if (typeof request.id !== 'number') return 'Missing query ID';
        } else if (request.a === 'op' || request.a === 'f' || request.a === 's' || request.a === 'u') {
            // Doc-based request.
            if (request.c != null && typeof request.c !== 'string') return 'Invalid collection';
            if (request.d != null && typeof request.d !== 'string') return 'Invalid id';

            if (request.a === 'op') {
                if (request.v != null && (typeof request.v !== 'number' || request.v < 0)) return 'Invalid version';
            }
        } else if (request.a === 'bf' || request.a === 'bs' || request.a === 'bu') {
            // Bulk request
            if (request.c != null && typeof request.c !== 'string') return 'Invalid collection';
            if (typeof request.b !== 'object') return 'Invalid bulk subscribe data';
        }
    };

// Handle an incoming message from the client
    public _handleMessage(request, callback) {
        try {
            var errMessage = this._checkRequest(request);
            if (errMessage) return callback({code: 4000, message: errMessage});

            switch (request.a) {
                case 'qf':
                    return this._queryFetch(request.id, request.c, request.q, Agent.getQueryOptions(request), callback);
                case 'qs':
                    return this._querySubscribe(request.id, request.c, request.q, Agent.getQueryOptions(request), callback);
                case 'qu':
                    return this._queryUnsubscribe(request.id, callback);
                case 'bf':
                    return this._fetchBulk(request.c, request.b, callback);
                case 'bs':
                    return this._subscribeBulk(request.c, request.b, callback);
                case 'bu':
                    return this._unsubscribeBulk(request.c, request.b, callback);
                case 'f':
                    return this._fetch(request.c, request.d, request.v, callback);
                case 's':
                    return this._subscribe(request.c, request.d, request.v, callback);
                case 'u':
                    return this._unsubscribe(request.c, request.d, callback);
                case 'op':
                    var op = this._createOp(request);
                    if (!op) return callback({code: 4000, message: 'Invalid op message'});
                    return this._submit(request.c, request.d, op, callback);
                default:
                    callback({code: 4000, message: 'Invalid or unknown message'});
            }
        } catch (err) {
            callback(err);
        }
    };

    static getQueryOptions(request) {
        var results = request.r;
        var ids, fetch, fetchOps;
        if (results) {
            ids = [];
            for (var i = 0; i < results.length; i++) {
                var result = results[i];
                var id = result[0];
                var version = result[1];
                ids.push(id);
                if (version == null) {
                    if (fetch) {
                        fetch.push(id);
                    } else {
                        fetch = [id];
                    }
                } else {
                    if (!fetchOps) fetchOps = {};
                    fetchOps[id] = version;
                }
            }
        }
        var options = request.o || {};
        options.ids = ids;
        options.fetch = fetch;
        options.fetchOps = fetchOps;
        return options;
    }

    public _queryFetch(queryId, collection, query, options, callback) {
        // Fetch the results of a query once
        var agent = this;
        this.backend.queryFetch(this, collection, query, options, function (err, results, extra) {
            if (err) return callback(err);
            var message = {
                data: Agent.getResultsData(results),
                extra: extra
            };
            callback(null, message);
        });
    };

    public _querySubscribe(queryId, collection, query, options, callback) {
        // Subscribe to a query. The client is sent the query results and its
        // notified whenever there's a change
        var agent = this;
        var wait = 1;
        var message;

        function finish(err?) {
            if (err) return callback(err);
            if (--wait) return;
            callback(null, message);
        }

        if (options.fetch) {
            wait++;
            this.backend.fetchBulk(this, collection, options.fetch, function (err, snapshotMap) {
                if (err) return finish(err);
                message = {data: Agent.getMapData(snapshotMap)};
                finish();
            });
        }
        if (options.fetchOps) {
            wait++;
            this._fetchBulkOps(collection, options.fetchOps, finish);
        }
        this.backend.querySubscribe(this, collection, query, options, (err, emitter, results, extra) => {
            if (err) return finish(err);
            if (this.closed) return emitter.destroy();

            agent._subscribeToQuery(emitter, queryId, collection, query);
            // No results are returned when ids are passed in as an option. Instead,
            // want to re-poll the entire query once we've established listeners to
            // emit any diff in results
            if (!results) {
                emitter.queryPoll(finish);
                return;
            }
            message = {
                data: Agent.getResultsData(results),
                extra: extra
            };
            finish();
        });
    };

    static getResultsData(results) {
        var items: any = [];
        for (var i = 0; i < results.length; i++) {
            var result = results[i];
            var item = Agent.getSnapshotData(result);
            item.d = result.id;
            items.push(item);
        }
        return items;
    }

    static getMapData(snapshotMap) {
        var data = {};
        for (var id in snapshotMap) {
            data[id] = Agent.getSnapshotData(snapshotMap[id]);
        }
        return data;
    }

    static getSnapshotData(snapshot) {
        var data: any = {
            v: snapshot.v,
            data: snapshot.data
        };
        if (types.defaultType !== types.map[snapshot.type]) {
            data.type = snapshot.type;
        }
        return data;
    }

    public _queryUnsubscribe(queryId, callback) {
        var emitter = this.subscribedQueries[queryId];
        if (emitter) {
            emitter.destroy();
            delete this.subscribedQueries[queryId];
        }
        process.nextTick(callback);
    };

    public _fetch(collection, id, version, callback) {
        if (version == null) {
            // Fetch a snapshot
            this.backend.fetch(this, collection, id, function (err, snapshot) {
                if (err) return callback(err);
                callback(null, {data: Agent.getSnapshotData(snapshot)});
            });
        } else {
            // It says fetch on the tin, but if a version is specified the client
            // actually wants me to fetch some ops
            this._fetchOps(collection, id, version, callback);
        }
    };

    public _fetchOps(collection, id, version, callback) {
        var agent = this;
        this.backend.getOps(this, collection, id, version, null, function (err, ops) {
            if (err) return callback(err);
            agent._sendOps(collection, id, ops);
            callback();
        });
    };

    public _fetchBulk(collection, versions, callback) {
        if (Array.isArray(versions)) {
            this.backend.fetchBulk(this, collection, versions, function (err, snapshotMap) {
                if (err) return callback(err);
                callback(null, {data: Agent.getMapData(snapshotMap)});
            });
        } else {
            this._fetchBulkOps(collection, versions, callback);
        }
    };

    public _fetchBulkOps(collection, versions, callback) {
        var agent = this;
        this.backend.getOpsBulk(this, collection, versions, null, function (err, opsMap) {
            if (err) return callback(err);
            for (var id in opsMap) {
                var ops = opsMap[id];
                agent._sendOps(collection, id, ops);
            }
            callback();
        });
    };

    public _subscribe(collection, id, version, callback) {
        // If the version is specified, catch the client up by sending all ops
        // since the specified version
        var agent = this;
        this.backend.subscribe(this, collection, id, version, function (err, stream, snapshot) {
            if (err) return callback(err);
            agent._subscribeToStream(collection, id, stream);
            // Snapshot is returned only when subscribing from a null version.
            // Otherwise, ops will have been pushed into the stream
            if (snapshot) {
                callback(null, {data: Agent.getSnapshotData(snapshot)});
            } else {
                callback();
            }
        });
    };

    public _subscribeBulk(collection, versions, callback) {
        var agent = this;
        this.backend.subscribeBulk(this, collection, versions, function (err, streams, snapshotMap) {
            if (err) return callback(err);
            for (var id in streams) {
                agent._subscribeToStream(collection, id, streams[id]);
            }
            if (snapshotMap) {
                callback(null, {data: Agent.getMapData(snapshotMap)});
            } else {
                callback();
            }
        });
    };

    public _unsubscribe(collection, id, callback) {
        // Unsubscribe from the specified document. This cancels the active
        // stream or an inflight subscribing state
        var docs = this.subscribedDocs[collection];
        var stream = docs && docs[id];
        if (stream) stream.destroy();
        process.nextTick(callback);
    };

    public _unsubscribeBulk(collection, ids, callback) {
        var docs = this.subscribedDocs[collection];
        if (!docs) return process.nextTick(callback);
        for (var i = 0; i < ids.length; i++) {
            var id = ids[i];
            var stream = docs[id];
            if (stream) stream.destroy();
        }
        process.nextTick(callback);
    };

    public _submit(collection, id, op, callback) {
        var agent = this;
        this.backend.submit(this, collection, id, op, null, function (err, ops) {
            // Message to acknowledge the op was successfully submitted
            var ack = {src: op.src, seq: op.seq, v: op.v};
            if (err) {
                // Occassional 'Op already submitted' errors are expected to happen as
                // part of normal operation, since inflight ops need to be resent after
                // disconnect. In this case, ack the op so the client can proceed
                if (err.code === 4001) return callback(null, ack);
                return callback(err);
            }

            // Reply with any operations that the client is missing.
            agent._sendOps(collection, id, ops);
            callback(null, ack);
        });
    };

// Normalize the properties submitted
    public _createOp(request) {
        // src can be provided if it is not the same as the current agent,
        // such as a resubmission after a reconnect, but it usually isn't needed
        var src = request.src || this.clientId;
        if (request.op) {
            return new EditOp(src, request.seq, request.v, request.op);
        } else if (request.create) {
            return new CreateOp(src, request.seq, request.v, request.create);
        } else if (request.del) {
            return new DeleteOp(src, request.seq, request.v, request.del);
        }
    };
}


class CreateOp {
    public src: any;
    public seq: any;
    public v: any;
    public create: any;
    public m: any;

    constructor(src, seq, v, create) {
        this.src = src;
        this.seq = seq;
        this.v = v;
        this.create = create;
        this.m = null;
    }
}

class EditOp {
    public src: any;
    public seq: any;
    public v: any;
    public op: any;
    public m: any;

    constructor(src, seq, v, op) {
        this.src = src;
        this.seq = seq;
        this.v = v;
        this.op = op;
        this.m = null;
    }
}

class DeleteOp {
    public src: any;
    public seq: any;
    public v: any;
    public del: any;
    public m: any;

    constructor(src, seq, v, del) {
        this.src = src;
        this.seq = seq;
        this.v = v;
        this.del = del;
        this.m = null;
    }
}

module.exports = Agent;