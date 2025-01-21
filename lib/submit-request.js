var ot = require('./ot');
var projections = require('./projections');
var ShareDBError = require('./error');
var types = require('./types');
var protocol = require('./protocol');
var util = require('./util');
var Transaction = require('./transaction/transaction');

var ERROR_CODE = ShareDBError.CODES;

function SubmitRequest(backend, agent, index, id, op, options) {
  this.backend = backend;
  this.agent = agent;
  // If a projection, rewrite the call into a call against the collection
  var projection = backend.projections[index];
  this.index = index;
  this.projection = projection;
  this.collection = (projection) ? projection.target : index;
  this.id = id;
  this.op = op;
  this.options = options || {};

  this.extra = op.x;
  delete op.x;

  this.start = Date.now();
  this._addOpMeta();

  // Set as this request is sent through middleware
  this.action = null;
  // For custom use in middleware
  this.custom = Object.create(null);

  // Whether or not to store a milestone snapshot. If left as null, the milestone
  // snapshots are saved according to the interval provided to the milestone db
  // options. If overridden to a boolean value, then that value is used instead of
  // the interval logic.
  this.saveMilestoneSnapshot = null;
  this.suppressPublish = backend.suppressPublish;
  this.maxRetries = backend.maxSubmitRetries;
  this.retries = 0;

  // return values
  this.snapshot = null;
  this.ops = [];
  this.channels = null;
  this._fixupOps = [];

  this._succeeded = false;
  if (this.options.transaction) {
    this.transaction = util.digOrCreate(agent._transactions, options.transaction, function() {
      return new Transaction(agent, options.transaction);
    });
    this.transaction.registerSubmitRequest(this);
  }
}
module.exports = SubmitRequest;

SubmitRequest.prototype.$fixup = function(op) {
  if (this.action !== this.backend.MIDDLEWARE_ACTIONS.apply) {
    throw new ShareDBError(
      ERROR_CODE.ERR_FIXUP_IS_ONLY_VALID_ON_APPLY,
      'fixup can only be called during the apply middleware'
    );
  }

  if (this.op.del) {
    throw new ShareDBError(
      ERROR_CODE.ERR_CANNOT_FIXUP_DELETION,
      'fixup cannot be applied on deletion ops'
    );
  }

  var typeId = this.op.create ? this.op.create.type : this.snapshot.type;
  var type = types.map[typeId];
  if (typeof type.compose !== 'function') {
    throw new ShareDBError(
      ERROR_CODE.ERR_TYPE_DOES_NOT_SUPPORT_COMPOSE,
      typeId + ' does not support compose'
    );
  }

  if (this.op.create) this.op.create.data = type.apply(this.op.create.data, op);
  else this.op.op = type.compose(this.op.op, op);

  var fixupOp = {
    src: this.op.src,
    seq: this.op.seq,
    v: this.op.v,
    op: op
  };

  this._fixupOps.push(fixupOp);
};

SubmitRequest.prototype.submit = function(callback) {
  var request = this;
  this._succeeded = false;
  var backend = this.backend;
  var collection = this.collection;
  var id = this.id;
  var op = this.op;
  // Send a special projection so that getSnapshot knows to return all fields.
  // With a null projection, it strips document metadata
  var fields = {$submit: true};

  if (request.transaction) {
    var originalCallback = callback;
    callback = function(error) {
      if (error) request.transaction.abort();
      originalCallback(error);
    }
  }

  var snapshotOptions = {};
  snapshotOptions.agentCustom = request.agent.custom;
  backend.db.getSnapshot(collection, id, fields, snapshotOptions, function(err, snapshot) {
    if (err) return callback(err);

    var transactionOps = [];
    if (request.transaction) {
      // Get other ops in the same transaction that have not yet been committed to the DB
      var transactionOps = request.transaction.pendingOpsUntil(request);
      var error = ot.applyOps(snapshot, transactionOps);
      if (error) return callback(error);
    }

    request.snapshot = snapshot;
    request._addSnapshotMeta();

    if (op.v == null) {
      if (op.create && snapshot.type && op.src) {
        // If the document was already created by another op, we will return a
        // 'Document already exists' error in response and fail to submit this
        // op. However, this could also happen in the case that the op was
        // already committed and the create op was simply resent. In that
        // case, we should return a non-fatal 'Op already submitted' error. We
        // must get the past ops and check their src and seq values to
        // differentiate.
        request._fetchCreateOpVersion(function(error, version) {
          if (error) return callback(error);
          if (version == null) {
            callback(request.alreadyCreatedError());
          } else {
            op.v = version;
            callback(request.alreadySubmittedError());
          }
        });
        return;
      }

      // Submitting an op with a null version means that it should get the
      // version from the latest snapshot. Generally this will mean the op
      // won't be transformed, though transform could be called on it in the
      // case of a retry from a simultaneous submit
      op.v = snapshot.v;
    }

    if (op.v === snapshot.v) {
      // The snapshot hasn't changed since the op's base version. Apply
      // without transforming the op
      return request.apply(callback);
    }

    if (op.v > snapshot.v) {
      // The op version should be from a previous snapshot, so it should never
      // never exceed the current snapshot's version
      return callback(request.newerVersionError());
    }

    var from = op.v - transactionOps.length;
    var to = snapshot.v - transactionOps.length;
    // Transform the op up to the current snapshot version, then apply
    backend.db.getOps(collection, id, from, to, {metadata: true}, function(err, ops) {
      if (err) return callback(err);

      if (ops.length !== to - from) {
        return callback(request.missingOpsError());
      }

      for (var o of ops) o.v = o.v + transactionOps.length;

      err = request._transformOp(ops);
      if (err) return callback(err);

      var skipNoOp = backend.doNotCommitNoOps &&
        protocol.checkAtLeast(request.agent.protocol, '1.2') &&
        request.op.op &&
        request.op.op.length === 0;

      if (skipNoOp) {
        // The op is a no-op, either because it was submitted as such, or - more
        // likely - because it was transformed into one. Let's avoid committing it
        // and tell the client.
        return callback(request.noOpError());
      }

      if (op.v !== snapshot.v) {
        // This shouldn't happen, but is just a final sanity check to make
        // sure we have transformed the op to the current snapshot version
        return callback(request.versionAfterTransformError());
      }

      request.apply(callback);
    });
  });
};

SubmitRequest.prototype.apply = function(callback) {
  // If we're being projected, verify that the op is allowed
  var projection = this.projection;
  if (projection && !projections.isOpAllowed(this.snapshot.type, projection.fields, this.op)) {
    return callback(this.projectionError());
  }

  // Always set the channels before each attempt to apply. If the channels are
  // modified in a middleware and we retry, we want to reset to a new array
  this.channels = this.backend.getChannels(this.collection, this.id);
  this._fixupOps = [];
  delete this.op.m.fixup;

  var request = this;
  this.backend.trigger(this.backend.MIDDLEWARE_ACTIONS.apply, this.agent, this, function(err) {
    if (err) return callback(err);

    // Apply the submitted op to the snapshot
    err = ot.apply(request.snapshot, request.op);
    if (err) return callback(err);

    request.commit(callback);
  });
};

SubmitRequest.prototype.commit = function(callback) {
  var request = this;
  var backend = this.backend;
  backend.trigger(backend.MIDDLEWARE_ACTIONS.commit, this.agent, this, function(err) {
    if (err) return callback(err);
    if (request._fixupOps.length) request.op.m.fixup = request._fixupOps;
    if (request.op.create) {
      // When we create the snapshot, we store a pointer to the op that created
      // it. This allows us to return OP_ALREADY_SUBMITTED errors when appropriate.
      request.snapshot.m._create = {
        src: request.op.src,
        seq: request.op.seq,
        v: request.op.v
      };
    }

    // Try committing the operation and snapshot to the database atomically
    backend.db.commit(
      request.collection,
      request.id,
      request.op,
      request.snapshot,
      request.options,
      function(err, succeeded) {
        // TODO: Should this callback be called before committing a transaction?
        if (err) return callback(err);
        if (!succeeded) {
          // Between our fetch and our call to commit, another client committed an
          // operation. We expect this to be relatively infrequent but normal.
          // TODO: What if multiple docs fail and trigger a retry?
          if (request.transaction) return request.transaction.retry(callback);
          else return request.retry(callback);
        }
        request._succeeded = !!succeeded;
        if (request.transaction) request.transaction.update();
        else request.publish();
        callback();
      });
  });
};

SubmitRequest.prototype.publish = function() {
  if (!this.suppressPublish) {
    var op = this.op;
    op.c = this.collection;
    op.d = this.id;
    op.m = undefined;
    // Needed for agent to detect if it can ignore sending the op back to
    // the client that submitted it in subscriptions
    if (this.collection !== this.index) op.i = this.index;
    this.backend.pubsub.publish(this.channels, op);
  }

  if (this._shouldSaveMilestoneSnapshot(this.snapshot)) {
    this.backend.milestoneDb.saveMilestoneSnapshot(this.collection, this.snapshot);
  }
};

SubmitRequest.prototype.retry = function(callback) {
  this.retries++;
  if (this.maxRetries != null && this.retries > this.maxRetries) {
    return callback(this.maxRetriesError());
  }
  this.backend.emit('timing', 'submit.retry', Date.now() - this.start, this);
  this.submit(callback);
};

SubmitRequest.prototype._transformOp = function(ops) {
  var type = this.snapshot.type;
  for (var i = 0; i < ops.length; i++) {
    var op = ops[i];

    if (this.op.src && this.op.src === op.src && this.op.seq === op.seq) {
      // The op has already been submitted. There are a variety of ways this
      // can happen in normal operation, such as a client resending an
      // unacknowledged operation at reconnect. It's important we don't apply
      // the same op twice
      if (op.m.fixup) this._fixupOps = op.m.fixup;
      return this.alreadySubmittedError();
    }

    if (this.op.v !== op.v) {
      return this.versionDuringTransformError();
    }

    var err = ot.transform(type, this.op, op);
    if (err) return err;
    delete op.m;
    this.ops.push(op);
  }
};

SubmitRequest.prototype._addOpMeta = function() {
  this.op.m = {
    ts: this.start
  };
  if (this.op.create) {
    // Consistently store the full URI of the type, not just its short name
    this.op.create.type = ot.normalizeType(this.op.create.type);
  }
};

SubmitRequest.prototype._addSnapshotMeta = function() {
  var meta = this.snapshot.m || (this.snapshot.m = {});
  if (this.op.create) {
    meta.ctime = this.start;
  } else if (this.op.del) {
    this.op.m.data = this.snapshot.data;
  }
  meta.mtime = this.start;
};

SubmitRequest.prototype._shouldSaveMilestoneSnapshot = function(snapshot) {
  // If the flag is null, it's not been overridden by the consumer, so apply the interval
  if (this.saveMilestoneSnapshot === null) {
    return snapshot && snapshot.v % this.backend.milestoneDb.interval === 0;
  }

  return this.saveMilestoneSnapshot;
};

SubmitRequest.prototype._fetchCreateOpVersion = function(callback) {
  var create = this.snapshot.m._create;
  if (create) {
    var version = (create.src === this.op.src && create.seq === this.op.seq) ? create.v : null;
    return callback(null, version);
  }

  // We can only reach here if the snapshot is missing the create metadata.
  // This can happen if a client tries to re-create or resubmit a create op to
  // a "legacy" snapshot that existed before we started adding the meta (should
  // be uncommon) or when using a driver that doesn't support metadata (eg Postgres).
  this.backend.db.getCommittedOpVersion(this.collection, this.id, this.snapshot, this.op, null, callback);
};

// Non-fatal client errors:
SubmitRequest.prototype.alreadySubmittedError = function() {
  return new ShareDBError(ERROR_CODE.ERR_OP_ALREADY_SUBMITTED, 'Op already submitted');
};
SubmitRequest.prototype.rejectedError = function() {
  return new ShareDBError(ERROR_CODE.ERR_OP_SUBMIT_REJECTED, 'Op submit rejected');
};
// Fatal client errors:
SubmitRequest.prototype.alreadyCreatedError = function() {
  return new ShareDBError(ERROR_CODE.ERR_DOC_ALREADY_CREATED, 'Invalid op submitted. Document already created');
};
SubmitRequest.prototype.newerVersionError = function() {
  return new ShareDBError(
    ERROR_CODE.ERR_OP_VERSION_NEWER_THAN_CURRENT_SNAPSHOT,
    'Invalid op submitted. Op version newer than current snapshot'
  );
};
SubmitRequest.prototype.projectionError = function() {
  return new ShareDBError(
    ERROR_CODE.ERR_OP_NOT_ALLOWED_IN_PROJECTION,
    'Invalid op submitted. Operation invalid in projected collection'
  );
};
// Fatal internal errors:
SubmitRequest.prototype.missingOpsError = function() {
  return new ShareDBError(
    ERROR_CODE.ERR_SUBMIT_TRANSFORM_OPS_NOT_FOUND,
    'Op submit failed. DB missing ops needed to transform it up to the current snapshot version'
  );
};
SubmitRequest.prototype.versionDuringTransformError = function() {
  return new ShareDBError(
    ERROR_CODE.ERR_OP_VERSION_MISMATCH_DURING_TRANSFORM,
    'Op submit failed. Versions mismatched during op transform'
  );
};
SubmitRequest.prototype.versionAfterTransformError = function() {
  return new ShareDBError(
    ERROR_CODE.ERR_OP_VERSION_MISMATCH_AFTER_TRANSFORM,
    'Op submit failed. Op version mismatches snapshot after op transform'
  );
};
SubmitRequest.prototype.maxRetriesError = function() {
  return new ShareDBError(
    ERROR_CODE.ERR_MAX_SUBMIT_RETRIES_EXCEEDED,
    'Op submit failed. Exceeded max submit retries of ' + this.maxRetries
  );
};
SubmitRequest.prototype.noOpError = function() {
  return new ShareDBError(
    ERROR_CODE.ERR_NO_OP,
    'Op is a no-op. Skipping apply.'
  );
};
