var PendingTransaction = require('./pending-transaction');

function Transaction(agent, id) {
  this.agent = agent;
  this.backend = agent.backend;
  this.id = id;

  this._state = new PendingTransaction(this);
  this._requests = [];
  this._callbacks = [];
}
module.exports = Transaction;

Transaction.prototype.commit = function(callback) {
  this._callbacks.push(callback);
  this._state.commit();
};

Transaction.prototype.abort = function(callback) {
  this._callbacks.push(callback);
  this._state.abort();
};

Transaction.prototype.registerSubmitRequest = function(request) {
  this._state.registerSubmitRequest(request);
};

Transaction.prototype.update = function() {
  this._state.update();
};

Transaction.prototype.retry = function(callback) {
  this._state.retry(callback);
};

Transaction.prototype.pendingOpsUntil = function(untilRequest) {
  var collection = untilRequest.collection;
  var id = untilRequest.id;
  var ops = [];
  for (var request of this._requests) {
    if (request === untilRequest) break;
    if (request.collection === collection && request.id === id) {
      ops.push(request.op);
    }
  }
  return ops;
};

Transaction.prototype._callAndClearCallbacks = function(error) {
  var callbacks = this._callbacks;
  this._callbacks = [];
  for (var callback of callbacks) {
    if (typeof callback === 'function') callback(error);
  }
};
