const AbortedTransaction = require("./aborted-transaction");
const CommittedTransaction = require("./committed-transaction");

function PendingTransaction(transaction) {
  this._transaction = transaction;
  this._wantsCommit = false;
  this._retryCallbacks = null;
}
module.exports = PendingTransaction;

PendingTransaction.prototype.commit = function() {
  this._wantsCommit = true;
  this.update();
};

PendingTransaction.prototype.abort = function() {
  this._transaction._state = new AbortedTransaction(this._transaction);
};

PendingTransaction.prototype.registerSubmitRequest = function(request) {
  if (!this._transaction._requests.length) {
    this._transaction.backend.db.startTransaction(this._transaction.id, function(error) {
      // TODO: Handle errors / wait for callback
    });
  }
  this._transaction._requests.push(request);
  this.update();
};

PendingTransaction.prototype.retry = function(callback) {
  if (Array.isArray(this._retryCallbacks)) return this._retryCallbacks.push(callback);
  this._retryCallbacks = [callback];

  var state = this;
  var cb = function (error) {
    var callbacks = state._retryCallbacks;
    state._retryCallbacks = null;
    for (var callback of callbacks) callback(error);
  }

  var transaction = this._transaction;
  var db = transaction.backend.db;
  db.abortTransaction(this._transaction.id, function(error) {
    if (error) return cb(error);
    db.restartTransaction(transaction.id, function(error) {
      if (error) return cb(error);
      var requests = transaction._requests.slice();
      var retryNext = function(error) {
        if (error) return cb(error);
        var request = requests.shift();
        if (!request) return cb();
        request.retry(retryNext);
      }
      retryNext();
    });
  });
};

PendingTransaction.prototype.update = function() {
  if (!this._shouldCommit()) return;
  this._transaction._state = new CommittedTransaction(this._transaction);
};

PendingTransaction.prototype._shouldCommit = function() {
  if (!this._wantsCommit) return false;
  return this._transaction._requests.every(function(request) {
    return request._succeeded;
  });
};
