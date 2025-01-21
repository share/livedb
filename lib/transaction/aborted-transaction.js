var ShareDBError = require('../error');

var ERROR_CODE = ShareDBError.CODES;

function AbortedTransaction(transaction) {
  this._transaction = transaction;

  var self = this;
  transaction.backend.db.abortTransaction(this._transaction.id, function(abortError) {
    self._abortCallbacks(abortError && abortError.message);
  });
}
module.exports = AbortedTransaction;

AbortedTransaction.prototype.commit = function() {
  this.update();
};

AbortedTransaction.prototype.abort = function() {
  this.update();
};

AbortedTransaction.prototype.registerSubmitRequest = function() {
  // TODO: Error?
};

AbortedTransaction.prototype.update = function() {
  this._abortCallbacks();
};

AbortedTransaction.prototype._abortCallbacks = function(message) {
  message = message || 'Transaction aborted';
  var error = new ShareDBError(ERROR_CODE.ERR_TRANSACTION_ABORTED, message);
  this._transaction._callAndClearCallbacks(error);
};

