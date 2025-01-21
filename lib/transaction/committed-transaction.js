function CommittedTransaction(transaction) {
  this._transaction = transaction;
    // TODO: Check DB support
  transaction.backend.db.commitTransaction(this._transaction.id, function(error) {
    transaction._callAndClearCallbacks(error);
  });
  // TODO: call request.publish() on transaction requests
}
module.exports = CommittedTransaction;

CommittedTransaction.prototype.commit = function() {
  // TODO: Error?
};

CommittedTransaction.prototype.abort = function() {
  // TODO: Error?
};

CommittedTransaction.prototype.registerSubmitRequest = function() {
  // TODO: Error?
};

CommittedTransaction.prototype.update = function() {};
