var emitter = require('../emitter');

var idCounter = 1;

module.exports = Transaction;
function Transaction(connection) {
  emitter.EventEmitter.call(this);

  this.connection = connection;
  this.id = (idCounter++).toString();

  this._callbacks = [];
  this._writeable = true;
}
emitter.mixin(Transaction);

Transaction.prototype.commit = function(callback) {
  // TODO: Catch multiple calls
  // TODO: Handle network changes
  this._callbacks.push(callback);
  this._writeable = false;
  this.connection._commitTransaction(this);
};

Transaction.prototype.abort = function(callback) {
  this._callbacks.push(callback);
  this._writeable = false;
};

Transaction.prototype._handleCommit = function(error) {
  this._writeable = false;
  // TODO: Handle callbacks
  if (error) this._localAbort(error);
  else this.emit('commit');

  var callbacks = this._callbacks;
  this._callbacks = [];
  if (!callbacks.length) this.emit('error', error);
  for (var callback of callbacks) callback(error);
};

Transaction.prototype._localAbort = function(error) {
  this._writeable = false;
  this.emit('abort', error);
};
