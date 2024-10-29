var nextTickImpl = require('./next-tick');

exports.doNothing = doNothing;
function doNothing() {}

exports.hasKeys = function(object) {
  for (var key in object) return true;
  return false;
};

var hasOwn;
exports.hasOwn = hasOwn = Object.hasOwn || Object.prototype.hasOwnProperty.call;

// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Number/isInteger#Polyfill
exports.isInteger = Number.isInteger || function(value) {
  return typeof value === 'number' &&
    isFinite(value) &&
    Math.floor(value) === value;
};

exports.isValidVersion = function(version) {
  if (version === null) return true;
  return exports.isInteger(version) && version >= 0;
};

exports.isValidTimestamp = function(timestamp) {
  return exports.isValidVersion(timestamp);
};

exports.MAX_SAFE_INTEGER = 9007199254740991;

exports.dig = function() {
  var obj = arguments[0];
  for (var i = 1; i < arguments.length; i++) {
    var key = arguments[i];
    obj = hasOwn(obj, key) ? obj[key] : (i === arguments.length - 1 ? undefined : Object.create(null));
  }
  return obj;
};

exports.digOrCreate = function() {
  var obj = arguments[0];
  var createCallback = arguments[arguments.length - 1];
  for (var i = 1; i < arguments.length - 1; i++) {
    var key = arguments[i];
    obj = hasOwn(obj, key) ? obj[key] :
      (obj[key] = i === arguments.length - 2 ? createCallback() : Object.create(null));
  }
  return obj;
};

exports.digAndRemove = function() {
  var obj = arguments[0];
  var objects = [obj];
  for (var i = 1; i < arguments.length - 1; i++) {
    var key = arguments[i];
    if (!hasOwn(obj, key)) break;
    obj = obj[key];
    objects.push(obj);
  };

  for (var i = objects.length - 1; i >= 0; i--) {
    var parent = objects[i];
    var key = arguments[i + 1];
    var child = parent[key];
    if (i === objects.length - 1 || !exports.hasKeys(child)) delete parent[key];
  }
};

exports.supportsPresence = function(type) {
  return type && typeof type.transformPresence === 'function';
};

exports.callEach = function(callbacks, error) {
  var called = false;
  callbacks.forEach(function(callback) {
    if (callback) {
      callback(error);
      called = true;
    }
  });
  return called;
};

exports.truthy = function(arg) {
  return !!arg;
};

if (typeof process !== 'undefined' && typeof process.nextTick === 'function') {
  exports.nextTick = process.nextTick;
} else if (typeof MessageChannel !== 'undefined') {
  exports.nextTick = nextTickImpl.messageChannel;
} else {
  exports.nextTick = nextTickImpl.setTimeout;
}

exports.clone = function(obj) {
  return (obj === undefined) ? undefined : JSON.parse(JSON.stringify(obj));
};

exports.deduplicateRequests = function(fn) {
  var callbacksByArgs = {};
  return function() {
    var args = Array.prototype.slice.call(arguments);
    var callback = args.pop();
    var argString = JSON.stringify(args);

    var callbacks = exports.digOrCreate(callbacksByArgs, argString, function() {
      return [];
    });
    callbacks.push(callback);

    if (callbacks.length > 1) return;

    args.push(function() {
      while (callbacks.length) {
        var cb = callbacks.shift();
        cb.apply(null, arguments);
      }
      delete callbacksByArgs[argString];
    });

    fn.apply(null, args);
  };
};

var objectProtoPropNames = Object.create(null);
Object.getOwnPropertyNames(Object.prototype).forEach(function(prop) {
  if (prop !== '__proto__') {
    objectProtoPropNames[prop] = true;
  }
});
exports.isDangerousProperty = function(propName) {
  return propName === '__proto__' || objectProtoPropNames[propName];
};

try {
  var util = require('util');
  if (typeof util.inherits !== 'function') throw new Error('Could not find util.inherits()');
  exports.inherits = util.inherits;
} catch (e) {
  try {
    exports.inherits = require('inherits');
  } catch (e) {
    throw new Error('If running sharedb in a browser, please install the "inherits" or "util" package');
  }
}
