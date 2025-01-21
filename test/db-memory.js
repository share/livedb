var expect = require('chai').expect;
var Backend = require('../lib/backend');
var DB = require('../lib/db');
var BasicQueryableMemoryDB = require('./BasicQueryableMemoryDB');
var async = require('async');

describe('DB base class', function() {
  it('can call db.close() without callback', function() {
    var db = new DB();
    db.close();
  });

  it('can call db.close() with callback', function(done) {
    var db = new DB();
    db.close(done);
  });

  it('returns an error if db.commit() is unimplemented', function(done) {
    var db = new DB();
    db.commit('testcollection', 'test', {}, {}, null, function(err) {
      expect(err).instanceOf(Error);
      done();
    });
  });

  it('returns an error if db.getSnapshot() is unimplemented', function(done) {
    var db = new DB();
    db.getSnapshot('testcollection', 'foo', null, null, function(err) {
      expect(err).instanceOf(Error);
      done();
    });
  });

  it('returns an error if db.getOps() is unimplemented', function(done) {
    var db = new DB();
    db.getOps('testcollection', 'foo', 0, null, null, function(err) {
      expect(err).instanceOf(Error);
      done();
    });
  });

  it('returns an error if db.query() is unimplemented', function(done) {
    var db = new DB();
    db.query('testcollection', {x: 5}, null, null, function(err) {
      expect(err).instanceOf(Error);
      done();
    });
  });

  it('returns an error if db.queryPollDoc() is unimplemented', function(done) {
    var db = new DB();
    db.queryPollDoc('testcollection', 'foo', {x: 5}, null, function(err) {
      expect(err).instanceOf(Error);
      done();
    });
  });
});

describe('MemoryDB', function() {
  // Run all the DB-based tests against the BasicQueryableMemoryDB.
  require('./db')({
    create: function(options, callback) {
      if (typeof options === 'function') {
        callback = options;
        options = null;
      }
      var db = new BasicQueryableMemoryDB(options);
      callback(null, db);
    },
    getQuery: function(options) {
      return {filter: options.query, sort: options.sort};
    },
    transactions: true
  });

  describe('deleteOps', function() {
    describe('with some ops', function() {
      var backend;
      var db;
      var connection;
      var doc;

      beforeEach(function(done) {
        backend = new Backend();
        db = backend.db;
        connection = backend.connect();
        doc = connection.get('dogs', 'fido');

        async.waterfall([
          doc.create.bind(doc, {name: 'Fido'}),
          doc.submitOp.bind(doc, [{p: ['tricks'], oi: ['fetch']}]),
          db.getOps.bind(db, 'dogs', 'fido', null, null, null),
          function(ops, next) {
            expect(ops).to.have.length(2);
            next();
          }
        ], done);
      });

      it('deletes all ops', function(done) {
        async.waterfall([
          db.deleteOps.bind(db, 'dogs', 'fido', null, null, null),
          function(next) {
            db.getOps('dogs', 'fido', null, null, null, function(error) {
              expect(error.message).to.equal('Missing ops');
              next();
            });
          }
        ], done);
      });

      it('deletes some ops', function(done) {
        async.waterfall([
          db.deleteOps.bind(db, 'dogs', 'fido', 0, 1, null),
          db.getOps.bind(db, 'dogs', 'fido', 1, 2, null),
          function(ops, next) {
            expect(ops).to.have.length(1);
            expect(ops[0].op).to.eql([{p: ['tricks'], oi: ['fetch']}]);
            db.getOps('dogs', 'fido', 0, 1, null, function(error) {
              expect(error.message).to.equal('Missing ops');
              next();
            });
          }
        ], done);
      });

      it('submits more ops after deleting ops', function(done) {
        async.series([
          db.deleteOps.bind(db, 'dogs', 'fido', null, null, null),
          doc.submitOp.bind(doc, [{p: ['tricks', 1], li: 'sit'}]),
          function(next) {
            expect(doc.data.tricks).to.eql(['fetch', 'sit']);
            next();
          }
        ], done);
      });
    });
  });
});
