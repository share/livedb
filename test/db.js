var async = require('async');
var expect = require('chai').expect;
var Backend = require('../lib/backend');
var ot = require('../lib/ot');

/**
 * Run the DB test suite. Useful for testing external DB adapters:
 * require('sharedb/test/db')(options);
 *
 * options.create() should instantiate a database adapter and call the
 * provided callback with it.
 * options.create = function(callback) {
 *   // Create instance of Db
 *   callback(error, db);
 * }
 *
 * options.getQuery() is optional, and only needed if the adapter supports
 * queries. It needs to accept the "generic" query provided by the
 * test suite, and return a query specific to the adapter.
 * options.getQuery = function(genericQuery) {
 *   // map genericQuery into adapter-specific query
 *   return mappedQuery;
 * }
 */
module.exports = function(options) {
  var create = options.create;

  describe('db', function() {
    beforeEach(function(done) {
      var self = this;
      create(function(err, db) {
        if (err) return done(err);
        self.db = db;
        self.backend = new Backend({db: db});
        done();
      });
    });

    afterEach(function(done) {
      this.backend.close(done);
    });

    it('can call db.close() without callback', function(done) {
      create(function(err, db) {
        if (err) return done(err);
        db.close();
        done();
      });
    });

    it('calls db.close() multiple times', function(done) {
      var db = this.db;
      db.close(function(error) {
        if (error) return done(error);
        db.close(function(error) {
          done(error);
        });
      });
    });

    if (options.getQuery) {
      require('./client/query-subscribe')({getQuery: options.getQuery});
      require('./client/query')({getQuery: options.getQuery});
      require('./client/projections')({getQuery: options.getQuery});
    }

    if (options.transactions) {
      require('./client/transaction')();
    }

    require('./client/submit')();
    require('./client/submit-json1')();
    require('./client/subscribe')();

    // Simplified mock of how submit request applies operations. The
    // noteworthy dependency is that it always calls getSnapshot with
    // the {$submit: true} projection and applies the op to the returned
    // snapshot before committing. Thus, commit may rely on the behavior
    // of getSnapshot with this special projection
    function submit(db, collection, id, op, callback) {
      db.getSnapshot(collection, id, {$submit: true}, null, function(err, snapshot) {
        if (err) return callback(err);
        if (snapshot.v !== op.v) {
          var succeeded = false;
          return callback(null, succeeded);
        }
        var err = ot.apply(snapshot, op);
        if (err) return callback(err);
        db.commit(collection, id, op, snapshot, null, callback);
      });
    }

    describe('commit', function() {
      function commitConcurrent(db, ops, test, done) {
        var numSucceeded = 0;
        async.each(ops, function(op, eachCb) {
          submit(db, 'testcollection', 'foo', op, function(err, succeeded) {
            if (err) return eachCb(err);
            if (succeeded) numSucceeded++;
            eachCb();
          });
        }, function(err) {
          if (err) return done(err);
          expect(numSucceeded).equal(1);
          if (!test) return done();
          db.getOps('testcollection', 'foo', 0, null, null, function(err, opsOut) {
            if (err) return done(err);
            db.getSnapshot('testcollection', 'foo', null, null, function(err, snapshotOut) {
              if (err) return done(err);
              test(opsOut, snapshotOut);
              done();
            });
          });
        });
      }

      function testCreateCommit(ops, snapshot) {
        expect(snapshot.v).eql(1);
        expect(ops.length).equal(1);
        expect(ops[0].create).ok;
      }

      it('one commit succeeds from 2 simultaneous creates', function(done) {
        var ops = [
          {v: 0, create: {type: 'json0', data: {x: 3}}},
          {v: 0, create: {type: 'json0', data: {x: 5}}}
        ];
        commitConcurrent(this.db, ops, testCreateCommit, done);
      });

      it('one commit succeeds from 5 simultaneous creates', function(done) {
        var ops = [
          {v: 0, create: {type: 'json0', data: {x: 3}}},
          {v: 0, create: {type: 'json0', data: {x: 5}}},
          {v: 0, create: {type: 'json0', data: {x: 7}}},
          {v: 0, create: {type: 'json0', data: {x: 9}}},
          {v: 0, create: {type: 'json0', data: {x: 11}}}
        ];
        commitConcurrent(this.db, ops, testCreateCommit, done);
      });

      function createDoc(db, callback) {
        var ops = [{v: 0, create: {type: 'json0', data: {x: 3}}}];
        commitConcurrent(db, ops, null, callback);
      }

      function testOpCommit(ops, snapshot) {
        expect(snapshot.v).equal(2);
        expect(ops.length).equal(2);
        expect(ops[0].create).ok;
        expect(ops[1].op).ok;
      }

      function testDelCommit(ops, snapshot) {
        expect(snapshot.v).equal(2);
        expect(ops.length).equal(2);
        expect(snapshot.data).equal(undefined);
        expect(snapshot.type).equal(null);
        expect(ops[0].create).ok;
        expect(ops[1].del).equal(true);
      }

      function testOpOrDelCommit(ops, snapshot) {
        if (ops[1].op) {
          testOpCommit(ops, snapshot);
        } else {
          testDelCommit(ops, snapshot);
        }
      }

      it('one commit succeeds from 2 simultaneous ops', function(done) {
        var ops = [
          {v: 1, op: []},
          {v: 1, op: []}
        ];
        var db = this.db;
        createDoc(db, function(err) {
          if (err) return done(err);
          commitConcurrent(db, ops, testOpCommit, done);
        });
      });

      it('one commit succeeds from 5 simultaneous ops', function(done) {
        var ops = [
          {v: 1, op: []},
          {v: 1, op: []},
          {v: 1, op: []},
          {v: 1, op: []},
          {v: 1, op: []}
        ];
        var db = this.db;
        createDoc(db, function(err) {
          if (err) return done(err);
          commitConcurrent(db, ops, testOpCommit, done);
        });
      });

      it('one commit succeeds from 2 simultaneous deletes', function(done) {
        var ops = [
          {v: 1, del: true},
          {v: 1, del: true}
        ];
        var db = this.db;
        createDoc(db, function(err) {
          if (err) return done(err);
          commitConcurrent(db, ops, testDelCommit, done);
        });
      });

      it('one commit succeeds from 5 simultaneous deletes', function(done) {
        var ops = [
          {v: 1, del: true},
          {v: 1, del: true},
          {v: 1, del: true},
          {v: 1, del: true},
          {v: 1, del: true}
        ];
        var db = this.db;
        createDoc(db, function(err) {
          if (err) return done(err);
          commitConcurrent(db, ops, testDelCommit, done);
        });
      });

      it('one commit succeeds from delete simultaneous with op', function(done) {
        var ops = [
          {v: 1, del: true},
          {v: 1, op: []}
        ];
        var db = this.db;
        createDoc(db, function(err) {
          if (err) return done(err);
          commitConcurrent(db, ops, testOpOrDelCommit, done);
        });
      });

      it('one commit succeeds from op simultaneous with delete', function(done) {
        var ops = [
          {v: 1, op: []},
          {v: 1, del: true}
        ];
        var db = this.db;
        createDoc(db, function(err) {
          if (err) return done(err);
          commitConcurrent(db, ops, testOpOrDelCommit, done);
        });
      });
    });

    function commitSnapshotWithMetadata(db, cb) {
      var data = {x: 5, y: 6};
      var metadata = {test: 3};
      var op = {v: 0, create: {type: 'http://sharejs.org/types/JSONv0', data: data}};
      var snapshot = {v: 1, type: 'http://sharejs.org/types/JSONv0', data: data, m: metadata};
      db.commit('testcollection', 'test', op, snapshot, null, cb);
    }

    describe('getSnapshot', function() {
      it('getSnapshot returns v0 snapshot', function(done) {
        this.db.getSnapshot('testcollection', 'test', null, null, function(err, result) {
          if (err) return done(err);
          expect(result).eql({id: 'test', type: null, v: 0, data: undefined, m: null});
          done();
        });
      });

      it('getSnapshot returns committed data', function(done) {
        var data = {x: 5, y: 6};
        var op = {v: 0, create: {type: 'json0', data: data}};
        var db = this.db;
        submit(db, 'testcollection', 'test', op, function(err) {
          if (err) return done(err);
          db.getSnapshot('testcollection', 'test', null, null, function(err, result) {
            if (err) return done(err);
            expect(result).eql({id: 'test', type: 'http://sharejs.org/types/JSONv0', v: 1, data: data, m: null});
            done();
          });
        });
      });

      it('getSnapshot does not return committed metadata by default', function(done) {
        var db = this.db;
        commitSnapshotWithMetadata(db, function(err) {
          if (err) return done(err);
          db.getSnapshot('testcollection', 'test', null, null, function(err, result) {
            if (err) return done(err);
            expect(result.m).equal(null);
            done();
          });
        });
      });

      it('getSnapshot returns metadata when option is true', function(done) {
        var db = this.db;
        commitSnapshotWithMetadata(db, function(err) {
          if (err) return done(err);
          db.getSnapshot('testcollection', 'test', null, {metadata: true}, function(err, result) {
            if (err) return done(err);
            expect(result.m).eql({test: 3});
            done();
          });
        });
      });

      it('getSnapshot returns metadata when fields is {$submit: true}', function(done) {
        var db = this.db;
        commitSnapshotWithMetadata(db, function(err) {
          if (err) return done(err);
          db.getSnapshot('testcollection', 'test', {$submit: true}, null, function(err, result) {
            if (err) return done(err);
            expect(result.m).eql({test: 3});
            done();
          });
        });
      });
    });

    describe('getSnapshotBulk', function() {
      it('getSnapshotBulk returns committed and v0 snapshots', function(done) {
        var data = {x: 5, y: 6};
        var op = {v: 0, create: {type: 'json0', data: data}};
        var db = this.db;
        submit(db, 'testcollection', 'test', op, function(err) {
          if (err) return done(err);
          db.getSnapshotBulk('testcollection', ['test2', 'test'], null, null, function(err, resultMap) {
            if (err) return done(err);
            expect(resultMap).eql({
              test: {id: 'test', type: 'http://sharejs.org/types/JSONv0', v: 1, data: data, m: null},
              test2: {id: 'test2', type: null, v: 0, data: undefined, m: null}
            });
            done();
          });
        });
      });

      it('getSnapshotBulk does not return committed metadata by default', function(done) {
        var db = this.db;
        commitSnapshotWithMetadata(db, function(err) {
          if (err) return done(err);
          db.getSnapshotBulk('testcollection', ['test2', 'test'], null, null, function(err, resultMap) {
            if (err) return done(err);
            expect(resultMap.test.m).equal(null);
            done();
          });
        });
      });

      it('getSnapshotBulk returns metadata when option is true', function(done) {
        var db = this.db;
        commitSnapshotWithMetadata(db, function(err) {
          if (err) return done(err);
          db.getSnapshotBulk('testcollection', ['test2', 'test'], null, {metadata: true}, function(err, resultMap) {
            if (err) return done(err);
            expect(resultMap.test.m).eql({test: 3});
            done();
          });
        });
      });
    });

    describe('getOps', function() {
      it('getOps returns empty list when there are no ops', function(done) {
        this.db.getOps('testcollection', 'test', 0, null, null, function(err, ops) {
          if (err) return done(err);
          expect(ops).eql([]);
          done();
        });
      });

      it('getOps returns 1 committed op', function(done) {
        var op = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}};
        var db = this.db;
        submit(db, 'testcollection', 'test', op, function(err) {
          if (err) return done(err);
          db.getOps('testcollection', 'test', 0, null, null, function(err, ops) {
            if (err) return done(err);
            expect(ops).eql([op]);
            done();
          });
        });
      });

      it('getOps returns 2 committed ops', function(done) {
        var op0 = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}};
        var op1 = {v: 1, op: [{p: ['x'], na: 1}]};
        var db = this.db;
        submit(db, 'testcollection', 'test', op0, function(err) {
          if (err) return done(err);
          submit(db, 'testcollection', 'test', op1, function(err) {
            if (err) return done(err);
            db.getOps('testcollection', 'test', 0, null, null, function(err, ops) {
              if (err) return done(err);
              expect(ops).eql([op0, op1]);
              done();
            });
          });
        });
      });

      it('getOps returns all ops when from and to are null', function(done) {
        var op0 = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}};
        var op1 = {v: 1, op: [{p: ['x'], na: 1}]};
        var db = this.db;
        submit(db, 'testcollection', 'test', op0, function(err) {
          if (err) return done(err);
          submit(db, 'testcollection', 'test', op1, function(err) {
            if (err) return done(err);
            db.getOps('testcollection', 'test', null, null, null, function(err, ops) {
              if (err) return done(err);
              expect(ops).eql([op0, op1]);
              done();
            });
          });
        });
      });

      it('getOps returns from specific op number', function(done) {
        var op0 = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}};
        var op1 = {v: 1, op: [{p: ['x'], na: 1}]};
        var db = this.db;
        submit(db, 'testcollection', 'test', op0, function(err) {
          if (err) return done(err);
          submit(db, 'testcollection', 'test', op1, function(err) {
            if (err) return done(err);
            db.getOps('testcollection', 'test', 1, null, null, function(err, ops) {
              if (err) return done(err);
              expect(ops).eql([op1]);
              done();
            });
          });
        });
      });

      it('getOps returns to specific op number', function(done) {
        var op0 = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}};
        var op1 = {v: 1, op: [{p: ['x'], na: 1}]};
        var db = this.db;
        submit(db, 'testcollection', 'test', op0, function(err) {
          if (err) return done(err);
          submit(db, 'testcollection', 'test', op1, function(err) {
            if (err) return done(err);
            db.getOps('testcollection', 'test', 0, 1, null, function(err, ops) {
              if (err) return done(err);
              expect(ops).eql([op0]);
              done();
            });
          });
        });
      });

      it('getOps does not return committed metadata by default', function(done) {
        var op = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}, m: {test: 3}};
        var db = this.db;
        submit(db, 'testcollection', 'test', op, function(err) {
          if (err) return done(err);
          db.getOps('testcollection', 'test', null, null, null, function(err, ops) {
            if (err) return done(err);
            expect(ops[0].m).eql(undefined);
            done();
          });
        });
      });

      it('getOps returns metadata when option is true', function(done) {
        var op = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}, m: {test: 3}};
        var db = this.db;
        submit(db, 'testcollection', 'test', op, function(err) {
          if (err) return done(err);
          db.getOps('testcollection', 'test', null, null, {metadata: true}, function(err, ops) {
            if (err) return done(err);
            expect(ops[0].m).eql({test: 3});
            done();
          });
        });
      });
    });

    describe('getOpsBulk', function() {
      it('getOpsBulk returns empty map when there are no ops', function(done) {
        this.db.getOpsBulk('testcollection', {test: 0, test2: 0}, null, null, function(err, opsMap) {
          if (err) return done(err);
          expect(opsMap).eql({
            test: [],
            test2: []
          });
          done();
        });
      });

      it('getOpsBulk returns committed ops', function(done) {
        var op = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}};
        var db = this.db;
        submit(db, 'testcollection', 'test', op, function(err) {
          if (err) return done(err);
          submit(db, 'testcollection', 'test2', op, function(err) {
            if (err) return done(err);
            db.getOpsBulk('testcollection', {test: 0, test2: 0}, null, null, function(err, opsMap) {
              if (err) return done(err);
              expect(opsMap).eql({
                test: [op],
                test2: [op]
              });
              done();
            });
          });
        });
      });

      it('getOpsBulk returns all ops committed from null', function(done) {
        var op = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}};
        var db = this.db;
        submit(db, 'testcollection', 'test', op, function(err) {
          if (err) return done(err);
          submit(db, 'testcollection', 'test2', op, function(err) {
            if (err) return done(err);
            db.getOpsBulk('testcollection', {test: null, test2: null}, null, null, function(err, opsMap) {
              if (err) return done(err);
              expect(opsMap).eql({
                test: [op],
                test2: [op]
              });
              done();
            });
          });
        });
      });

      it('getOpsBulk returns committed ops with specific froms', function(done) {
        var op0 = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}};
        var op1 = {v: 1, op: [{p: ['x'], na: 1}]};
        var db = this.db;
        submit(db, 'testcollection', 'test', op0, function(err) {
          if (err) return done(err);
          submit(db, 'testcollection', 'test2', op0, function(err) {
            if (err) return done(err);
            submit(db, 'testcollection', 'test', op1, function(err) {
              if (err) return done(err);
              submit(db, 'testcollection', 'test2', op1, function(err) {
                if (err) return done(err);
                db.getOpsBulk('testcollection', {test: 0, test2: 1}, null, null, function(err, opsMap) {
                  if (err) return done(err);
                  expect(opsMap).eql({
                    test: [op0, op1],
                    test2: [op1]
                  });
                  done();
                });
              });
            });
          });
        });
      });

      it('getOpsBulk returns committed ops with specific to', function(done) {
        var op0 = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}};
        var op1 = {v: 1, op: [{p: ['x'], na: 1}]};
        var db = this.db;
        submit(db, 'testcollection', 'test', op0, function(err) {
          if (err) return done(err);
          submit(db, 'testcollection', 'test2', op0, function(err) {
            if (err) return done(err);
            submit(db, 'testcollection', 'test', op1, function(err) {
              if (err) return done(err);
              submit(db, 'testcollection', 'test2', op1, function(err) {
                if (err) return done(err);
                db.getOpsBulk('testcollection', {test: 1, test2: 0}, {test2: 1}, null, function(err, opsMap) {
                  if (err) return done(err);
                  expect(opsMap).eql({
                    test: [op1],
                    test2: [op0]
                  });
                  done();
                });
              });
            });
          });
        });
      });

      it('getOpsBulk does not return committed metadata by default', function(done) {
        var op = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}, m: {test: 3}};
        var db = this.db;
        submit(db, 'testcollection', 'test', op, function(err) {
          if (err) return done(err);
          db.getOpsBulk('testcollection', {test: null}, null, null, function(err, opsMap) {
            if (err) return done(err);
            expect(opsMap.test[0].m).equal(undefined);
            done();
          });
        });
      });

      it('getOpsBulk returns metadata when option is true', function(done) {
        var op = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}, m: {test: 3}};
        var db = this.db;
        submit(db, 'testcollection', 'test', op, function(err) {
          if (err) return done(err);
          db.getOpsBulk('testcollection', {test: null}, null, {metadata: true}, function(err, opsMap) {
            if (err) return done(err);
            expect(opsMap.test[0].m).eql({test: 3});
            done();
          });
        });
      });
    });

    describe('getOpsToSnapshot', function() {
      it('getOpsToSnapshot returns committed op', function(done) {
        var op = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}};
        var db = this.db;
        submit(db, 'testcollection', 'test', op, function(err) {
          if (err) return done(err);
          db.getSnapshot('testcollection', 'test', {$submit: true}, null, function(err, snapshot) {
            if (err) return done(err);
            db.getOpsToSnapshot('testcollection', 'test', 0, snapshot, null, function(err, ops) {
              if (err) return done(err);
              expect(ops).eql([op]);
              done();
            });
          });
        });
      });

      it('getOpsToSnapshot does not return committed metadata by default', function(done) {
        var op = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}, m: {test: 3}};
        var db = this.db;
        submit(db, 'testcollection', 'test', op, function(err) {
          if (err) return done(err);
          db.getSnapshot('testcollection', 'test', {$submit: true}, null, function(err, snapshot) {
            if (err) return done(err);
            db.getOpsToSnapshot('testcollection', 'test', 0, snapshot, null, function(err, ops) {
              if (err) return done(err);
              expect(ops[0].m).equal(undefined);
              done();
            });
          });
        });
      });

      it('getOpsToSnapshot returns metadata when option is true', function(done) {
        var op = {v: 0, create: {type: 'json0', data: {x: 5, y: 6}}, m: {test: 3}};
        var db = this.db;
        submit(db, 'testcollection', 'test', op, function(err) {
          if (err) return done(err);
          db.getSnapshot('testcollection', 'test', {$submit: true}, null, function(err, snapshot) {
            if (err) return done(err);
            db.getOpsToSnapshot('testcollection', 'test', 0, snapshot, {metadata: true}, function(err, ops) {
              if (err) return done(err);
              expect(ops[0].m).eql({test: 3});
              done();
            });
          });
        });
      });
    });

    if (options.getQuery) {
      describe('query', function() {
        it('query returns data in the collection', function(done) {
          var snapshot = {v: 1, type: 'json0', data: {x: 5, y: 6}, m: null};
          var db = this.db;
          db.commit('testcollection', 'test', {v: 0, create: {}}, snapshot, null, function(err) {
            if (err) return done(err);
            db.query('testcollection', {x: 5}, null, null, function(err, results) {
              if (err) return done(err);
              delete results[0].id;
              expect(results).eql([snapshot]);
              done();
            });
          });
        });

        it('query returns nothing when there is no data', function(done) {
          this.db.query('testcollection', {x: 5}, null, null, function(err, results) {
            if (err) return done(err);
            expect(results).eql([]);
            done();
          });
        });

        it('query does not return committed metadata by default', function(done) {
          var db = this.db;
          commitSnapshotWithMetadata(db, function(err) {
            if (err) return done(err);
            db.query('testcollection', {x: 5}, null, null, function(err, results) {
              if (err) return done(err);
              expect(results[0].m).equal(null);
              done();
            });
          });
        });

        it('query returns metadata when option is true', function(done) {
          var db = this.db;
          commitSnapshotWithMetadata(db, function(err) {
            if (err) return done(err);
            db.query('testcollection', {x: 5}, null, {metadata: true}, function(err, results) {
              if (err) return done(err);
              expect(results[0].m).eql({test: 3});
              done();
            });
          });
        });
      });

      describe('projections', function() {
        it('query returns only projected fields', function(done) {
          if (!this.db.projectsSnapshot) return done();

          var snapshot = {type: 'json0', v: 1, data: {x: 5, y: 6}};
          var db = this.db;
          db.commit('testcollection', 'test', {v: 0, create: {}}, snapshot, null, function(err) {
            if (err) return done(err);
            db.query('testcollection', {x: 5}, {y: true}, null, function(err, results) {
              if (err) return done(err);
              expect(results).eql([{type: 'json0', v: 1, data: {y: 6}, id: 'test'}]);
              done();
            });
          });
        });

        it('query returns no data for matching documents if fields is empty', function(done) {
          if (!this.db.projectsSnapshot) return done();

          var snapshot = {type: 'json0', v: 1, data: {x: 5, y: 6}};
          var db = this.db;
          db.commit('testcollection', 'test', {v: 0, create: {}}, snapshot, null, function(err) {
            if (err) return done(err);
            db.query('testcollection', {x: 5}, {}, null, function(err, results) {
              if (err) return done(err);
              expect(results).eql([{type: 'json0', v: 1, data: {}, id: 'test'}]);
              done();
            });
          });
        });

        it('query does not return committed metadata by default with projection', function(done) {
          var db = this.db;
          commitSnapshotWithMetadata(db, function(err) {
            if (err) return done(err);
            db.query('testcollection', {x: 5}, {x: true}, null, function(err, results) {
              if (err) return done(err);
              expect(results[0].m).equal(null);
              done();
            });
          });
        });

        it('query returns metadata when option is true with projection', function(done) {
          var db = this.db;
          commitSnapshotWithMetadata(db, function(err) {
            if (err) return done(err);
            db.query('testcollection', {x: 5}, {x: true}, {metadata: true}, function(err, results) {
              if (err) return done(err);
              expect(results[0].m).eql({test: 3});
              done();
            });
          });
        });
      });

      describe('queryPoll', function() {
        it('returns data in the collection', function(done) {
          var snapshot = {v: 1, type: 'json0', data: {x: 5, y: 6}};
          var db = this.db;
          db.commit('testcollection', 'test', {v: 0, create: {}}, snapshot, null, function(err) {
            if (err) return done(err);
            var dbQuery = options.getQuery({query: {x: 5}});
            db.queryPoll('testcollection', dbQuery, null, function(err, ids) {
              if (err) return done(err);
              expect(ids).eql(['test']);
              done();
            });
          });
        });

        it('returns nothing when there is no data', function(done) {
          var dbQuery = options.getQuery({query: {x: 5}});
          this.db.queryPoll('testcollection', dbQuery, null, function(err, ids) {
            if (err) return done(err);
            expect(ids).eql([]);
            done();
          });
        });
      });

      describe('queryPollDoc', function() {
        it('returns false when the document does not exist', function(done) {
          var dbQuery = options.getQuery({query: {}});
          if (!this.db.canPollDoc('testcollection', dbQuery)) return done();

          var db = this.db;
          db.queryPollDoc('testcollection', 'doesnotexist', dbQuery, null, function(err, result) {
            if (err) return done(err);
            expect(result).equal(false);
            done();
          });
        });

        it('returns true when the document matches', function(done) {
          var dbQuery = options.getQuery({query: {x: 5}});
          if (!this.db.canPollDoc('testcollection', dbQuery)) return done();

          var snapshot = {type: 'json0', v: 1, data: {x: 5, y: 6}};
          var db = this.db;
          db.commit('testcollection', 'test', {v: 0, create: {}}, snapshot, null, function(err) {
            if (err) return done(err);
            db.queryPollDoc('testcollection', 'test', dbQuery, null, function(err, result) {
              if (err) return done(err);
              expect(result).equal(true);
              done();
            });
          });
        });

        it('returns false when the document does not match', function(done) {
          var dbQuery = options.getQuery({query: {x: 6}});
          if (!this.db.canPollDoc('testcollection', dbQuery)) return done();

          var snapshot = {type: 'json0', v: 1, data: {x: 5, y: 6}};
          var db = this.db;
          db.commit('testcollection', 'test', {v: 0, create: {}}, snapshot, null, function(err) {
            if (err) return done(err);
            db.queryPollDoc('testcollection', 'test', dbQuery, null, function(err, result) {
              if (err) return done(err);
              expect(result).equal(false);
              done();
            });
          });
        });
      });

      describe('getQuery', function() {
        it('getQuery argument order', function(done) {
          // test that options.getQuery({query: {}, sort: [['foo', 1], ['bar', -1]]})
          // sorts by foo first, then bar
          var snapshots = [
            {type: 'json0', id: '0', v: 1, data: {foo: 1, bar: 1}, m: null},
            {type: 'json0', id: '1', v: 1, data: {foo: 2, bar: 1}, m: null},
            {type: 'json0', id: '2', v: 1, data: {foo: 1, bar: 2}, m: null},
            {type: 'json0', id: '3', v: 1, data: {foo: 2, bar: 2}, m: null}
          ];
          var db = this.db;
          var dbQuery = options.getQuery({query: {}, sort: [['foo', 1], ['bar', -1]]});

          async.each(snapshots, function(snapshot, cb) {
            db.commit('testcollection', snapshot.id, {v: 0, create: {}}, snapshot, null, cb);
          }, function(err) {
            if (err) throw err;
            db.query('testcollection', dbQuery, null, null, function(err, results) {
              if (err) throw err;
              expect(results).eql(
                [snapshots[2], snapshots[0], snapshots[3], snapshots[1]]);
              done();
            });
          });
        });
      });
    }
  });
};
