var async = require('async');
var expect = require('chai').expect;

var idCounter = 0;

module.exports = function() {
  describe.only('transaction', function() {
    var backend;
    var connection;

    beforeEach(function() {
      backend = this.backend;
      connection = backend.connect();
    });

    describe('single Doc', function() {
      var id;
      var doc;
      var remoteDoc;
      var transaction;

      beforeEach(function() {
        id = (idCounter++).toString();
        doc = connection.get('dogs', id);
        remoteDoc = backend.connect().get('dogs', id);
        transaction = connection.startTransaction();

        // TODO: Discuss if this is an acceptable API? Doc will always emit error on
        // a failed transaction, since the ops may have been successfully acked for this Doc, and
        // we force a hard rollback with no callback, which causes an 'error' event
        doc.on('error', function() {});
      });

      it('commits two ops as a transaction', function(done) {
        async.series([
          doc.create.bind(doc, {name: 'Gaspode'}),
          doc.submitOp.bind(doc, [{p: ['age'], oi: 3}], {transaction: transaction}),
          doc.submitOp.bind(doc, [{p: ['tricks'], oi: ['fetch']}], {transaction: transaction}),
          remoteDoc.fetch.bind(remoteDoc),
          function(next) {
            expect(remoteDoc.data).to.eql({name: 'Gaspode'});
            next();
          },
          transaction.commit.bind(transaction),
          remoteDoc.fetch.bind(remoteDoc),
          function(next) {
            expect(remoteDoc.data).to.eql({
              name: 'Gaspode',
              age: 3,
              tricks: ['fetch']
            });
            expect(doc.data).to.eql(remoteDoc.data);
            next();
          }
        ], done);
      });

      it('does not commit the first op if the second op fails', function(done) {
        backend.use('commit', function(request, next) {
          if (!request.snapshot.data.tricks) return next();
          next(new Error('fail'));
        });

        async.series([
          doc.create.bind(doc, {name: 'Gaspode'}),
          doc.submitOp.bind(doc, [{p: ['age'], oi: 3}], {transaction: transaction}),
          function(next) {
            doc.submitOp([{p: ['tricks'], oi: ['fetch']}], {transaction: transaction}, function(error) {
              expect(error.message).to.equal('fail');
            });
            doc.once('load', next);
          },
          remoteDoc.fetch.bind(remoteDoc),
          function(next) {
            expect(remoteDoc.data).to.eql({name: 'Gaspode'});
            expect(doc.data).to.eql(remoteDoc.data);
            next();
          }
        ], done);
      });

      it('deletes and creates as part of a transaction', function(done) {
        async.series([
          doc.create.bind(doc, {name: 'Gaspode'}),
          doc.del.bind(doc, {transaction: transaction}),
          doc.create.bind(doc, {name: 'Recreated'}, 'json0', {transaction: transaction}),
          remoteDoc.fetch.bind(remoteDoc),
          function (next) {
            expect(remoteDoc.data).to.eql({name: 'Gaspode'});
            next();
          },
          transaction.commit.bind(transaction),
          remoteDoc.fetch.bind(remoteDoc),
          function(next) {
            expect(remoteDoc.data).to.eql({name: 'Recreated'});
            expect(doc.data).to.eql(remoteDoc.data);
            next();
          }
        ], done);
      });

      it('does not delete if the following create fails', function(done) {
        async.series([
          doc.create.bind(doc, {name: 'Gaspode'}),
          function(next) {
            backend.use('commit', function(request, next) {
              var error = request.op.create ? new Error('Create not allowed') : null;
              next(error);
            });
            next();
          },
          doc.del.bind(doc, {transaction: transaction}),
          function(next) {
            doc.create({name: 'Recreated'}, 'json0', {transaction: transaction}, function(error) {
              expect(error.message).to.equal('Create not allowed');
            });
            doc.once('load', next);
          },
          remoteDoc.fetch.bind(remoteDoc),
          function(next) {
            expect(remoteDoc.data).to.eql({name: 'Gaspode'});
            expect(doc.data).to.eql(remoteDoc.data);
            next();
          }
        ], done);
      });

      it('transaction is behind remote', function(done) {
        async.series([
          doc.create.bind(doc, {tricks: ['fetch']}),
          remoteDoc.fetch.bind(remoteDoc),
          remoteDoc.submitOp.bind(remoteDoc, [{p: ['tricks', 0], ld: 'fetch'}]),
          doc.submitOp.bind(doc, [{p: ['tricks', 1], li: 'sit'}], {transaction: transaction}),
          doc.submitOp.bind(doc, [{p: ['tricks', 2], li: 'shake'}], {transaction: transaction}),
          remoteDoc.fetch.bind(remoteDoc),
          function(next) {
            expect(remoteDoc.data).to.eql({tricks: []});
            next();
          },
          transaction.commit.bind(transaction),
          remoteDoc.fetch.bind(remoteDoc),
          function(next) {
            expect(remoteDoc.data).to.eql({tricks: ['sit', 'shake']});
            expect(doc.data).to.eql(remoteDoc.data);
            next();
          }
        ], done);
      });

      it('remote submits after but commits first', function(done) {
        async.series([
          doc.create.bind(doc, {tricks: ['fetch']}),
          remoteDoc.fetch.bind(remoteDoc),
          doc.submitOp.bind(doc, [{p: ['tricks', 1], li: 'sit'}], {transaction: transaction}),
          remoteDoc.submitOp.bind(remoteDoc, [{p: ['tricks', 0], ld: 'fetch'}]),
          doc.submitOp.bind(doc, [{p: ['tricks', 2], li: 'shake'}], {transaction: transaction}),
          remoteDoc.fetch.bind(remoteDoc),
          function(next) {
            expect(remoteDoc.data).to.eql({tricks: []});
            next();
          },
          transaction.commit.bind(transaction),
          remoteDoc.fetch.bind(remoteDoc),
          function(next) {
            expect(remoteDoc.data).to.eql({tricks: ['sit', 'shake']});
            expect(doc.data).to.eql(remoteDoc.data);
            next();
          }
        ], done);
      });
    });

    describe('multiple Docs', function() {
      it('rolls back multiple Docs if one commit fails', function(done) {
        var id1 = (idCounter++).toString();
        var id2 = (idCounter++).toString();
        var doc1 = connection.get('dogs', id1);
        var doc2 = connection.get('dogs', id2);
        var remoteDoc1 = backend.connect().get('dogs', id1);
        var remoteDoc2 = backend.connect().get('dogs', id2);

        var transaction = connection.startTransaction();

        // Doc1 will throw even though its op is accepted, since the
        // whole transaction is rejected
        doc1.on('error', function() {});
        doc2.on('error', function() {});

        async.series([
          doc1.create.bind(doc1, {name: 'Gaspode'}),
          doc2.create.bind(doc2, {name: 'Snoopy'}),
          function(next) {
            backend.use('commit', function(request, next) {
              var error = request.id === id2 ? new Error('fail') : null;
              next(error);
            });
            next();
          },
          doc1.submitOp.bind(doc1, [{p: ['age'], oi: 3}], {transaction: transaction}),
          function(next) {
            doc2.submitOp([{p: ['age'], oi: 4}], {transaction: transaction}, function(error) {
              expect(error.message).to.equal('fail');
            });
            doc2.once('load', next);
          },
          remoteDoc1.fetch.bind(remoteDoc1),
          remoteDoc2.fetch.bind(remoteDoc2),
          function(next) {
            expect(remoteDoc1.data).to.eql({name: 'Gaspode'});
            expect(remoteDoc2.data).to.eql({name: 'Snoopy'});
            expect(doc1.data).to.eql(remoteDoc1.data);
            expect(doc2.data).to.eql(remoteDoc2.data);
            next();
          }
        ], done);
      });
    });
  });
}
