var assert = require('assert');
var text = require('ot-text').type;
var ot = require('../lib/ot');

describe('ot', function() {
  before(function() {
    var before = Date.now();
    var after = before + 10 * 1000;

    function checkMetaTs(field) {
      return function(data) {
        assert.ok(data.m);
        assert.ok(before <= data.m[field] && data.m[field] < after);
        delete data.m[field];
        data;
      };
    };

    this.checkOpTs = checkMetaTs('ts');
    this.checkDocCreate = checkMetaTs('ctime');
    this.checkDocModified = checkMetaTs('mtime');

    var _this = this;
    this.checkDocTs = function(doc) {
      _this.checkDocCreate(doc);
      _this.checkDocModified(doc);
      doc;
    };
  });

  describe('checkOpData', function() {
    it('fails if opdata is not an object', function() {
      assert.ok(ot.checkOpData('hi'));
      assert.ok(ot.checkOpData());
      assert.ok(ot.checkOpData(123));
      assert.ok(ot.checkOpData([]));
    });

    it('fails if op data is missing op, create and del', function() {
      assert.ok(ot.checkOpData({
        v: 5
      }));
    });

    it('fails if src/seq data is invalid', function() {
      assert.ok(ot.checkOpData({
        del: true,
        v: 5,
        src: 'hi'
      }));

      assert.ok(ot.checkOpData({
        del: true,
        v: 5,
        seq: 123
      }));

      assert.ok(ot.checkOpData({
        del: true,
        v: 5,
        src: 'hi',
        seq: 'there'
      }));
    });

    it('fails if a create operation is missing its type', function() {
      assert.ok(ot.checkOpData({
        create: {}
      }));

      assert.ok(ot.checkOpData({
        create: 123
      }));
    });

    it('fails if the type is missing', function() {
      assert.ok(ot.checkOpData({
        create: {
          type: "something that does not exist"
        }
      }));
    });

    it('accepts valid create operations', function() {
      assert.equal(null, ot.checkOpData({
        create: {
          type: text.uri
        }
      }));

      assert.equal(null, ot.checkOpData({
        create: {
          type: text.uri,
          data: 'hi there'
        }
      }));
    });

    it('accepts valid delete operations', function() {
      assert.equal(null, ot.checkOpData({
        del: true
      }));
    });

    it('accepts valid ops', function() {
      assert.equal(null, ot.checkOpData({
        op: [1, 2, 3]
      }));
    });
  });

  describe('normalize', function() {
    it('expands type names in normalizeType', function() {
      assert.equal(text.uri, ot.normalizeType('text'));
    });

    it('expands type names in an op', function() {
      var opData = {
        create: {
          type: 'text'
        }
      };

      ot.normalize(opData);
      this.checkOpTs(opData);

      assert.deepEqual(opData, {
        create: {
          type: text.uri
        },
        m: {},
        src: ''
      });
    });
  });

  describe('apply', function() {
    it('fails if the versions dont match', function() {
      assert.equal('Version mismatch', ot.apply({
        v: 5
      }, {
        v: 6,
        create: {
          type: text.uri
        }
      }));

      assert.equal('Version mismatch', ot.apply({
        v: 5
      }, {
        v: 6,
        del: true
      }));

      assert.equal('Version mismatch', ot.apply({
        v: 5
      }, {
        v: 6,
        op: []
      }));
    });

    it('allows the version field to be missing', function() {
      assert.equal(null, ot.apply({
        v: 5
      }, {
        create: {
          type: text.uri
        }
      }));

      assert.equal(null, ot.apply({}, {
        v: 6,
        create: {
          type: text.uri
        }
      }));
    });

    describe('create', function() {
      it('fails if the document already exists', function() {
        var doc = {
          v: 6,
          create: {
            type: text.uri
          }
        };

        assert.equal('Document already exists', ot.apply({
          v: 6,
          type: text.uri,
          data: 'hi'
        }, doc));

        assert.deepEqual(doc, {
          v: 6,
          create: {
            type: text.uri
          }
        });
      });

      it('creates doc data correctly when no initial data is passed', function() {
        var doc = {
          v: 5
        };

        assert.equal(null, ot.apply(doc, {
          v: 5,
          create: {
            type: text.uri
          }
        }));

        this.checkDocTs(doc);
        assert.deepEqual(doc, {
          v: 6,
          type: text.uri,
          m: {},
          data: ''
        });
      });

      it('creates doc data when it is given initial data', function() {
        var doc = {
          v: 5
        };

        assert.equal(null, ot.apply(doc, {
          v: 5,
          create: {
            type: text.uri,
            data: 'Hi there'
          }
        }));

        this.checkDocTs(doc);
        assert.deepEqual(doc, {
          v: 6,
          type: text.uri,
          m: {},
          data: 'Hi there'
        });
      });

      // TODO Not implemented.
      it.skip('runs pre and post validation functions');
    });

    describe('del', function() {
      it('deletes the document data', function() {
        var doc = {
          v: 6,
          type: text.uri,
          data: 'Hi there'
        };

        assert.equal(null, ot.apply(doc, {
          v: 6,
          del: true
        }));

        delete doc.m.mtime;
        assert.deepEqual(doc, {
          v: 7,
          m: {}
        });
      });

      it('still works if the document doesnt exist anyway', function() {
        var doc = {
          v: 6
        };

        assert.equal(null, ot.apply(doc, {
          v: 6,
          del: true
        }));

        delete doc.m.mtime;
        assert.deepEqual(doc, {
          v: 7,
          m: {}
        });
      });

      it('keeps any metadata from op on the doc', function() {
        var doc = {
          v: 6,
          type: text.uri,
          m: {
            ctime: 1,
            mtime: 2
          },
          data: 'hi'
        };

        assert.equal(null, ot.apply(doc, {
          v: 6,
          del: true
        }));

        delete doc.m.mtime;
        assert.deepEqual(doc, {
          v: 7,
          m: {
            ctime: 1
          }
        });
      });
    });

    describe('op', function() {
      it('fails if the document does not exist', function() {
        assert.equal('Document does not exist', ot.apply({
          v: 6
        }, {
          v: 6,
          op: [1, 2, 3]
        }));
      });

      it('fails if the type is missing', function() {
        assert.equal('Type not found', ot.apply({
          v: 6,
          type: 'some non existant type'
        }, {
          v: 6,
          op: [1, 2, 3]
        }));
      });

      it('applies the operation to the document data', function() {
        var doc = {
          v: 6,
          type: text.uri,
          data: 'Hi'
        };

        assert.equal(null, ot.apply(doc, {
          v: 6,
          op: [2, ' there']
        }));

        this.checkDocModified(doc);
        assert.deepEqual(doc, {
          v: 7,
          type: text.uri,
          m: {},
          data: 'Hi there'
        });
      });

      it('updates mtime', function() {
        var doc = {
          v: 6,
          type: text.uri,
          m: {
            ctime: 1,
            mtime: 2
          },
          data: 'Hi'
        };

        assert.equal(null, ot.apply(doc, {
          v: 6,
          op: [2, ' there']
        }));

        this.checkDocModified(doc);
        assert.deepEqual(doc, {
          v: 7,
          type: text.uri,
          m: {
            ctime: 1
          },
          data: 'Hi there'
        });
      });

      // TODO Not implemented.
      it.skip('shatters the operation if it can, and applies it incrementally');
    });

    describe('noop', function() {
      it('works on existing docs', function() {
        var doc = {
          v: 6,
          type: text.uri,
          m: {
            ctime: 1,
            mtime: 2
          },
          data: 'Hi'
        };

        assert.equal(null, ot.apply(doc, {
          v: 6
        }));

        assert.deepEqual(doc, {
          v: 7,
          type: text.uri,
          m: {
            ctime: 1,
            mtime: 2
          },
          data: 'Hi'
        });
      });

      it('works on nonexistant docs', function() {
        var doc = {
          v: 0
        };

        assert.equal(null, ot.apply(doc, {
          v: 0
        }));

        assert.deepEqual(doc, {
          v: 1
        });
      });
    });
  });

  describe('transform', function() {
    it('fails if the version is specified on both and does not match', function() {
      var op1 = {
        v: 5,
        op: [10, 'hi']
      };
      var op2 = {
        v: 6,
        op: [5, 'abcde']
      };

      assert.equal('Version mismatch', ot.transform(text.uri, op1, op2));

      assert.deepEqual(op1, {
        v: 5,
        op: [10, 'hi']
      });
    });

    it('create by create fails', function() {
      assert.equal('Document created remotely', ot.transform(null, {
        v: 10,
        create: {
          type: text.uri
        }
      }, {
        v: 10,
        create: {
          type: text.uri
        }
      }));
    });

    it('create by delete fails', function() {
      assert.ok(ot.transform(null, {
        create: {
          type: text.uri
        }
      }, {
        del: true
      }));
    });

    it('create by op fails', function() {
      assert.equal('Document created remotely', ot.transform(null, {
        v: 10,
        create: {
          type: text.uri
        }
      }, {
        v: 10,
        op: [15, 'hi']
      }));
    });

    it('create by noop ok', function() {
      var op = {
        create: {
          type: text.uri
        },
        v: 6
      };

      assert.equal(null, ot.transform(null, op, {
        v: 6
      }));

      assert.deepEqual(op, {
        create: {
          type: text.uri
        },
        v: 7
      });
    });

    it('delete by create fails', function() {
      assert.ok(ot.transform(null, {
        del: true
      }, {
        create: {
          type: text.uri
        }
      }));
    });

    it('delete by delete ok', function() {
      var op = {
        del: true,
        v: 6
      };

      assert.equal(null, ot.transform(text.uri, op, {
        del: true,
        v: 6
      }));

      assert.deepEqual(op, {
        del: true,
        v: 7
      });

      op = {
        del: true
      };

      assert.equal(null, ot.transform(text.uri, op, {
        del: true,
        v: 6
      }));

      assert.deepEqual(op, {
        del: true
      });
    });

    it('delete by op ok', function() {
      var op = {
        del: true,
        v: 8
      };

      assert.equal(null, ot.transform(text.uri, op, {
        op: [],
        v: 8
      }));

      assert.deepEqual(op, {
        del: true,
        v: 9
      });

      op = {
        del: true
      };

      assert.equal(null, ot.transform(text.uri, op, {
        op: [],
        v: 8
      }));

      assert.deepEqual(op, {
        del: true
      });
    });

    it('delete by noop ok', function() {
      var op = {
        del: true,
        v: 6
      };

      assert.equal(null, ot.transform(null, op, {
        v: 6
      }));

      assert.deepEqual(op, {
        del: true,
        v: 7
      });

      op = {
        del: true
      };

      assert.equal(null, ot.transform(null, op, {
        v: 6
      }));

      assert.deepEqual(op, {
        del: true
      });
    });

    it('op by create fails', function() {
      assert.ok(ot.transform(null, {
        op: {}
      }, {
        create: {
          type: text.uri
        }
      }));
    });

    it('op by delete fails', function() {
      assert.equal('Document was deleted', ot.transform(text.uri, {
        v: 10,
        op: []
      }, {
        v: 10,
        del: true
      }));
    });

    it('op by op ok', function() {
      var op1 = {
        v: 6,
        op: [10, 'hi']
      };

      var op2 = {
        v: 6,
        op: [5, 'abcde']
      };

      assert.equal(null, ot.transform(text.uri, op1, op2));

      assert.deepEqual(op1, {
        v: 7,
        op: [15, 'hi']
      });

      op1 = {
        op: [10, 'hi']
      };

      op2 = {
        v: 6,
        op: [5, 'abcde']
      };

      assert.equal(null, ot.transform(text.uri, op1, op2));

      assert.deepEqual(op1, {
        op: [15, 'hi']
      });
    });

    it('op by noop ok', function() {
      var op = {
        v: 6,
        op: [10, 'hi']
      };

      assert.equal(null, ot.transform(text.uri, op, {
        v: 6
      }));

      assert.deepEqual(op, {
        v: 7,
        op: [10, 'hi']
      });
    });

    it('noop by anything is ok', function() {
      var op = {};

      assert.equal(null, ot.transform(text.uri, op, {
        v: 6,
        op: [10, 'hi']
      }));

      assert.deepEqual(op, {});

      assert.equal(null, ot.transform(text.uri, op, {
        del: true
      }));

      assert.deepEqual(op, {});

      assert.equal(null, ot.transform(null, op, {
        create: {
          type: text.uri
        }
      }));

      assert.deepEqual(op, {});

      assert.equal(null, ot.transform(null, op, {}));

      assert.deepEqual(op, {});
    });
  });

  describe('applyPresence', function() {
    it('sets', function() {
      var p = {
        data: {}
      };

      assert.equal(null, ot.applyPresence(p, {
        val: {
          id: {
            y: 6
          }
        }
      }));

      assert.deepEqual(p, {
        data: {
          id: {
            y: 6
          }
        }
      });

      assert.equal(null, ot.applyPresence(p, {
        p: ['id'],
        val: {
          z: 7
        }
      }));

      assert.deepEqual(p, {
        data: {
          id: {
            z: 7
          }
        }
      });

      assert.equal(null, ot.applyPresence(p, {
        p: ['id', 'z'],
        val: 8
      }));

      assert.deepEqual(p, {
        data: {
          id: {
            z: 8
          }
        }
      });
    });

    it('clears data', function() {
      var p = {
        data: {
          id: {
            name: 'sam'
          }
        }
      };

      assert.equal(null, ot.applyPresence(p, {
        val: null
      }));

      assert.deepEqual(p, {
        data: {}
      });
    });

    it("doesn't allow special keys other than _cursor", function() {
      var p = {};

      assert.equal('Cannot set reserved value', ot.applyPresence(p, {
        p: ['id'],
        val: {
          _x: 'hi'
        }
      }));

      assert.deepEqual(p, {});

      assert.equal('Cannot set reserved value', ot.applyPresence(p, {
        p: ['id', '_x'],
        val: 'hi'
      }));

      assert.deepEqual(p, {});
    });
  });

  describe('transformPresence', function() {
    it('updates cursor positions', function() {});
  });
});