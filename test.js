var it = require('it-is')
var plumbdb = require('./index')
var async = require('async')
var _ = require('underscore')

function setup(cb) {
  var instance = plumbdb('test', function(err, db) {
    cb(err, instance)
  })
}

function teardown(instance, cb) {
  instance.destroy(cb)
}

function assert(test) {
  return function(cb) {
    setup(function(err, instance) {
      function done(result) {
        teardown(instance, function(destroyErr) {
          if (destroyErr) console.log("Destroy Error", destroyErr)
          cb(false, result)
        })
      }
      try {
        test(instance, done)
      } catch(e) {
        done(e)
      }
    })
  }
}

var tests = {
  _store: assert(function(p, cb) {
    p._store({'hello': 'world'}, function(err, doc) {
      cb(!doc._rev && !doc._stamp && !doc._id)
    })
  }),
  update: assert(function(p, cb) {
    p._store({'hello': 'world'}, function(err, doc) {
      doc.foo = "bar"
      p._store(doc, function(err, edited) {
        p.get(doc._id, function(err, retrieved) {
          cb(JSON.stringify(edited) !== JSON.stringify(retrieved))
        })
      })
    })
  }),
  get: assert(function(p, cb) {
    p._store({'hello': 'get'}, function(err, doc) {
      p.get(doc._id, function(err, stored) {
        cb(JSON.stringify(doc) !== JSON.stringify(stored))
      })
    })
  }),
  updateRev: assert(function(p, cb) {
    p._store({'hello': 'update'}, function(err, doc) {
      doc.foo = "bar"
      p._store(doc, function(err, updated) {
        cb(!(updated._rev > doc._rev))
      })
    })
  }),
  existingRev: assert(function(p, cb) {
    var doc = {
      hello: 'update',
      foo: 'bar',
      _id: 'ea7eb9fb-998f-4210-ab1b-8463033913c4',
      _stamp: '1339875060007344'
    }
    doc._rev = '2-' + p._hash(doc)
    p._store(doc, function(err, stored) {
      cb(doc._rev !== stored._rev)
    })
  }),
  keyStream: assert(function(p, cb) {
    p._store({'hello': 'world'}, function(err, doc) {
      p._store({'goodbye': 'world'}, function(err, doc) {
        var query = []
        var ok = true
        p.keyStream('h')
          .on('data', function(doc) { query.push(doc) })
          .on('error', function(err) { ok = false })
          .on('end', function() {
            cb(!ok && JSON.stringify(query) !== JSON.stringify([{'hello': 'world'}]))
          })
      })
    })
    
  })
}

console.log('running tests...')
async.series(tests, function(err, results) {
  _.each(results, function(result, test) {
    console.log(test, result ? 'FAIL ' + result : 'PASS')
  })
})