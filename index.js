var leveldb = require('leveldb')
var JSONStream = require('JSONStream')
var async = require('async')
var microtime = require('microtime')
var crypto = require('crypto')
var uuid = require('node-uuid')

function PlumbDB(name, cb) {
  var me = this
  // make sure changes prefix sorts later than doc prefix
  me.docPrefix = "@"
  me.stampPrefix = "\u9999"
  leveldb.open(name + ".leveldb", { create_if_missing: true }, loaded)
  function loaded(err, db) {
    if (db) me.db = db
    cb(err, db)
  }
}

module.exports = function(name, cb) {
  return new PlumbDB(name, cb)
}

module.exports.PlumbDB = PlumbDB

PlumbDB.prototype.get = function(id, cb) {
  var me = this
  me.db.get(me.docPrefix + id, function (err, stamp) {
    if (err) return cb(err)
    if (!stamp) return cb(false, null)
    me.db.get(me.stampPrefix + stamp, function (err, data) {
      if (err) return cb(err)
      cb(false, JSON.parse(data))
    })
  })
}

PlumbDB.prototype.put = function(readStream, cb) {
  var me = this
  var data = []
  readStream.on('data', function(chunk) { data.push(chunk) })
  readStream.on('end', function() {
    var json = JSON.parse(data.join(''))
    me._store(json, cb)
  })
  readStream.on('error', function(err) { cb(err) })
}

// hack until node-leveldb gets streams
PlumbDB.prototype._getLast = function(cb) {
  this.db.iterator(function(err, iterator) {
    if (err) return cb(err)
    iterator.last(function(err) {
      if (err) return cb(err)
      iterator.current(function(err, key, val) {
        cb(err, key)
      })
    })
  })
}

PlumbDB.prototype.bulk = function(readStream, cb) {
  var me = this
  var parser = JSONStream.parse(['docs', /./])
  var results = []
  var doneParsing = false
  var error = false
  
  var q = async.queue(function (doc, cb) {
    me._store(doc, function(err) {
      if (err) return cb(err)
      results.push(doc)
      cb(false)
    })
  }, 1)
  
  q.drain = function() { if (doneParsing && !error) cb(false, results) }
  readStream.pipe(parser)
  parser.on('data', q.push)
  parser.on('error', function(err) {
    error = true
    return cb(err)
  })
  parser.on('end', function() {
    doneParsing = true
    if (q.length() === 0 && !error) cb(false, results)
  })
}

PlumbDB.prototype._incrementRev = function(json) {
  var rev = 0
  if (json._rev) rev = json._rev.split('-')[0]
  return ++rev + '-' + this._hash(json)
}

PlumbDB.prototype._hash = function(json) {
  return crypto.createHash('md5').update(JSON.stringify(json)).digest("hex")
}

PlumbDB.prototype._dumpAll = function() {
  this.db.iterator(function(err, iterator) {
    iterator.forRange(function(err, key, val) {
      console.log(err, key, val)
    })
  })
}

PlumbDB.prototype._store = function(json, cb) {
  console.log('storing', json)
  var me = this
  json._stamp = microtime.now() + ""
  if (!json._id) json._id = uuid.v4()
  function save(afterPut) {
    json._rev = me._incrementRev(json)
    // todo break out into easy batch function
    var batch = me.db.batch()
    batch.put(me.stampPrefix + json._stamp, JSON.stringify(json))
    batch.put(me.docPrefix + json._id, json._stamp)
    if (afterPut) afterPut(batch)
    batch.write(done)
  }
  function done(err) { cb(err, json) }
  
  me.get(json._id, function(err, stored) {
    // todo decide how to handle err
    if (!stored) return save()
    if (stored._rev !== json._rev) return done({conflict: true})
    return save(function afterPut(batch) {
      batch.del(me.stampPrefix + stored._stamp)
    })
  })

}