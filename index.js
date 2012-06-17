var stream = require('stream')
var util = require('util')
var leveldb = require('leveldb')
var JSONStream = require('JSONStream')
var async = require('async')
var microtime = require('microtime')
var crypto = require('crypto')
var uuid = require('node-uuid')

function PlumbDB(name, cb) {
  var me = this
  me.name = name
  // make sure stamp prefix sorts later than doc prefix
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

PlumbDB.prototype.destroy = function(cb) {
  leveldb.destroy(this.name + '.leveldb', cb)
}

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
    me._store(doc, function(err, stored) {
      if (err) return cb(err)
      results.push(stored)
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

PlumbDB.prototype._computeRev = function(json) {
  json = this._cloneObj(json)
  var rev = json._rev
  var version = 0
  var oldHash = false
  if (json._rev) {
    version = json._rev.split('-')[0]
    oldHash = json._rev.split('-')[1]
  }
  delete json._rev
  var newHash = this._hash(json)
  if (newHash === oldHash) return rev
  return ++version + '-' + this._hash(json)
}

PlumbDB.prototype._updateMetadata = function(json) {
  if (!json._rev) json._stamp = microtime.now() + ""
  json._rev = this._computeRev(json)
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

PlumbDB.prototype._cloneObj = function(json) {
  return JSON.parse(JSON.stringify(json))
}

PlumbDB.prototype._advanceKeySeek = function(key, iterator, readStream) {
  var me = this
  iterator.current(function(err, k, v) {
    if (!k.match(new RegExp('^' + key))) return readStream.emit('end')
    readStream.emit('data', JSON.parse(v))
    iterator.next(function(err) {
      me._advanceKeySeek(key, iterator, readStream)
    })
  })
}

PlumbDB.prototype.keyStream = function(key) {
  var me = this
  var readStream = new stream.Stream()
  this.db.iterator(function(err, iterator) {
    if (err) return readStream.emit('error', err)
    iterator.seek(key, function(err) {
      if (err) return readStream.emit('error', err)
      me._advanceKeySeek(key, iterator, readStream)
    })
  })
  return readStream
}

PlumbDB.prototype._store = function(json, cb) {
  var me = this
  json = me._cloneObj(json)
  if (!json._id) json._id = uuid.v4()
  function save(beforePut) {
    me._updateMetadata(json)
    // todo break out into easy batch function
    var batch = new leveldb.Batch
    if (beforePut) beforePut(batch)
    batch.put(me.stampPrefix + json._stamp, JSON.stringify(json))
    batch.put(me.docPrefix + json._id, json._stamp)
    me.db.write(batch, done)
  }
  function done(err) { cb(err, json) }
  
  me.get(json._id, function(err, stored) {
    // todo decide how to handle err
    if (!stored) return save()
    if (stored._rev !== json._rev) return done({conflict: true})
    return save(function beforePut(batch) {
      batch.del(me.stampPrefix + stored._stamp)
    })
  })

}