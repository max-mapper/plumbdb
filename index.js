var leveldb = require('leveldb')
var JSONStream = require('JSONStream')
var async = require('async')
var microtime = require('microtime')

function PlumbDB(name, cb) {
  var me = this
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
 this.db.get(id, function (err, data) {
    if (err) return cb(err)
    cb(false, JSON.parse(data))
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
  })
}

PlumbDB.prototype._store = function(json, cb) {
  json._stamp = microtime.now() + ""
  this.db.put(json._stamp, JSON.stringify(json), function(err) {
    cb(err, json)
  })
}