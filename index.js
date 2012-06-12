var leveldb = require('leveldb')

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

PlumbDB.prototype._store = function(json, cb) {
  json._stamp = this._getUniqueTime()
  this.db.put(json._stamp, JSON.stringify(json), function(err) {
    cb(err, json)
  })
}

PlumbDB.prototype._getUniqueTime = function() {
  var time = +new Date()
  while (time === +new Date())
  return +new Date()+""
}