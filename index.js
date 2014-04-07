var events = require('events')
  , mutex = require('level-mutex')
  , uuid = require('node-uuid')
  , byteslice = require('byteslice')
  , util = require('util')
  , events = require('events')
  , levelup = require('levelup')
  , stream = require('stream')
  , once = require('once')
  , noop = function () {}
  ;

function BaseQuery () {}
util.inherits(BaseQuery, stream.Transform)
BaseQuery.prototype.map = function (func) {
}
BaseQuery.prototype.filter = function (func) {
}
BaseQuery.prototype.reduce = function (func, cb, last) {
}

function Query (index, opts, cb) {
  var self = this
  // index.mutex.afterWrite(function () {
  //   self.init(index.mutex.lev.createReadStream(opts))
  // })
}
util.inherits(Query, BaseQuery)

function Index (name, map, opts) {
  if (typeof opts.lev === 'string') opts.lev = levelup(opts.lev, {keyEncoding:'binary', valueEncoding:'json'})
  this.mutex = mutex(opts.lev)
  this.lev = opts.lev
  this.name = name
  this.map = map
  this.bytes = byteslice([name, 'level-mapreduce'])
  this.opts = opts || {}
  // if (!this.opts.key) this.opts.key = function (entry) { return entry.key }
  stream.Transform.call(this, {objectMode:true})
}
util.inherits(Index, stream.Transform)
Index.prototype.createReadStream = function (opts) {
  if (!opts.raw) {
    if (opts.start) opts.start = self.bytes.encode(['index', opts.start])
    if (opts.end) opts.end = self.bytes.encode(['index', opts.end])
    if (!opts.start) opts.start = self.bytes.encode(['index'])
    if (!opts.end) opts.end = self.bytes.encode(['index', {}])
  }

  return this.mutex.lev.createReadStream(opts)
}

Index.prototype.query = function (opts, cb) {
  // return new Query(this, opts, cb)
}
Index.prototype.count = function (cb) {

}
Index.prototype.get = function (key, opts, cb) {
  var self = this
  if (!cb) {
    cb = opts
    opts = {}
  }
  cb = once(cb)
  opts.raw = true
  opts.start = self.bytes.encode(['index', key])
  opts.end = self.bytes.encode(['index', key, {}])
  var reader = self.createReadStream(opts)
    , chunks = []
    ;

  reader.on('data', function (chunk) {
    chunks.push(chunk)
  })
  reader.on('error', cb)
  reader.on('end', function () {
    cb(null, chunks)
  })
}
Index.prototype._transform = function (chunk, encoding, cb) {
  var self = this
  if (typeof chunk !== 'object') return cb() // someone piped us a not-object

  if (!chunk.deleted && (!chunk.key || !chunk.value)) return cb() // not a valid format

  self.getMeta(chunk.key, function (e, meta) {
    if (!e) {
      meta.keys.forEach(function (key) {
        self.mutex.del(key, noop)
      })
      if (chunk.deleted) {
        self.mutex.del(meta.index, function (e) {
          if (e) return cb(e)
          if (self._piped) self.push({key:chunk.key, value:[]})
          cb()
        })
        return
      }
    }

    // if deleted just finish up by removing the meta
    if (chunk.deleted) {
      self.mutex.del(meta.index, function (e) {
        if (e) return cb(e)
        if (self._piped) self.push({key:chunk.key, value:[]})
        cb()
      })
      return
    }

    var mapped = self.map(chunk)

    if (!mapped[0]) {
      // if there is a meta we have to remove it
      if (!e) self.mutex.del(meta.index, function (e) {
        if (e) return cb(e)
        if (self._piped) self.push({key:chunk.key, value:[]})
        cb()
      })
      return
    }

    var meta = {keys:[]}
    for (var i in mapped) {
      var key = mapped[i][0]
        , value = mapped[i][1]
        , encoded = self.bytes.encode(['index', key, uuid()])
        ;
      meta.keys.push(encoded)
      self.mutex.put(encoded, value, noop)
    }

    self.mutex.put(self.bytes.encode(['meta', chunk.key]), meta, function (e) {
      if (e) return cb(e)
      if (self._piped) self.push({key:chunk.key, value:mapped})
      cb()
    })
  })
}
Index.prototype.pipe = function () {
  this._piped = true
  stream.Transform.prototype.pipe.apply(this, arguments)
}

Index.prototype.getMeta = function (key, cb) {
  var index = this.bytes.encode(['meta', key])
  this.mutex.get(index, function (e, meta) {
    // TODO: add conveniences
    if (!meta) meta = {}
    meta.index = index
    return cb(e, meta)
  })
}

// Index.prototype.write = function (entry, ref, tuples, cb) {
// }

function AsyncIndex () {
  Index.apply(this, arguments)
}
util.inherits(AsyncIndex, Index)

module.exports = function (lev, name, map, opts) { return new Index(lev, name, map, opts) }
module.exports.async = function (lev, name, map, opts) {return new AsyncIndex(lev, name, map, opts) }
