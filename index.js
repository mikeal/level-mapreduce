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

function Query (cb) {
  this.cb = once(cb)
  this.mutations = []
  if (this.cb) {
    this.results = []
    this.on('error', cb)
    console.log('cb')
  }
  stream.Transform.call(this, {objectMode:true})
}
util.inherits(Query, stream.Transform)
Query.prototype._transform = function (chunk, encoding, cb) {
  var self = this
  if (self.mutations.length === 0) {
    if (self.cb) self.results.push(chunk)
    self.push(chunk)
    return cb()
  }
  var _chunk = chunk
  function iterate (i) {
    self.mutations[i](_chunk, function (e, rechunk) {
      if (e) return cb()
      if (i === (self.mutations.length - 1)) {
        if (self.cb) self.results.push(rechunk)
        self.push(rechunk)
        return cb()
      }
      _chunk = rechunk
      iterate(i+1)
    })
  }
  iterate(0)
}

Query.prototype.map = function (func) {
  this.mutations.push(function (chunk, cb) {
    cb(null, func(chunk))
  })
  return this
}
Query.prototype.asyncMap = function (func) {
  this.mutations.push(func)
  return this
}
Query.prototype.filter = function (func) {
  this.mutations.push(function (chunk, cb) {
    if (func(chunk)) cb(null, chunk)
    else cb('skip')
  })
  return this
}
Query.prototype.group = function (finish) {
  var current
    , results = []
    ;
  this.mutations.push(function (chunk, cb) {
    if (chunk.key !== current) {
      if (current) {
        cb(null, results)
        current = null
        results = []
      } else {
        current = chunk.key
        results.push(chunk.value)
        cb('skip')
      }
    } else {
      results.push(chunk.value)
      cb('skip')
    }
  })
  this.on('preend', function () {
    console.error('preend')
    if (current) this.push({key:current, results:results})
  })
  return this
}
Query.prototype.end = function () {
  this.emit('preend')
  if (this.cb) this.cb(null, this.results)
  stream.Transform.prototype.end.apply(this, arguments)
}

function DecodeStream (decode) {
  this.decode = decode
  stream.Transform.call(this, {objectMode:true})
}
util.inherits(DecodeStream, stream.Transform)
DecodeStream.prototype._transform = function (chunk, encoding, cb) {
  chunk.rawkey = chunk.key
  chunk.key = this.decode(chunk.key)[1]
  this.push(chunk)
  cb()
}
DecodeStream.prototype.end = function () {
  stream.Transform.prototype.end.apply(this, arguments)
}


function Index (lev, name, map, opts) {
  if (typeof lev === 'string') lev = levelup(lev, {keyEncoding:'binary', valueEncoding:'json'})
  this.mutex = mutex(lev)
  this.lev = lev
  this.name = name
  this.map = map
  this.bytes = byteslice([name, 'level-mapreduce'])
  this.opts = opts || {}
  // if (!this.opts.key) this.opts.key = function (entry) { return entry.key }
  stream.Transform.call(this, {objectMode:true})
}
util.inherits(Index, stream.Transform)
Index.prototype.createReadStream = function (opts) {
  if (!opts) opts = {}
  if (!opts.raw) {
    if (opts.start) opts.start = this.bytes.encode(['index', opts.start])
    if (opts.end) opts.end = this.bytes.encode(['index', opts.start])
    if (opts.key) {
      opts.start = this.bytes.encode(['index', opts.key])
      opts.end = this.bytes.encode(['index', opts.key, {}])
    }
    if (!opts.start) opts.start = this.bytes.encode(['index'])
    if (!opts.end) opts.end = this.bytes.encode(['index', {}])
  }

  return this.mutex.lev.createReadStream(opts).pipe(new DecodeStream(this.bytes.decode.bind(this.bytes)))
}

Index.prototype.query = function (opts, cb) {
  var decode = this.createReadStream(opts)
    , query = new Query(cb)
    ;
  decode.pipe(query)
  return query
  // return new Query(this, opts, cb)
}
Index.prototype.count = function (opts, cb) {

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
    chunks.push(chunk.value)
  })
  reader.on('error', cb)
  reader.on('end', function () {
    cb(null, chunks)
  })
}
Index.prototype._transform = function (chunk, encoding, cb) {
  var self = this

  if (typeof chunk !== 'object') return cb() // someone piped us a not-object

  if (!chunk.deleted &&
       (typeof chunk.key === 'undefined' || typeof chunk.value === 'undefined')
     ) return cb() // not a valid format

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


    if (self.async) {
      self.map(chunk, function (e, mapped) {
        if (e) return cb(e)
        finish(mapped)
      })
    } else {
      finish(self.map(chunk))
    }

    function finish (mapped)  {
      if (!mapped[0]) {
        // if there is a meta we have to remove it
        if (!e) {
          self.mutex.del(meta.index, function (e) {
            if (e) return cb(e)
            if (self._piped) self.push({key:chunk.key, value:[]})
            cb()
          })
        } else {
          cb()
        }
        return
      }

      var _meta = {keys:[]}
      for (var i in mapped) {
        var key = mapped[i][0]
          , value = mapped[i][1]
          , encoded = self.bytes.encode(['index', key, uuid()])
          ;
        _meta.keys.push(encoded)
        self.mutex.put(encoded, value, noop)
      }

      self.mutex.put(self.bytes.encode(['meta', chunk.key]), _meta, function (e) {
        if (e) return cb(e)
        if (self._piped) self.push({key:chunk.key, value:mapped})
        cb()
      })
    }
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
  this.async = true
  Index.apply(this, arguments)
}
util.inherits(AsyncIndex, Index)

module.exports = function (lev, name, map, opts) { return new Index(lev, name, map, opts) }
module.exports.async = function (lev, name, map, opts) {
  return new AsyncIndex(lev, name, map, opts)
}
