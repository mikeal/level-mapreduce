var levelsleep = require('../level-sleep')
  , sleepmapreduce = require('./')
  , assert = require('assert')
  , cleanup = require('cleanup')
  , rimraf = require('rimraf')
  , assert = require('assert')
  , ok = require('okdone')
  , sleepref = require('sleep-ref')
  , http = require('http')
  , noop = function () {}
  ;

var d = cleanup(function (error) {
  rimraf.sync(__dirname+'/testdb')
  if (error) process.exit(1)
  ok.done()
  process.exit()
})

var c = levelsleep(__dirname+'/testdb')
c.get('test', function (e, db) {
  if (e) throw e
  ok('create db')
  db.put('asdf2', {test:1}, noop)
  db.put('asdf', {test:2}, function (e, info) {
    if (e) throw e

    var map = function (obj) { return [['test', obj.data.test]] }
      , index = sleepmapreduce(db.mutex.lev, 'testindex', map)
      , asyncmap = function (entry, cb) {setTimeout( cb(null, [['test', entry.data.test]]), 100 )}
      , asyncindex = sleepmapreduce.async(db.mutex.lev, 'asyncindex', asyncmap)
      , s = sleepref(db.getSequences.bind(db))
      ;

    asyncindex.from(db.nextSequence.bind(db), 's', function (e) {
      if (e) throw e
      asyncindex.query({key:'test'}, function (e, rows) {
        assert.equal(rows.length, 2)
        ok('async')

        http.createServer(s.httpHandler.bind(s)).listen(8080, function () {
          index.pull('http://localhost:8080', function (e) {
            if (e) throw e
            ok('pull')
            index.query({key:'test'}, function (e, rows) {
              assert.equal(rows.length, 2)
              ok('query')
              d.cleanup()
            })
          })
        })
      })
    })
  })
})