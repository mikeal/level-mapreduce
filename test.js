var couchup = require('couchup')
  , sleepmapreduce = require('./')
  , assert = require('assert')
  , cleanup = require('cleanup')
  , rimraf = require('rimraf')
  , assert = require('assert')
  , ok = require('okdone')
  , sleepref = require('sleep-ref')
  , http = require('http')
  ;

var d = cleanup(function (error) {
  rimraf.sync(__dirname+'/testdb')
  if (error) process.exit(1)
  ok.done()
})

var c = couchup(__dirname+'/testdb')
c.put('test', function (e, db) {
  if (e) throw e
  ok('create db')
  db.put({_id:'asdf', test:1}, function (e, info) {
    if (e) throw e

    var map = function (obj) { return [['test', obj.test]] }
      , index = sleepmapreduce(db.mutex.lev, 'testindex', map)
      , s = sleepref(db.sleep.bind(db))
      ;
    http.createServer(s.httpHandler.bind(s)).listen(8080, function () {
      index.pull('http://localhost:8080', function (e) {
        if (e) throw e
        ok('pull')
        index.query({key:'test'}, function (e, rows) {
          assert.equal(rows.length, 1)
          ok('query')
          d.cleanup()
        })
      })
    })

  })
})