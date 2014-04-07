var mapstore = require('../')
  , assert = require('assert')
  , cleanup = require('cleanup')
  , rimraf = require('rimraf')
  , assert = require('assert')
  , http = require('http')
  , noop = function () {}
  , tape = require('tape')
  , index
  , secondary
  ;

tape('setup', function (t) {
  rimraf.sync(__dirname+'/test-replication.db')
  var map = function (obj) { return [['test', obj.value.test]] }
  index = mapstore('testindex', map, {lev:__dirname+'/test-replication.db'})

  var mapSecondary = function (obj) { return [['test2', obj.value[0][1] ]] }

  secondary = mapstore('secondaryindex', mapSecondary, {lev: index.lev})

  index.pipe(secondary)

  t.end()
})

tape('write and get', function (t) {
  index.write({key:'asdf', value:{test:123}}, function () {
    setTimeout(function () {
      secondary.get('test2', function (e, results) {
        t.equal(results.length, 1)
        t.equal(results[0].value, 123)
        t.end()
      })
    }, 10)
  })
})

tape('overwrite', function (t) {
  index.write({key:'asdf', value:{test:123}}, function () {
    index.write({key:'asdf', value:{test:345}}, function () {
      setTimeout(function () {
        secondary.get('test2', function (e, results) {
          t.equal(results.length, 1)
          t.equal(results[0].value, 345)
          t.end()
        })
      }, 10)
    })
  })
})

tape('cleanup', function (t) {
  rimraf.sync(__dirname+'/test-replication.db')
  t.end()
})
