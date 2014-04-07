var mapstore = require('../')
  , assert = require('assert')
  , cleanup = require('cleanup')
  , rimraf = require('rimraf')
  , assert = require('assert')
  , http = require('http')
  , noop = function () {}
  , tape = require('tape')
  , index
  ;

tape('setup', function (t) {
  rimraf.sync(__dirname+'/test-basic.db')
  var map = function (obj) { return [['test', obj.value.test]] }
  index = mapstore('testindex', map, {lev:__dirname+'/test-basic.db'})
  t.end()
})

tape('write and get', function (t) {
  index.write({key:'asdf', value:{test:123}}, function () {
    index.get('test', function (e, results) {
      t.equal(results.length, 1)
      t.equal(results[0].value, 123)
      t.end()
    })
  })
})

tape('overwrite', function (t) {
  index.write({key:'asdf', value:{test:123}}, function () {
    index.write({key:'asdf', value:{test:345}}, function () {
      index.get('test', function (e, results) {
        t.equal(results.length, 1)
        t.equal(results[0].value, 345)
        t.end()
      })
    })
  })
})

tape('cleanup', function (t) {
  rimraf.sync(__dirname+'/test-basic.db')
  t.end()
})
