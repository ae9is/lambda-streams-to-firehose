var assert = require('assert')
var router = require('../router.js')

describe('Routing tests', function () {
  var defaultDeliveryStream = 'MyDeliveryStream'

  describe('Default Routing', function () {
    var records = [
      Buffer.from('IanTest1').toString('base64'),
      Buffer.from('IanTest2').toString('base64'),
    ]

    router.routeToDestination(
      defaultDeliveryStream,
      records,
      router.defaultRouting.bind(undefined),
      function (err, data) {
        if (err) {
          assert.fail(err, undefined, 'Unexpected Error')
        } else {
          // check the record count
          it('Returns the correct number of records', function () {
            var totalRecords = 0
            Object.keys(data).map(function (key) {
              data[key].map(function (item) {
                totalRecords += 1
              })
            })

            assert.equal(totalRecords, 2, 'Correct Record Count')
          })
          // check that we only get back the default delivery stream
          it('Returns a single destination', function () {
            var keyLen = Object.keys(data).length
            if (keyLen > 1) {
              assert.fail(keyLen, 1, 'Unexpected number of delivery streams')
            }
          })
          it('Returns the correct delivery stream', function () {
            // check the delivery stream name
            Object.keys(data).map(function (key) {
              assert.equal(key, defaultDeliveryStream, 'Unexpected destination')
            })
          })
        }
      }
    )
  })
})
