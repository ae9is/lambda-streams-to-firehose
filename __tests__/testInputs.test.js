var assert = require('assert')
var router = require('../router.js')
var transformer = require('../transformer.js')
require('../constants')

var csvFragment = '"ABC"|12345|22.589|"This is a the product description"'
// These should be identical, but with low-level DynamoDB types in marshalled fragment's Keys, NewImage, and OldImage
var jsonFragment = {Keys:{"key1":"value1","key2":"value2",},"key3":567}
var jsonFragmentString = '{Keys:{"key1":"value1","key2":"value2",},"key3":567}'
var marshalledJsonFragment = {Keys:{"key1":{"S":"value1"},"key2":{"S":"value2"},},"key3":567}
describe('Input Data Tests', function () {
  describe('Verify the transformer does not add escape sequence to strings with double quotes', function () {
    var record = jsonFragmentString
    transformer.addNewlineTransformer(record, function (err, data) {
      if (err) {
        assert.fail(err, undefined, 'Unexpected Error')
      } else {
        it('Does not add escape sequence', function () {
          assert.equal(data.toString(targetEncoding), record + '\n', ' The data got modified ')
        })
      }
    })
  })
  describe('Verify the transformer does not modify non JSON data', function () {
    var record = csvFragment
    transformer.addNewlineTransformer(record, function (err, data) {
      if (err) {
        assert.fail(err, undefined, 'Unexpected Error')
      } else {
        it('Verify that the CSV data is right', function () {
          assert.equal(
            data.toString(targetEncoding),
            record + '\n',
            ' The CSV data got modified'
          )
        })
      }
    })
  })
  describe('Verify the transformer unmarshalls DynamoDB typed data correctly', function () {
    var record = marshalledJsonFragment
    transformer.unmarshallDynamoDBTransformer(record, function (err, data) {
      if (err) {
        assert.fail(err, undefined, 'Unexpected Error')
      } else {
        it('Unmarshalls data correctly', function () {
          assert.equal(data.toString(targetEncoding), JSON.stringify(jsonFragment) + '\n', ' The data got modified ')
        })
      }
    })
  })
})
