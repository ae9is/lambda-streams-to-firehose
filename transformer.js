/**
 * Example transformer that adds a newline to each event
 *
 * Args:
 *
 * data - Object or string containing the data to be transformed
 *
 * callback(err, Buffer) - callback to be called once transformation is
 * completed. If supplied callback is with a null/undefined output (such as
 * filtering) then nothing will be sent to Firehose
 */
var aws = require('aws-sdk')
var async = require('async')
require('./constants')

var debug = process.env.DEBUG || false

function addNewlineTransformer(data, callback) {
  // emitting a new buffer as text with newline
  callback(null, Buffer.from(data + '\n', targetEncoding))
}
exports.addNewlineTransformer = addNewlineTransformer

/** Convert JSON data to its String representation */
function jsonToStringTransformer(data, callback) {
  // emitting a new buffer as text with newline
  callback(null, Buffer.from(JSON.stringify(data) + '\n', targetEncoding))
}
exports.jsonToStringTransformer = jsonToStringTransformer

/** Literally nothing at all transformer - just wrap the object in a buffer */
function doNothingTransformer(data, callback) {
  // emitting a new buffer as text with newline
  callback(null, Buffer.from(data, targetEncoding))
}
exports.doNothingTransformer = doNothingTransformer

// DynamoDB Streams emit typed low-level JSON data, like: {"key":{"S":"value"}}
// This transformer unboxes that typing.
function unmarshallDynamoDBTransformer(data, callback) {
  let json = data
  if (typeof data === 'string' || data instanceof String) {
    json = JSON.parse(data)
  }
  const keys = data?.Keys && unmarshall(data.Keys)
  const newImage = data?.NewImage && unmarshall(data.NewImage)
  const oldImage = data?.OldImage && unmarshall(data.OldImage)
  const unmarshalled = { ...data, Keys: keys }
  if (newImage) {
    unmarshalled.NewImage = newImage
  }
  if (oldImage) {
    unmarshalled.OldImage = oldImage
  }
  callback(null, Buffer.from(JSON.stringify(unmarshalled) + '\n', targetEncoding))
}
exports.unmarshallDynamoDBTransformer = unmarshallDynamoDBTransformer

function unmarshall(json) {
  // Data must be JSON, not string
  return aws.DynamoDB.Converter.unmarshall(json)
}

// Unmarshall DynamoDB streams data, and flatten/filter data, extracting only the specified keys from NewImage.
// Data in Keys (i.e. id, sort) will always be extracted but can be overwritten by extract keys for NewImage if specified.
// 
// Also extracts data.eventName (INSERT, MODIFY, REMOVE). It's missing in the docs but present in the actual stream record.
// ref: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_Record.html
//      https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_StreamRecord.html
function flattenDynamoDBTransformer(data, callback) {
  let extractKeys = EXTRACT_KEYS?.split(',')
  if (!extractKeys || extractKeys.length <= 0) {
    extractKeys = []
  }
  let json = data
  if (typeof data === 'string' || data instanceof String) {
    json = JSON.parse(data)
  }
  let keys = data?.Keys ? unmarshall(data.Keys) : {}
  let newImage = data?.NewImage ? unmarshall(data.NewImage) : {}
  const eventName = data?.eventName
  const filtered = { eventName, ...keys, ...filter(newImage, extractKeys) }
  callback(null, Buffer.from(JSON.stringify(filtered) + '\n', targetEncoding))
}
exports.flattenDynamoDBTransformer = flattenDynamoDBTransformer

// Return an object with only specified keys in it
function filter(object, filterKeys) {
  return Object.keys(object).filter(key => filterKeys.includes(key)).reduce((filtered, key) => {
    filtered[key] = object[key]
    return filtered
  }, {})
}

/**
 * Example transformer that converts a regular expression to delimited text
 * 
 * Example regex transformer:
 *  var transformer = exports.regexToDelimTextTransformer.bind(undefined, /(myregex) (.*)/, "|");
 */
function regexToDelimTextTransformer(regex, delimiter, data, callback) {
  var tokens = JSON.stringify(data).match(regex)

  if (tokens) {
    // emitting a new buffer as delimited text whose contents are the regex
    // character classes
    callback(null, Buffer.from(tokens.slice(1).join(delimiter) + '\n', targetEncoding))
  } else {
    callback('Configured Regular Expression does not match any tokens', null)
  }
}
exports.regexToDelimTextTransformer = regexToDelimTextTransformer

function transformRecords(serviceName, transformer, userRecords, callback) {
  async.map(
    userRecords,
    function (userRecord, userRecordCallback) {
      var dataItem =
        serviceName === KINESIS_SERVICE_NAME
          ? Buffer.from(userRecord.data, 'base64').toString(targetEncoding)
          : userRecord

      transformer.call(undefined, dataItem, function (err, transformed) {
        if (err) {
          console.log(JSON.stringify(err))
          userRecordCallback(err)
        } else {
          if (transformed && transformed instanceof Buffer) {
            // call the map callback with the
            // transformed Buffer decorated for use as a
            // Firehose batch entry
            userRecordCallback(null, transformed)
          } else {
            // don't know what this transformed
            // object is
            userRecordCallback(
              'Output of Transformer was malformed. Must be instance of Buffer or routable Object'
            )
          }
        }
      })
    },
    function (err, transformed) {
      // user records have now been transformed, so call
      // errors or invoke the transformed record processor
      if (err) {
        callback(err)
      } else {
        callback(null, transformed)
      }
    }
  )
}
exports.transformRecords = transformRecords

function setupTransformer(callback) {
  // Set the default transformer
  var t = jsonToStringTransformer.bind(undefined)
  // Check if the transformer has been overridden by environment settings
  if (process.env[TRANSFORMER_FUNCTION_ENV]) {
    console.log(process.env[TRANSFORMER_FUNCTION_ENV])
    var found = false
    Object.keys(transformerRegistry).forEach(function (key) {
      if (process.env[TRANSFORMER_FUNCTION_ENV] === transformerRegistry[key]) {
        found = true
      }
    })
    if (found === false) {
      callback(
        'Configured Transformer function ' +
          process.env[TRANSFORMER_FUNCTION_ENV] +
          ' is not a valid transformation method in the transformer.js module'
      )
    } else {
      if (debug) {
        console.log('Setting data transformer based on Transformer Override configuration')
      }
      // Dynamically bind in the transformer function
      t = this[process.env[TRANSFORMER_FUNCTION_ENV]].bind(undefined)
    }
  } else {
    if (debug) {
      console.log('No Transformer Override Environment Configuration found')
    }
    // Set the transformer based on specified datatype of the stream
    if (process.env[STREAM_DATATYPE_ENV]) {
      var found = false
      Object.keys(supportedDatatypeTransformerMappings).forEach(function (key) {
        if (process.env[STREAM_DATATYPE_ENV] === key) {
          found = true
        }
      })
      if (found === true) {
        // Set the transformer class via a cross reference to the transformer mapping
        if (debug) {
          console.log('Setting data transformer based on Stream Datatype configuration')
        }
        t =
          this[
            transformerRegistry[
              supportedDatatypeTransformerMappings[process.env[STREAM_DATATYPE_ENV]]
            ]
          ].bind(undefined)
      }
    } else {
      if (debug) {
        console.log('No Stream Datatype Environment Configuration found')
      }
    }
  }
  if (debug) {
    console.log('Using Transformer function ' + t.name)
  }
  callback(null, t)
}
exports.setupTransformer = setupTransformer
