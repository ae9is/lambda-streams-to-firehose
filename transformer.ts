// Transformers that take data, transform it, and return it in a buffer to write to the delivery stream.

import { unmarshall } from '@aws-sdk/util-dynamodb'

import * as c from './constants'
import { DynamoDBDataItem } from './handler'
import { filter } from './utils'

const debug = c.DEBUG

type TransformerData = string | DynamoDBDataItem
export type TransformerFunction = (data: TransformerData) => Buffer

const transformerFunctions = {
  doNothingTransformer: doNothingTransformer,
  addNewlineTransformer: addNewlineTransformer,
  jsonToStringTransformer: jsonToStringTransformer,
  unmarshallDynamoDBTransformer: unmarshallDynamoDBTransformer,
  flattenDynamoDBTransformer: flattenDynamoDBTransformer,
}

/** Emit buffer as text with newline */
export function addNewlineTransformer(data: TransformerData) {
  return Buffer.from(data + '\n', c.targetEncoding)
}

/** Convert JSON data to its String representation */
export function jsonToStringTransformer(data: TransformerData) {
  return Buffer.from(JSON.stringify(data) + '\n', c.targetEncoding)
}

/** Just wrap the object in a buffer */
export function doNothingTransformer(data: TransformerData) {
  return Buffer.from(data + '', c.targetEncoding)
}

/**
 * DynamoDB Streams emit typed low-level JSON data, like: {"key":{"S":"value"}}
 * This transformer unboxes that typing.
 */
export function unmarshallDynamoDBTransformer(data: TransformerData) {
  let json: any
  if (typeof data === 'string' || data instanceof String) {
    json = JSON.parse(data.toString())
  } else {
    json = data
  }
  const keys = json?.Keys && unmarshall(json.Keys)
  const newImage = json?.NewImage && unmarshall(json.NewImage)
  const oldImage = json?.OldImage && unmarshall(json.OldImage)
  const unmarshalled = { ...json, Keys: keys }
  if (newImage) {
    unmarshalled.NewImage = newImage
  }
  if (oldImage) {
    unmarshalled.OldImage = oldImage
  }
  return Buffer.from(JSON.stringify(unmarshalled) + '\n', c.targetEncoding)
}

/**
 * Unmarshall DynamoDB streams data, and flatten/filter data, extracting only the specified keys from NewImage.
 * Data in Keys (i.e. id, sort) will always be extracted but can be overwritten by extract keys for NewImage if specified.
 *
 * Also extracts data.eventName (INSERT, MODIFY, REMOVE). It's missing in the docs but present in the actual stream record.
 * ref: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_Record.html
 *      https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_StreamRecord.html
 */
export function flattenDynamoDBTransformer(data: TransformerData) {
  let extractKeys = c.EXTRACT_KEYS?.split(',')
  if (!extractKeys || extractKeys.length <= 0) {
    extractKeys = []
  }
  let json: any
  if (typeof data === 'string' || data instanceof String) {
    json = JSON.parse(data.toString())
  } else {
    json = data
  }
  let keys = json?.Keys ? unmarshall(json.Keys) : {}
  let newImage = json?.NewImage ? unmarshall(json.NewImage) : {}
  const eventName = json?.eventName
  const filtered = { eventName, ...keys, ...filter(newImage, extractKeys) }
  return Buffer.from(JSON.stringify(filtered) + '\n', c.targetEncoding)
}

export function transformRecords(
  transformer: TransformerFunction,
  userRecords: DynamoDBDataItem[]
): Buffer[] {
  const transformedRecords: Buffer[] = []
  for (const userRecord of userRecords) {
    let record: string | typeof userRecord = userRecord
    const dataItem = record
    try {
      const transformed: Buffer = transformer(dataItem)
      if (transformed && transformed instanceof Buffer) {
        transformedRecords.push(transformed)
      } else {
        console.error(
          'Output of Transformer was malformed. Must be instance of Buffer or routable Object'
        )
      }
    } catch (err) {
      console.error(JSON.stringify(err))
    }
  }
  return transformedRecords
}

export function setupTransformer(): TransformerFunction {
  // Set the default transformer
  let transformer: TransformerFunction = jsonToStringTransformer
  // Check if the transformer has been overridden by environment settings
  const TRANSFORMER_FUNCTION_ENV: string | undefined = process.env[c.TRANSFORMER_FUNCTION_ENV]
  let TRANSFORMER_FUNCTION: keyof typeof transformerFunctions = 'jsonToStringTransformer'
  if (TRANSFORMER_FUNCTION_ENV) {
    let found = false
    for (const [key, value] of Object.entries(c.transformerRegistry)) {
      if (TRANSFORMER_FUNCTION_ENV === value) {
        found = true
        TRANSFORMER_FUNCTION = TRANSFORMER_FUNCTION_ENV as keyof typeof c.transformerRegistry
      }
    }
    if (!found) {
      const error =
        `Configured Transformer function ${TRANSFORMER_FUNCTION} is not a valid ` +
        `transformation method in the transformer.js module ${transformer}`
      throw Error(error)
    } else {
      if (debug) {
        console.log('Setting data transformer based on Transformer Override configuration')
      }
      transformer = transformerFunctions[TRANSFORMER_FUNCTION]
    }
  } else {
    if (debug) {
      console.log('No Transformer Override Environment Configuration found')
    }
    // Set the transformer based on specified datatype of the stream
    const STREAM_DATATYPE_ENV: string | undefined = process.env[c.STREAM_DATATYPE_ENV]
    let STREAM_DATATYPE: keyof typeof c.supportedDatatypeTransformerMappings = 'JSON'
    if (STREAM_DATATYPE_ENV) {
      let found = false
      Object.keys(c.supportedDatatypeTransformerMappings).forEach(function (key) {
        if (STREAM_DATATYPE_ENV === key) {
          found = true
          STREAM_DATATYPE =
            STREAM_DATATYPE_ENV as keyof typeof c.supportedDatatypeTransformerMappings
        }
      })
      if (found) {
        // Set the transformer class via a cross reference to the transformer mapping
        if (debug) {
          console.log('Setting data transformer based on Stream Datatype configuration')
        }
        const supportedDatatype = c.supportedDatatypeTransformerMappings[STREAM_DATATYPE]
        transformer = transformerFunctions[supportedDatatype]
      }
    } else {
      if (debug) {
        console.log('No Stream Datatype Environment Configuration found')
      }
    }
  }
  if (debug) {
    console.log(`Using Transformer function ${transformer.name}`)
  }
  return transformer
}
