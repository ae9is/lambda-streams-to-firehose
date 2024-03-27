export const OK = 'OK'
export const ERROR = 'ERROR'
export const FORWARD_TO_FIREHOSE_STREAM = 'ForwardToFirehoseStream'
export const DDB_SERVICE_NAME = 'aws:dynamodb'
export const FIREHOSE_MAX_BATCH_COUNT = 500
// Firehose max PutRecordBatch size 4MB
export const FIREHOSE_MAX_BATCH_BYTES = 4 * 1000 * 1000
export const MAX_RETRY_ON_FAILED_PUT: number = Number(process.env['MAX_RETRY_ON_FAILED_PUT']) || 3
export const RETRY_INTERVAL_MS: number = Number(process.env['RETRY_INTERVAL_MS']) || 300
// The names of the transformer function and stream data type environment variables,
//  not the values themselves.
// I.e. you may set environment variable TRANSFORMER_FUNCTION = 'doNothingTransformer'
//  or STREAM_DATATYPE = 'JSON'
export const TRANSFORMER_FUNCTION_ENV = 'TRANSFORMER_FUNCTION'
export const STREAM_DATATYPE_ENV = 'STREAM_DATATYPE'
export const targetEncoding = 'utf8'
export enum transformerRegistry {
  doNothingTransformer = 'doNothingTransformer',
  addNewlineTransformer = 'addNewlineTransformer',
  jsonToStringTransformer = 'jsonToStringTransformer',
  unmarshallDynamoDBTransformer = 'unmarshallDynamoDBTransformer',
  flattenDynamoDBTransformer = 'flattenDynamoDBTransformer',
}
export type transformerRegistryKey = keyof typeof transformerRegistry
export enum supportedDatatypeTransformerMappings {
  JSON = transformerRegistry.jsonToStringTransformer,
  CSV = transformerRegistry.addNewlineTransformer,
  BINARY = transformerRegistry.doNothingTransformer,
  'CSV-WITH-NEWLINES' = transformerRegistry.doNothingTransformer,
}
export const setRegion = process.env['AWS_REGION'] || 'us-east-1'
export const DEFAULT_DELIVERY_STREAM =
  process.env['DEFAULT_DELIVERY_STREAM'] || 'anything-dev-abc123-main'
// For flattenDynamoDBTransformer only:
export const EXTRACT_KEYS =
  process.env['EXTRACT_KEYS'] || 'modified,author,type,ctype,deleted,currentVersion'
export const DEBUG = process.env.DEBUG || false
const allEventTypes = ['INSERT', 'MODIFY', 'REMOVE']
export const writableEventTypes = process.env['WRITABLE_EVENT_TYPES']?.split(',') ?? allEventTypes
