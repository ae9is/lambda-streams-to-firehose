OK = 'OK'
ERROR = 'ERROR'
FORWARD_TO_FIREHOSE_STREAM = 'ForwardToFirehoseStream'
DDB_SERVICE_NAME = 'aws:dynamodb'
KINESIS_SERVICE_NAME = 'aws:kinesis'
FIREHOSE_MAX_BATCH_COUNT = 500
// Firehose max PutRecordBatch size 4MB
FIREHOSE_MAX_BATCH_BYTES = 4 * 1000 * 1000
MAX_RETRY_ON_FAILED_PUT = process.env['MAX_RETRY_ON_FAILED_PUT'] || 3
RETRY_INTERVAL_MS = process.env['RETRY_INTERVAL_MS'] || 300
// The names of the transformer function and stream data type environment variables, 
//  not the values themselves.
// I.e. you may set environment variable TRANSFORMER_FUNCTION = 'doNothingTransformer'
//  or STREAM_DATATYPE = 'JSON'
TRANSFORMER_FUNCTION_ENV = 'TRANSFORMER_FUNCTION'
STREAM_DATATYPE_ENV = 'STREAM_DATATYPE'
targetEncoding = 'utf8'
transformerRegistry = {
  doNothingTransformer: 'doNothingTransformer',
  addNewlineTransformer: 'addNewlineTransformer',
  jsonToStringTransformer: 'jsonToStringTransformer',
  regexToDelimTextTransformer: 'regexToDelimTextTransformer',
  unmarshallDynamoDBTransformer: 'unmarshallDynamoDBTransformer',
  flattenDynamoDBTransformer: 'flattenDynamoDBTransformer',
}
supportedDatatypeTransformerMappings = {
  JSON: transformerRegistry.jsonToStringTransformer,
  CSV: transformerRegistry.addNewlineTransformer,
  BINARY: transformerRegistry.doNothingTransformer,
  'CSV-WITH-NEWLINES': transformerRegistry.doNothingTransformer,
}
setRegion = process.env['AWS_REGION'] || 'us-east-1'
DEFAULT_DELIVERY_STREAM = process.env['DEFAULT_DELIVERY_STREAM'] || 'delivery-stream-name'
// For flattenDynamoDBTransformer only:
EXTRACT_KEYS = process.env['EXTRACT_KEYS'] || 'modified,author,type,ctype,deleted,currentVersion'