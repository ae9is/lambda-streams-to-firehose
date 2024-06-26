/*
AWS Streams to Firehose

Modified from: https://github.com/awslabs/lambda-streams-to-firehose

Copyright 2024 ae9is
Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

import { DynamoDBRecord, DynamoDBStreamEvent, StreamRecord } from 'aws-lambda'
import {
  FirehoseClient,
  PutRecordBatchCommand,
  PutRecordBatchCommandInput,
  PutRecordBatchCommandOutput,
  _Record,
} from '@aws-sdk/client-firehose'

import * as pjson from 'package.json'
import { notEmpty, stringify } from './utils'
import * as c from './constants'
import * as transform from './transformer'
import * as router from './router'

const debug = c.DEBUG

// Configure transform utility
// Create the transformer instance - change this to be regexToDelimter, or your own new function
let useTransformer: transform.TransformerFunction

export function setTransformer(transformer: transform.TransformerFunction) {
  useTransformer = transformer
}

// Configure destination router. By default all records route to the configured stream
// Create the routing rule reference that you want to use. This uses the default router.
let useRouter = router.defaultRouting

export function setRouter(router: router.RoutingFunction) {
  useRouter = router
}

// StreamRecord plus a couple attributes from DynamoDBRecord
export type DynamoDBDataItem = StreamRecord & {
  eventName?: 'INSERT' | 'MODIFY' | 'REMOVE'
  userIdentity?: any
}

export async function handler(event: DynamoDBStreamEvent, context: any) {
  if (debug) {
    console.log(stringify(event))
  }
  // Fail if wrong event source type is being sent, or if there is no data, etc
  let noProcessStatus = c.ERROR
  let noProcessReason
  let serviceName: string = ''
  if (!event?.Records || event?.Records?.length === 0) {
    // Not fatal - just got an empty event
    noProcessReason = 'Event contains no Data'
    noProcessStatus = c.OK
  } else if (event?.Records?.[0]?.eventSource === c.DDB_SERVICE_NAME) {
    // There are records in this event
    serviceName = event.Records[0].eventSource
  } else {
    noProcessReason = 'Invalid Event Source ' + event.Records[0].eventSource
  }
  if (noProcessReason) {
    // Terminate if there were any non process reasons
    onCompletion(context, noProcessStatus, c.ERROR, noProcessReason)
  } else {
    try {
      const firehoseClient = await init()
      const deliveryStreamName = getStreamName(event.Records[0].eventSourceARN)
      await processEvent(event, serviceName, deliveryStreamName, firehoseClient)
      onCompletion(context, null, c.OK, 'Finished processing records')
    } catch (err) {
      onCompletion(context, String(err), c.ERROR, 'Error processing records')
    }
  }
}

async function init(): Promise<FirehoseClient> {
  let firehoseClient: FirehoseClient
  let setRegion = process.env.AWS_REGION
  if (!setRegion) {
    setRegion = 'us-east-1'
    console.log('Warning: Setting default region ' + setRegion)
  }
  if (debug) {
    console.log('AWS Streams to Firehose Forwarder v' + pjson.version + ' in ' + setRegion)
  }
  useTransformer = transform.setupTransformer()
  // Configure a new connection to firehose
  if (debug) {
    console.log('Connecting to Amazon Data Firehose in ' + setRegion)
  }
  firehoseClient = new FirehoseClient({
    region: setRegion,
  })
  return firehoseClient
}

/**
 * Extract the stream name from a stream ARN
 */
function getStreamName(arn?: string) {
  try {
    if (!arn) {
      throw Error('No ARN passed')
    }
    const eventSourceARNTokens = arn.split(':')
    return eventSourceARNTokens[5].split('/')[1]
  } catch (e) {
    console.log('Malformed Stream ARN')
    throw e
  }
}

function onCompletion(context: any, err: string | null, status: string, message: string) {
  console.log('Processing Complete')
  if (err) {
    console.error(err)
  }
  // Log the event if we've failed
  if (status !== c.OK) {
    if (message) {
      console.log(message)
    }
    // Ensure that Lambda doesn't checkpoint to stream on error
    context.done(status, stringify(message))
  } else {
    context.done(null, message)
  }
}

/**
 * Process a stream event and generate requests to forward the embedded records to Firehose.
 * Before delivery, the user specified transformer will be invoked,
 * and the messages will be passed through a router which can determine the delivery stream dynamically if needed.
 */
async function processEvent(
  event: DynamoDBStreamEvent,
  serviceName: string,
  deliveryStreamName: string,
  firehoseClient: FirehoseClient
) {
  const itemsToHandle: DynamoDBDataItem[] = []
  if (debug) {
    console.log('Processing event')
  }
  if (debug) {
    console.log(
      `Forwarding ${event.Records.length} ${serviceName} records to Delivery Stream ${deliveryStreamName}`
    )
  }
  for (const record of event.Records) {
    // DynamoDB update stream record
    if (record?.eventName && c.writableEventTypes.includes(record.eventName)) {
      if (debug) {
        console.log(
          `Processing record: ${stringify(record)} with event type: ${record.eventName} when writable events are: ${c.writableEventTypes}`
        )
      }
      const item = createDynamoDataItem(record)
      itemsToHandle.push(item)
    } else {
      if (debug) {
        console.log(
          `Skipping record: ${stringify(record)} with event type: ${record.eventName} when writable events are: ${c.writableEventTypes}`
        )
      }
    }
  }
  await handleResults(firehoseClient, deliveryStreamName, itemsToHandle)
}

/**
 * Create a condensed version of a dynamodb change record.
 * This is returned as a base64 encoded Buffer so as to implement the same interface
 * used for transforming stream records
 */
function createDynamoDataItem(record: DynamoDBRecord): DynamoDBDataItem {
  const output: DynamoDBDataItem = {}
  output.Keys = record?.dynamodb?.Keys
  if (record?.dynamodb?.NewImage) {
    output.NewImage = record.dynamodb.NewImage
  }
  if (record?.dynamodb?.OldImage) {
    output.OldImage = record.dynamodb.OldImage
  }
  // Add the sequence number and other metadata
  output.SequenceNumber = record?.dynamodb?.SequenceNumber
  output.SizeBytes = record?.dynamodb?.SizeBytes
  output.ApproximateCreationDateTime = record?.dynamodb?.ApproximateCreationDateTime
  output.eventName = record.eventName
  // Adding userIdentity, used by DynamoDB TTL to indicate removal by TTL as opposed to user initiated remove
  output.userIdentity = record.userIdentity
  return output
}

async function handleResults(
  firehoseClient: FirehoseClient,
  deliveryStreamName: string,
  results?: DynamoDBDataItem[]
) {
  const extractedUserRecords = results ?? []
  // ExtractedUserRecords will be array[array[Object]], so flatten to array[Object]
  const userRecords: DynamoDBDataItem[] = extractedUserRecords.filter(notEmpty).flat()
  // Transform the user records
  try {
    const transformed: Buffer[] = transform.transformRecords(useTransformer, userRecords)
    if (transformed === undefined) {
      if (debug) {
        console.log('Nothing to route')
      }
      return
    }
    // Apply the routing function that has been configured
    let routingDestinationMap: router.RoutingMap = {}
    let hadError = false
    try {
      routingDestinationMap = router.routeToDestination(deliveryStreamName, transformed, useRouter)
      // Send the routed records to the delivery processor
      for (const destinationStream of Object.keys(routingDestinationMap)) {
        try {
          const records = routingDestinationMap[destinationStream]
          await processFinalRecords(firehoseClient, records, deliveryStreamName, destinationStream)
        } catch (err) {
          hadError = true
        }
      }
    } catch (err) {
      console.error(err)
      hadError = true
    }
    if (hadError) {
      // If routing to any of the destination streams failed, or the whole thing failed,
      //  also route all records to the default delivery stream to avoid losing data.
      // If frequent failures were expected, this could be reworked to route only the failed batches of records.
      await processFinalRecords(firehoseClient, transformed, deliveryStreamName, deliveryStreamName)
    }
  } catch (err) {
    console.error(err)
  }
}

/**
 * Handle output of the defined transformation on each record.
 */
async function processFinalRecords(
  firehoseClient: FirehoseClient,
  records: Uint8Array[],
  streamName: string,
  deliveryStreamName: string
) {
  if (debug) {
    console.log('Delivering records to destination Streams')
  }
  // Get the set of batch offsets based on the transformed record sizes
  const batches = getBatchRanges(records)
  if (debug) {
    console.log(stringify(batches))
  }
  // Push to Firehose using PutRecords API at max record count or size.
  // Want records from Stream to appear in the Firehose PutRecords request in the same order as they
  // were received by this function.
  let successCount = 0
  try {
    let failedCount = 0
    for (const item of batches) {
      if (debug) {
        console.log(
          `Forwarding records ${item.lowOffset}: ${item.highOffset} - ${item.sizeBytes} Bytes`
        )
      }
      // Grab subset of the records assigned for this batch and push to firehose
      const processRecords = records.slice(item.lowOffset, item.highOffset)
      // Decorate the array for the Firehose API
      const decorated: _Record[] = []
      processRecords.map(function (item: Uint8Array | undefined) {
        decorated.push({
          Data: item,
        })
      })
      const failedRecords = await writeToFirehose(
        firehoseClient,
        decorated,
        streamName,
        deliveryStreamName
      )
      // Keep track of (partially) failed batches, but continue processing for now and throw exception at end
      failedCount += failedRecords?.FailedPutCount ?? 0
      successCount += 1
    }
    if (failedCount > 0) {
      throw Error(`Failed writing a batch with ${failedCount} failed records`)
    }
    console.log(
      `Event forwarding complete. Forwarded ${successCount} batches comprising ${records.length} records to Firehose ${deliveryStreamName}`
    )
  } catch (err) {
    console.log(`Forwarding failure after ${successCount} successful batches`)
    throw err
  }
}

interface BatchItem {
  lowOffset: number
  highOffset: number
  sizeBytes: number
}

/**
 * Convenience function which generates the batch set with low and high offsets
 * for pushing data to Firehose in blocks of FIREHOSE_MAX_BATCH_COUNT and
 * staying within the FIREHOSE_MAX_BATCH_BYTES max payload size.
 *
 * Batch ranges are calculated to be compatible with the array.slice() function which uses a
 * non-inclusive upper bound.
 */
function getBatchRanges(records: Uint8Array[]): BatchItem[] {
  const batches = []
  let currentLowOffset = 0
  let batchCurrentBytes = 0
  let batchCurrentCount = 0
  let recordSize
  let nextRecordSize
  for (let i = 0; i < records.length; i++) {
    // Need to calculate the total record size for the call to Firehose on
    //  the basis of of non-base64 encoded values
    recordSize = Buffer.byteLength(records[i].toString(), c.targetEncoding)
    // Batch always has 1 entry, so add it first
    batchCurrentBytes += recordSize
    batchCurrentCount += 1
    // To get next record size inorder to calculate the FIREHOSE_MAX_BATCH_BYTES
    if (i === records.length - 1) {
      nextRecordSize = 0
    } else {
      nextRecordSize = Buffer.byteLength(records[i + 1].toString(), c.targetEncoding)
    }
    // Generate a new batch marker every 4MB or 500 records, whichever comes first
    if (
      batchCurrentCount === c.FIREHOSE_MAX_BATCH_COUNT ||
      batchCurrentBytes + nextRecordSize > c.FIREHOSE_MAX_BATCH_BYTES ||
      i === records.length - 1
    ) {
      batches.push({
        lowOffset: currentLowOffset,
        // Annoying special case handling for record sets of size 1
        highOffset: i + 1,
        sizeBytes: batchCurrentBytes,
      })
      // Reset accumulators
      currentLowOffset = i + 1
      batchCurrentBytes = 0
      batchCurrentCount = 0
    }
  }
  return batches
}

/**
 * Forward a batch of records to a firehose delivery stream
 */
async function writeToFirehose(
  firehoseClient: FirehoseClient,
  firehoseBatch: _Record[] | undefined,
  streamName: string,
  deliveryStreamName: string,
  retries?: number
) {
  let leftover: PutRecordBatchCommandOutput | undefined = undefined
  const numRetries = retries ?? 0
  // Write the batch to firehose with putRecordBatch
  const putRecordBatchParams: PutRecordBatchCommandInput = {
    DeliveryStreamName: deliveryStreamName.substring(0, 64),
    Records: firehoseBatch,
  }
  if (debug) {
    console.log(`Writing to firehose delivery stream (attempt ${numRetries})`)
    console.log(stringify(putRecordBatchParams))
  }
  const cmd = new PutRecordBatchCommand(putRecordBatchParams)
  try {
    if (debug) {
      console.log('Firehose client', firehoseClient)
      console.log('Cmd:', cmd)
      console.log('Sending cmd at: ' + new Date())
    }
    const startTime = new Date().getTime()
    const resp = await firehoseClient.send(cmd)
    const elapsedMs = new Date().getTime() - startTime
    if (debug) {
      console.log('Finished cmd at:' + new Date())
      const successCount = Math.max(0, (firehoseBatch?.length ?? 0) - (resp.FailedPutCount ?? 0))
      if (successCount > 0) {
        console.log(
          `Successfully wrote ${successCount} records to Firehose ${deliveryStreamName} in ${elapsedMs} ms`
        )
      }
    }
    if (resp.FailedPutCount !== 0) {
      console.log(
        `Failed to write ${resp.FailedPutCount}/${firehoseBatch?.length ?? 0} records. Retrying to write...`
      )
      if (numRetries < c.MAX_RETRY_ON_FAILED_PUT) {
        // Extract the failed records
        const failedBatch: _Record[] = []
        resp.RequestResponses?.map(function (item: any, index: number) {
          if (item.hasOwnProperty('ErrorCode') && firehoseBatch?.[index]) {
            failedBatch.push(firehoseBatch?.[index])
          }
        })
        await new Promise((r) => setTimeout(r, c.RETRY_INTERVAL_MS))
        return await writeToFirehose(
          firehoseClient,
          failedBatch,
          streamName,
          deliveryStreamName,
          numRetries + 1
        )
      } else {
        console.log('Maximum retries reached, giving up')
        leftover = resp
      }
    }
  } catch (err) {
    console.log(stringify(err))
    throw err
  }
  return leftover
}
