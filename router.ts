
export type RoutingFunction = (defaultDeliveryStreamName: string, records: Buffer[]) => RoutingMap
export type RoutingMap = { [streamName: string]: Buffer[] }

/** Simply route records to the provided delivery stream */
export function defaultRouting(defaultDeliveryStreamName: string, records: Buffer[]) {
  const routingMap: RoutingMap = {}
  routingMap[defaultDeliveryStreamName] = records
  return routingMap
}

/**
 * Apply a routing function to a group of records
 */
export function routeToDestination(defaultDeliveryStreamName: string, records: Buffer[], routingFunction: RoutingFunction) {
  const routingMap = routingFunction(defaultDeliveryStreamName, records)
  return routingMap
}
