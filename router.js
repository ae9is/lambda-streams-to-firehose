require('./constants')

/** function which will simply route records to the provided delivery stream */
function defaultRouting(defaultDeliveryStreamName, records, callback) {
  var routingMap = {}
  routingMap[defaultDeliveryStreamName] = records
  callback(null, routingMap)
}
exports.defaultRouting = defaultRouting

/**
 * Function to apply a routing function to a group of records
 *
 * @param defaultDeliveryStreamName
 * @param records
 * @param routingFunction
 * @param callback
 * @returns
 */
function routeToDestination(defaultDeliveryStreamName, records, routingFunction, callback) {
  routingFunction.call(undefined, defaultDeliveryStreamName, records, callback)
}
exports.routeToDestination = routeToDestination
