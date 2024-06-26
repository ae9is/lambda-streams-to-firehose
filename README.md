# Lambda streams to firehose

Process DynamoDB streams data through Lambda to Firehose. This is an alternative to setting up a Kinesis Stream, which incurs per hour costs.

Based on code from: [awslabs/lambda-streams-to-firehose](https://github.com/awslabs/lambda-streams-to-firehose)

This rewrite reduces scope, adds typing, and modernises. It was written as part of a larger project. See here for an example of packaging and deploying: https://github.com/ae9is/anything/tree/main/packages/infra/data

A lot was changed, make sure to review and test to see that it does what you want!
