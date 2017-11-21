# OSM Change Stream Analysis


[Open street map](http://www.openstreetmap.org/) is open data project which
map data is contributed by users all over the world.

It provides a replication data stream where all user changes are aggregated
and published periodically. (See http://wiki.openstreetmap.org/wiki/Planet.osm/diffs 
for publish address and data format).

This project demostrates how to do simple stream analysis on OSC stream using 
AWS services like Lambda, Kinesis Analytics, and manage resources using 
CodePipeline and ServerlessApplicationModel.

# Design

![Architecture](assets/architect-v1-fs8.png)

1. Deploy CloudFormation CD template.
2. Commit SAM enabled template and Lambda code to CodeCommit repository.
3. CodePipeline packages the template and resources and upload them to artifact bucket, then automaticlly deploys the inner stack.
4. Check OSC state from planet osm site.
5. Write state and download URL to DynamoDB table.
6. DynamoDB steaming triggers OSC parsing lambda function.
7. Parser downloads OSC file, parse OSC XML on-the-fly.
8. Write result to Firehose as JSON records.
9. Firehose write aggrated and compressed records to S3.
10. Firehose streaming records to Kinesis Analytics.
11. Kinesis Analytics publish steaming analytics results to Kinesis Stream.
12. Kinesis stream triggers lambda function to publish analytics results.
13. Analytics results are written to CloudWatch as custom metrics.
14. CloudWatch Dashboard collects and display metrics.

Here's the dashboard showing changes of last day:

![Dashboard](assets/dashboard-v1-fs8.png)

