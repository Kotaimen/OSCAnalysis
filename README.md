# OSM Change Stream Analysis


[Open street map](http://www.openstreetmap.org/) is open data project which
map data is contributed by users all over the world.

It provides a replication data stream where all user changes are aggregated
and published periodically. (See [this OSM wiki](http://wiki.openstreetmap.org/wiki/Planet.osm/diffs) 
for data url and format specfication).

## NOTE

At the time of writing following features are not in Cloudformation

- Kinesis Analytics application is in stopped state when created (start manually)
- Kinesis Analytics does not support lambda (transform and output)
- Kinesis stream does not support set SSE (set via CLI or console)
