# recordservice-presto

RecordService connector for [prestodb](http://prestodb.io).

## build and installation

```
$ mvn package
$ mv target/recordservice-presto-<version>.jar path/to/presto/plugin/recordservice
```

## configuration


```
$ cat recordservice.properies
connector.name=recordservice

recordservice.planner.hostports=<PLANNER_HOST>

$ cp recordservice.properties path/to/presto/etc/catalog
$ ./presto --server localhost:8080 --catalog recordservice --schema t
or, simply
$ ./presto
```

## configuration in emr
```
# First copy the jar to the plugin folder
$ cd /usr/lib/presto/plugin
$ mkdir recordservice/
$ cp <presto-recordservice.jar> recordservice/

# Configure presto to pick up the recordservice connector
$ cd /etc/presto/conf/catalog

echo "connector.name=recordservice
recordservice.planner.hostports=<HOST:PORT>" > catalog/recordservice.properties

# Restart the server
sudo bin/launcher restart
```

## Using it in presto-cli
```
presto:t> show catalogs;
 Catalog
 ---------
  jmx
  recordservice
 (2 rows)
 presto:t> use recordservice.t;
 presto:t> show schemas;
# To show available commands
 presto> help
