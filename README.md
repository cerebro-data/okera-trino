# okera-trino

Okera connector for [Trino](http://trino.io).

## Building the JAR

First, install the required core library from the matching release into the local Maven repository:

```
$ cd okera-trino
$ aws s3 cp s3://okera-release-uswest/2.12.0/client/recordservice-core-2.12.0.jar recordservice-core-2.12.0-SNAPSHOT.jar
$ mvn install:install-file -Dfile=recordservice-core-2.12.0-SNAPSHOT.jar -DgroupId=com.okera.recordservice -DartifactId=recordservice-core -Dversion=2.12.0-SNAPSHOT -Dpackaging=jar
```

Then use Maven to package the JAR:

```
$ mvn package
```

## Local Installation and Configuration

Download and unpack Trino:

```
$ cd ~
$ curl -O https://repo1.maven.org/maven2/io/trino/trino-server/400/trino-server-400.tar.gz
$ tar -zxvf trino-server-400.tar.gz
$ cd trino-server-400
```

Copy the JAR into the Trino directory.
Set `$PROJECT_HOME` to where you build the connector above:

```
$ mkdir plugin/okera
$ mv $PROJECT_HOME/target/recordservice-trino-<version>.jar plugin/okera/
```

Create a connector properties file in the proper location:

```
$ cat okera.properties
connector.name=okera
okera.planner.hostports=<PLANNER_HOST>

$ cp okera.properties etc/catalog
```

Start Trino:

```
$ bin/launcher run
```

## Configuration in EMR

```
# First copy the JAR to the plugin folder
$ cd /usr/lib/trino/plugin
$ mkdir okera/
$ cp <recordservice-trino.jar> okera/

# Configure Trino to pick up the okera connector
$ cd /etc/trino/conf/catalog

echo "connector.name=okera
okera.planner.hostports=<HOST:PORT>" > catalog/okera.properties

# Restart the server
sudo bin/launcher restart
```

## Using it in trino-cli

```
presto:t> show catalogs;
 Catalog
 ---------
  jmx
  okera
 (2 rows)
 presto:t> use okera.t;
 presto:t> show schemas;
# To show available commands
 presto> help
