# df-cassandra-connector

The Cassandra Connector is an Actian DataFlow operator for querying and updating [Apache Cassandra](http://cassandra.apache.org/) databases.

## Configuration

Before building df-cassandra-connector you need to define the following environment variables to point to the local DataFlow update site [dataflow-p2-site](https://github.com/ActianCorp/dataflow-p2-site) root directory and the DataFlow version.

    export DATAFLOW_REPO_HOME=/Users/myuser/dataflow-p2-site
    export DATAFLOW_VER=6.5.0.117

## Building

The update site is built using [Apache Maven 3.0.5 or later](http://maven.apache.org/).

To build, run:

    mvn clean install
    
You can update the version number by running

    mvn org.eclipse.tycho:tycho-versions-plugin:set-version -DnewVersion=version
    
where version is of the form x.y.z or x.y.z-SNAPSHOT.

## Using the Cassandra Connector with the DataFlow Engine

The build generates a JAR file in the target directory under
[df-cassandra-connector/cassandra-connector-op](https://github.com/ActianCorp/df-cassandra-connector/tree/master/cassandra-op)
with a name similar to 

    cassandra-connector-op-1.y.z.jar

which can be included on the classpath when using the DataFlow engine.

## Installing the Cassandra Connector plug-in in KNIME

The build also produces a ZIP file which can be used as an archive file with the KNIME 'Help/Install New Software...' dialog.
The ZIP file can be found in the target directory under
[df-cassandra-connector/cassandra-connector-ui-top/update-site](https://github.com/ActianCorp/df-cassandra-connector/tree/master/cassandra-ui-top/update-site) 
and with a name like 


    com.actian.ilabs.dataflow.cassandra.ui.update-1.y.z.zip
 
## Limitations

The current implementation has a couple of limitations:

* The Cassandra Writer expects the keyspace and table it updates to exist beforehand.
* The Cassandra Writer currently has no support for the inet, uuid, timeuuid, list, map, or set data types.
* Query parameters are bound by position only.





