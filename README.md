Sequencing Demo
====================

## Scenario

This demo shows how to create a distributed sequence for use in system that need a id system in DataStax Enterprise or with leveled compaction in Cassandra.

## Schema Setupp
Note : This will drop the keyspace "datastax_sequence_demo" and create a new one. All existing data will be lost. 

To specify contact points use the contactPoints command line parameter e.g. '-DcontactPoints=192.168.25.100,192.168.25.101'
The contact points can take mulitple points in the IP,IP,IP (no spaces).

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"

To run the reader

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.sequence.Main"
		
To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
    
    
    
