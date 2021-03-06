## Sample data

The sample data is Airline on-time performance data from the [U.S. Department of Transportation (DOT) Bureau of Transportation Statistics (BTS)](http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236).

* [419234754_T_ONTIME_2015_1.csv](https://raw.githubusercontent.com/ActianCorp/df-cassandra-connector/master/examples/419234754_T_ONTIME_2015_1.csv) Data for the month of 1/2015
* [419234754_T_ONTIME_ReadMe.csv]() Description of columns in the data
* [419234754_T_ONTIME_Terms.csv]()  Definition of terms used in column names

## Preparing Cassandra to run the examples

All of the examples are configured to use a local Cassandra instance at **localhost**.  If you use a remote instance 
you will need to reconfigure the examples before running them.

To run the examples you will need to create a table called *ontime* in a keyspace called *airline* using the following
CQL statements.   You can copy and paste these statements into a cqlsh console session to create the keyspace and table.

```SQL

    create keyspace airline with replication = {'class' : 'SimpleStrategy','replication_factor' : 1};

    create table airline.ontime (
        FL_DATE timestamp,
        UNIQUE_CARRIER text,
        FL_NUM int,
        ORIGIN_AIRPORT_ID int,
        ORIGIN_AIRPORT_SEQ_ID int,
        ORIGIN_CITY_MARKET_ID int,
        DEST_AIRPORT_ID int,
        DEST_AIRPORT_SEQ_ID int,
        DEST_CITY_MARKET_ID int,
        CRS_DEP_TIME int,
        DEP_TIME int,
        DEP_DELAY double,
        CRS_ARR_TIME int,
        ARR_TIME int,
        ARR_DELAY double,
        CRS_ELAPSED_TIME double,
        ACTUAL_ELAPSED_TIME double,
        PRIMARY KEY (FL_DATE, UNIQUE_CARRIER, FL_NUM, CRS_DEP_TIME)
        )
        WITH comment='Airline on-time performance data';
```

## Cassandra_Load_Airline_Data example

The load airline data example in [KNIME/Cassandra_Load_Airline_Data.zip](https://github.com/ActianCorp/df-cassandra-connector/raw/master/examples/KNIME/Cassandra_Load_Airline_Data.zip) is a simple KNIME workflow that loads the [airline on-time performance data](https://raw.githubusercontent.com/ActianCorp/df-cassandra-connector/master/examples/419234754_T_ONTIME_2015_1.csv) into the Cassandra table airline.ontime.

The CassandraWriter node in the workflow is configured to use the following parameterized CQL INSERT statement to load the data.

```SQL

    insert into airline.ontime (
        FL_DATE,
        UNIQUE_CARRIER,
        FL_NUM,
        ORIGIN_AIRPORT_ID,
        ORIGIN_AIRPORT_SEQ_ID,
        ORIGIN_CITY_MARKET_ID,
        DEST_AIRPORT_ID,
        DEST_AIRPORT_SEQ_ID,
        DEST_CITY_MARKET_ID,
        CRS_DEP_TIME,
        DEP_TIME,
        DEP_DELAY,
        CRS_ARR_TIME,
        ARR_TIME,
        ARR_DELAY,
        CRS_ELAPSED_TIME,
        ACTUAL_ELAPSED_TIME)
        values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
```

## Cassandra_Query_Airline_Data example

Once data has been loaded into the airline.ontime table you can use the query airline data example in [KNIME/Cassandra_Query_Airline_Data.zip](https://github.com/ActianCorp/df-cassandra-connector/raw/master/examples/KNIME/Cassandra_Query_Airline_Data.zip) query data from Cassandra.

For this example the CassandraReader node in the workflow is configured to execute the following CQL query

```SQL

    select * from airline.ontime where unique_carrier = 'US' limit 200 allow filtering;
```