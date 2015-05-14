## Sample data

The sample data is Airline on-time performance data from the [U.S. Department of Transportation (DOT) Bureau of Transportation Statistics (BTS)](http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236).

* [419234754_T_ONTIME_2015_1.csv]() Data for the month of 1/2015
* [419234754_T_ONTIME_ReadMe.csv]() Description of columns in the data
* [419234754_T_ONTIME_Terms.csv]()  Definition of terms used in column names

## Preparing Cassandra to run the examples

All of the examples are configured to use a local Cassandra instance at localhost:9160.  If you use a remote instance 
you will need to reconfigure the examples before running them.

To run the examples you will need to create a table called *ontime* in a keyspace called *airline* using the following
CQL statements.   You can copy and paste these statements into a cqlsh console session to create the keyspace and table.

    create keyspace airline with replication = {'class' : 'SimpleStrategy','replication_factor' : 1};

    create table airline.ontime (
        FL_DATE text,
        UNIQUE_CARRIER text,
        FL_NUM int,
        ORIGIN_AIRPORT_ID int,
        ORIGIN_AIRPORT_SEQ_ID int,
        ORIGIN_CITY_MARKET_ID int,
        DEST_AIRPORT_ID int,
        DEST_AIRPORT_SEQ_ID int,
        DEST_CITY_MARKET_ID int,
        CRS_DEP_TIME text,
        DEP_TIME text,
        DEP_DELAY int,
        CRS_ARR_TIME text,
        ARR_TIME text,
        ARR_DELAY int,
        CRS_ELAPSED_TIME int,
        ACTUAL_ELAPSED_TIME int,
        PRIMARY KEY (FL_DATE, UNIQUE_CARRIER, FL_NUM, CRS_DEP_TIME)
        )
        WITH comment='Airline on-time performance data';
