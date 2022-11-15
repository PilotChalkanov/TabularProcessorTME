# TabularProcessorTME

The API is meant to be used from Azure Data Factory to orchestrate the processing, merging and partitioning of Analysis Service Tabular Models.
The entry point of the app are the functions, binded to the corresponding endpoints, which execute the logic behind. The main abstract class is 
called Processor, which has all the main functionalities shared among all the tabular cube models. 
The SrsProcessor inherits and extends the Processor class and adds the customized method MergeTables, which executes incremental processing of the new data
which is pushed from the datalake. It uses config table from the database - DQ.PartitionConfigurator, for getting the mQuery expression, in order to build the
new partition and to store the current max primary key - msgId, from the DWH table, which is feeding the Tabular model.
What the method does, it is merging the cold data partition with the hot processed partition, than creates a new daily partition as a hot and process it, until
next day when it repeats the same procedure.
The VhfProcessor inherits and extends the abstract class Processor and add functionallity for creating all needed partitions of the fact table, 
partitioned by id key with granularity of 5 million records each. It reads the config data for every single partition from config table in the 
db - DQ.VhfPartitionConfig. 

## Run the app

It can be run on the localhost for test purposes:
http://localhost:7071/api
Or inside azure data factory
