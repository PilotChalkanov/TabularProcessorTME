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
Or inside azure data factory pipeline as Azure Function

Also as a web activity with the proper endpoint address.
https://tabularprocessortme.azurewebsites.net/api/ + functionKey

The endpoint https://tabularprocessortme.azurewebsites.net/api/ will be referred as /api for the rest of the documentation.


## Process Dimension Tables - VHF, SRS
###/api/vhf/dimensions
###/api/srs/dimensions
req body
{    
    "TabularModelName": "DataQuality_VHF",
    "TableName": "VHF",
    "DimTables": ["Channel mapping", "A2D Dealers", "EventDate"],   
    "Partition":"",
    "ProcessType" :"1"

}

Processes all the tables in the "DimTables" as an array of strings. If the names are correct it will process the corresponding tables, otherwise it will throw an error. Here "Partition" and "ProcessType" can be omitted from the req body. The logic is the same for VHF and SRS.


## Creates all the partitions - VHF
### /api/vhf/partitions/create

req body
{    
    "TabularModelName": "DataQuality_VHF",
    "TableName": "VHF",  
    "ProcessType" :"1"
}

It is meant to be run only ones, when we need to partition the VHF table.
Deletes all the partitions inside VHF table and creates new ones according to the configuration stipulated in the DQ.VhfPartitionConfig table, using the mQuery from DQ.PartitionConfigurator for TableName VHF.


## Process Single Partition - VHF, SRS

###/api/vhf/partitions
###/api/srs/single_partition

req body 

{    
    "TabularModelName": "DataQuality_VHF",
    "TableName": "VHF",     
    "Partition":"VHF40",
    "ProcessType" :"1"
}

Processes a single partition - sent in the body as string.


## Merge and process SRS
### /api/srs/merge_process

{    
    "TabularModelName": "DataQualitySRS_PRD",
    "TableName": "SRS",
    "DimTables": [],   
    "Partition":"",
    "ProcessType" :"1"

}

Merges the current hot partition with the cold historical partition. Then creates the new hot partition with the current date. It should run daily,
ensuring daily granularity of the report.


## Create custom partition
### api/custom_partition

{    
    "TabularModelName": "DataQuality_VHF",
    "TableName": "VHF",    
    "Partition":"custom",
    "PartitionQuery" :"let\r\n    Source = #\"SQL\/kpireportingsqldev database windows net,1433;PanEToshiko\",\r\n    SRSe2e_SRS_MIDDLEWARE_MSG_NEW = Source{[Schema=\"SRSe2e\",Item=\"SRS_MIDDLEWARE_MSG_NEW\"]}[Data],\r\n    #\"Renamed Columns\" = Table.RenameColumns(SRSe2e_SRS_MIDDLEWARE_MSG_NEW,{{\"eventId\", \"EVENT ID\"}, {\"dueDate\", \"DUE DATE\"}, {\"processedAt\", \"PROCESSED AT\"}}),\r\n    #\"Added Custom\" = Table.AddColumn(#\"Renamed Columns\", \"PREFERRED DEALER CODE?\", each if [preferredDealerId] = null then \"no\" else \"yes\"),\r\n    #\"Renamed Columns1\" = Table.RenameColumns(#\"Added Custom\",{{\"deliveryCountry\", \"DELIVERY COUNTRY UIO\"}, {\"licensePlateCountry\", \"LICENSE PLATE COUNTRY UIO\"}, {\"eventSource\", \"EVENT SOURCE\"}, {\"brand\", \"BRAND\"}, {\"errorCase\", \"ERROR CASE\"}, {\"nmscCode\", \"NMSC\"}}),\r\n    #\"Removed Columns2\" = Table.RemoveColumns(#\"Renamed Columns1\",{\"preferredDealerId\"}),\r\n    #\"Added Custom1\" = Table.AddColumn(#\"Removed Columns2\", \"DUE DATE SENT\", each if [dueDatePublished] = 1 then \"yes\" else \"no\"),\r\n    #\"Changed Type\" = Table.TransformColumnTypes(#\"Added Custom1\",{{\"DUE DATE\", type datetime}}),\r\n    #\"Changed Type1\" = Table.TransformColumnTypes(#\"Changed Type\",{{\"DUE DATE\", type date}}),\r\n    #\"Changed Type2\" = Table.TransformColumnTypes(#\"Changed Type1\",{{\"PROCESSED AT\", type datetime}}),\r\n    #\"Changed Type3\" = Table.TransformColumnTypes(#\"Changed Type2\",{{\"PROCESSED AT\", type date}}),\r\n    #\"Sorted Rows\" = Table.Sort(#\"Changed Type3\",{{\"msgID\", Order.Ascending}}),\r\n    #\"Changed Type4\" = Table.TransformColumnTypes(#\"Sorted Rows\",{{\"eventDealerCodeIsBad\", type text}}),\r\n    #\"Replaced Value\" = Table.ReplaceValue(#\"Changed Type4\",\"0\",\"No\",Replacer.ReplaceText,{\"eventDealerCodeIsBad\"}),\r\n    #\"Replaced Value1\" = Table.ReplaceValue(#\"Replaced Value\",\"1\",\"Yes\",Replacer.ReplaceText,{\"eventDealerCodeIsBad\"}),\r\n    #\"Filtered Rows\" = Table.SelectRows(#\"Replaced Value1\", each [msgID] > {0} and [msgID] <= {1})\r\nin\r\n    #\"Filtered Rows\""

}

Creates a partition with the mQuery send in the req. Valid for all tabular models.

