using AutomatedProcessingDataQuality.Models;
using processAAS.StaticText;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using TabularProcessorTME.Helpers;
using TabularProcessorTME.Processors.Contracts;
using TabularProcessorTME.Models;

namespace processAAS
{
    public class SrsProcessor : Processor
    {

        public SrsProcessor(SqlConnection sqlCnn, AnalysisServer aasCnn, ILogger log) : base(sqlCnn, aasCnn, log)
        {

        }
        public override IActionResult MergeTables(CubeModel cube)

        {

            if (cube is null)
            {
                throw new ArgumentNullException(nameof(cube));
            }


            // Query - DQ.Partitionconfigurator to get the last max msgID
            string lastMaxMsgIdQuery = StaticTextData.lastMaxMsgId;
            string msgId = ReadConfigDetails(lastMaxMsgIdQuery,  "currentMaxKey");

            // Query - DQ.Partitionconfigurator to get the current max msgID
            string currentMaxMsgId = StaticTextData.srsMaxMsgID;
            string maxMsgId = ReadConfigDetails(currentMaxMsgId, "msgID");

            //Query DQ.PartitionConfigurator to get the mQuery template
            string templateQuery = StaticTextData.srsTemplateQuery.Replace("{0}", "SRS");
            string mQueryExpr = ReadConfigDetails(templateQuery, "TemplateSourceQuery");

            aasCnn.ConnectAAS();            
            log.LogInformation("Connection established successfully.\n");
            log.LogInformation("Server name:\t\t{0}", aasCnn.Name);

            Database tabularModel = aasCnn.Databases.FindByName(cube.TabularModelName.ToString());
            if(tabularModel == null)
            {
                log.LogInformation("SRS Tabular model doesn't exist or it is not initialized properly!");
                throw new ArgumentNullException("SRS Tabular model doesn't exist or it is not initialized properly!");
            }
            Table partitionedSRS = tabularModel.Model.Tables.Find("SRS");
            if (partitionedSRS == null)
            {
                throw new ArgumentNullException("Srs table name doesn't exist or the input is not correct!");
            }
            /// <summary>
            /// Create new cold partition - daily.
            /// </summary>                
            List<Partition> partitionsToMerge = partitionedSRS.Partitions.ToList();
            Partition mergeSourcePartition = partitionsToMerge.First();
            partitionsToMerge.Remove(mergeSourcePartition);
            string name = "20220317" + "_" + DateTime.Now.ToString("yyyyMMdd");
            MPartitionSource coldPartitionSourceQuery = new MPartitionSource();
            coldPartitionSourceQuery.Expression = PartitionManager.BuildMQuery(mQueryExpr, "0", msgId);
            PartitionManager.Merge(tabularModel, partitionedSRS, partitionsToMerge, mergeSourcePartition, name, coldPartitionSourceQuery);

            /// <summary>
            /// Create new hot partition - daily.
            /// </summary>
            Partition hotPartition = new Partition();
            string partName = DateTime.Now.ToString("yyyyMMdd");
            hotPartition.Name = partName;
            MPartitionSource newSource = new MPartitionSource();
            newSource.Expression = PartitionManager.BuildMQuery(mQueryExpr, msgId, maxMsgId);
            hotPartition.Source = newSource;
            partitionedSRS.Partitions.Add(hotPartition);
            hotPartition.RequestRefresh(RefreshType.Full);
            tabularModel.Model.SaveChanges();
            aasCnn.Disconnect();

            //update maxID in PartitionConfig Table
            string updateQuery = StaticTextData.updateMaxId;
            WriteConfigDetails(updateQuery, maxMsgId, log);

            return new ObjectResult($"Created merged partitions:\ncold - {mergeSourcePartition.Name} - msgI > 0 and msgId <= {msgId}" +
                $"                     \nhot - {hotPartition.Name} - msgId > {msgId} and msgId < {maxMsgId}");

        }

        public IActionResult CreatePartitions()
        {
            return new OkResult();
        }

        //read config params from config tables
        string ReadConfigDetails(string query, string columnName)
        {
            
            
            SqlDataReader dataReader;
            // Query - DQ.Partitionconfigurator to get the last max msgID            
            string result = "";
            sqlCnn.Open();
            SqlCommand command = new SqlCommand(query, sqlCnn);
            dataReader = command.ExecuteReader();
            while (dataReader.Read())
            {
                string queryResult = Convert.ToString(dataReader[columnName]);
                result += queryResult;
            }
            sqlCnn.Close();
            return result;
        }

        public override IActionResult CreateAllPartitions(CubeModel data)
        {
            throw new NotImplementedException();
        }
       

        private void WriteConfigDetails(string query, string maxId, ILogger log)
        {           
            sqlCnn.Open();
            SqlCommand command = new SqlCommand(query, sqlCnn);
            command.Parameters.AddWithValue("@value", maxId);
            int result = command.ExecuteNonQuery();

            // Check Error
            if (result < 0)
            {
                log.LogInformation("Error inserting data into Database!");
                throw new Exception("Error inserting data into Database!");
            }
            sqlCnn.Close();


        }
   
    }
}

