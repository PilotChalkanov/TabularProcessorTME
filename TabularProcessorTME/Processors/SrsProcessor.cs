using AutomatedProcessingDataQuality.Models;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using processAAS.StaticText;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using TabularProcessorTME.Helpers;
using TabularProcessorTME.Models;
using TabularProcessorTME.Processors.Contracts;

namespace processAAS
{
    public class SrsProcessor : Processor
    {
        /// <summary>
        ///  Handle all the processing, merging and partitioning of SRS Tabular DB. Extends Abstract Class Processor 
        /// </summary>
        /// <param name="sqlCnn"></param>
        /// <param name="aasCnn"></param>
        /// <param name="log"></param>
        public SrsProcessor(SqlConnection sqlCnn, AnalysisServer aasCnn, ILogger log) : base(sqlCnn, aasCnn, log)
        {

        }

        /// <summary>
        /// Merges the cold partition with the previous day partition. Creates new partition for the current day - daily granularity.
        /// </summary>
        /// <param name="cube">CubModel created based on the req body</param>
        /// <returns></returns>
        public override IActionResult MergeTables(CubeModel cube)

        {

            if (cube is null)
            {
                throw new ArgumentNullException(nameof(cube));
            }

            // Query - DQ.Partitionconfigurator to get the last max msgID
            string lastMaxMsgIdQuery = StaticTextData.lastMaxMsgId;
            string msgId = ReadConfigDetails(lastMaxMsgIdQuery, "currentMaxKey");

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
            if (tabularModel == null)
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
            PartitionManager.Merge(tabularModel, partitionsToMerge, mergeSourcePartition, name, coldPartitionSourceQuery);

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

        
        /// <summary>
        /// Read the config params from the config tables in the sql db
        /// </summary>
        /// <param name="query"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
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

        /// <summary>
        /// Updates the config params in the config tables in the sql db
        /// </summary>
        /// <param name="query"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
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

