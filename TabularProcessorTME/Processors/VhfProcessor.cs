using AutomatedProcessingDataQuality.Models;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using processAAS.StaticText;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using TabularProcessorTME.Processors.Contracts;
using Microsoft.Extensions.Logging;
using TabularProcessorTME.Models;

namespace TabularProcessorTME.Processors
{
    public class VhfProcessor : Processor
    {
        

        public VhfProcessor(SqlConnection sqlCnn, AnalysisServer aasCnn, ILogger log) : base(sqlCnn, aasCnn,log)
        {
            
        }
        

        /// <summary>
        /// Vhf Processing of Dimension Tables 
        /// </summary>
        /// <param name="tempConnectionString"></param>
        /// <param name="sqlConnectionString"></param>
        /// <param name="log"></param>
        /// <param name="data"></param>
        /// <returns></returns>
       

        public override IActionResult CreatePartitions(CubeModel data)
        {
            string getPartitionInfo = StaticTextData.vhfPartitionsInfo;
            string getTemplateMQuery = StaticTextData.vhfTemplateQuery.Replace("{0}", "VHF");
            List<Partition> partitionsToCreate = new List<Partition>();
            SqlDataReader dataReader;
            sqlCnn.Open();
            SqlCommand command = new SqlCommand(getPartitionInfo, sqlCnn);
            dataReader = command.ExecuteReader();
            while (dataReader.Read())
            {
                Partition partition = new Partition();
                partition.Name = dataReader[0].ToString();

                log.LogInformation(dataReader[0].ToString());
                partitionsToCreate.Add(partition);
            }
            sqlCnn.Close();
            

            return new ObjectResult("Succesfully processed - dimension tables.")
            {
                StatusCode = (int?)System.Net.HttpStatusCode.OK
            };
        }

        /// <summary>
        /// Get the partitioning details and adds them to a list of Partitions
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
       
        public override IActionResult MergeTables(CubeModel data)
        {
            throw new NotImplementedException();
        }

        public override IActionResult ProcessPartition(CubeModel cube)
        {
            aasCnn.ConnectAAS();
            Database vhfTabularModel = aasCnn.Databases.Find(cube.TabularModelName);
            Table partitionedTable = vhfTabularModel.Model.Tables.Find(cube.TableName);
            Partition partitionToProcess = partitionedTable.Partitions.Find(cube.Partition);
            partitionToProcess.RequestRefresh(RefreshType.ClearValues);
            vhfTabularModel.Model.SaveChanges();
            log.LogInformation($"Table: {partitionedTable}, Partition: {partitionToProcess.Name} -- " +
                $"state -- {partitionToProcess.State} -- modTime -- {partitionToProcess.ModifiedTime}");
            
            partitionToProcess.RequestRefresh(RefreshType.Full);
            log.LogInformation($"Table: {partitionedTable}, Partition: {partitionToProcess.Name} -- " +
                $"state -- {partitionToProcess.State} -- modTime -- {partitionToProcess.ModifiedTime}");
            vhfTabularModel.Model.SaveChanges();
            aasCnn.Disconnect();
            
            return new ObjectResult($"Succesfully processed partition - {partitionedTable}: {partitionToProcess} .")
            {
                StatusCode = (int?)System.Net.HttpStatusCode.OK
            };
        }
        

        public string WriteConfigDetails(string query, string sqlConnectionString, string columnName)
        {
            throw new NotImplementedException();
        }

        
    }
}

