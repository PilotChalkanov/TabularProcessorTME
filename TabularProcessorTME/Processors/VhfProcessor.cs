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
using TabularProcessorTME.Helpers;

namespace TabularProcessorTME.Processors
{
    public class VhfProcessor : Processor
    {
        /// <summary>
        /// Handle all the processing, merging and partitioning of VHF Tabular DB. Extends Abstract Class Processor  
        /// </summary>
        /// <param name="sqlCnn"></param>
        /// <param name="aasCnn"></param>
        /// <param name="log"></param>

        public VhfProcessor(SqlConnection sqlCnn, AnalysisServer aasCnn, ILogger log) : base(sqlCnn, aasCnn, log)
        {

        }

        /// <summary>
        /// Deletes and recreates all the partitions in the required table
        /// </summary>
        /// <param name="cube">CubeModel as per req body from the api call</param>
        /// <returns></returns>
        public override IActionResult CreateAllPartitions(CubeModel cube)
        {
            // Get the mQuery template from the config table in the sql db
            string getPartitionInfo = StaticTextData.vhfPartitionsInfo;
            string getTemplateMQuery = StaticTextData.vhfTemplateQuery.Replace("{0}", "'VHF'");
            List<Partition> partitionsToCreate = new List<Partition>();
            SqlDataReader dataReader;
            sqlCnn.Open();
            SqlCommand commandMQuery = new SqlCommand(getTemplateMQuery, sqlCnn);
            dataReader = commandMQuery.ExecuteReader();
            string mQuery = null;
            while (dataReader.Read())
            {
                mQuery = dataReader[0].ToString();
            }
            if (mQuery == null)
            {
                throw new ArgumentNullException("Invalid MQuery!");
            }
            dataReader.Close();

            //add suffix "_delete" to old partitions
            aasCnn.ConnectAAS();
            Database vhfTabularModel = aasCnn.Databases.FindByName(cube.TabularModelName);
            Table vhfTable = vhfTabularModel.Model.Tables.Find(cube.TableName);
            PartitionCollection partitionsToDelete = vhfTable.Partitions;
            partitionsToDelete.ToList().ForEach(p => p.Name = p.Name + "_delete");
            vhfTabularModel.Model.SaveChanges();

            // Get the partitiong properties from VhfPArtitionConfig table - name, min, max value of the partition.
            SqlCommand commandPartitionInfo = new SqlCommand(getPartitionInfo, sqlCnn);
            dataReader = commandPartitionInfo.ExecuteReader();
            List<string> result = new List<string>();
            while (dataReader.Read())
            {
                Partition partition = new Partition();
                string partitionName = dataReader[0].ToString();
                partition.Name = partitionName;
                string partitionLowerBoundary = dataReader[1].ToString();
                string partitionUpperBoundary = dataReader[2].ToString();
                MPartitionSource newSource = new MPartitionSource();
                newSource.Expression = PartitionManager.BuildMQuery(mQuery, partitionLowerBoundary, partitionUpperBoundary);
                partition.Source = newSource;
                vhfTable.Partitions.Add(partition);
                string partitionInfo = $"Created partition -- {partitionName} -- range -- {partitionLowerBoundary}:{partitionUpperBoundary}";
                result.Add(partitionInfo);
                vhfTabularModel.Model.SaveChanges();

            }
            sqlCnn.Close();
            try
            {
                //deletes the required partitions
                foreach (Partition p in partitionsToDelete)
                {
                    if (p.Name.Contains("delete"))
                    {
                        partitionsToDelete.Remove(p.Name);
                        log.LogInformation($"{p.Name} - Deleted");
                        vhfTabularModel.Model.SaveChanges();
                    }
                }
                aasCnn.Disconnect();
            }
            catch (Exception e)
            {
                log.LogInformation(e.ToString());
                aasCnn.Disconnect();
            }

            log.LogInformation($"Succesfully partitioned -  table - {cube.TableName}:\n{String.Join("\n", result)}");

            return new ObjectResult($"Succesfully partitioned -  table {cube.TableName}:\n{String.Join("\n", result)}")
            {
                StatusCode = (int?)System.Net.HttpStatusCode.OK
            };
        }


        public override IActionResult MergeTables(CubeModel data)
        {
            throw new NotImplementedException();
        }

        public string WriteConfigDetails(string query, string sqlConnectionString, string columnName)
        {
            throw new NotImplementedException();
        }


    }
}

