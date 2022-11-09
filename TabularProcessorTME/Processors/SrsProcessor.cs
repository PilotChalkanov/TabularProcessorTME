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
using TabularProcessorTME.Processors;

namespace processAAS
{
    public class SrsProcessor : IProcessor
    {
        public IActionResult ProcessDimTables(string tempConnectionString, string sqlConnectionString, ILogger log, CubeModel data)
        {
            if (string.IsNullOrWhiteSpace(tempConnectionString))
            {
                throw new ArgumentException($"'{nameof(tempConnectionString)}' cannot be null or whitespace.", nameof(tempConnectionString));
            }

            if (string.IsNullOrWhiteSpace(sqlConnectionString))
            {
                throw new ArgumentException($"'{nameof(sqlConnectionString)}' cannot be null or whitespace.", nameof(sqlConnectionString));
            }

            if (log is null)
            {
                throw new ArgumentNullException(nameof(log));
            }

            if (data is null)
            {
                throw new ArgumentNullException(nameof(data));
            }
            using (Server server = new Server())
            {
                server.Connect(tempConnectionString);
                log.LogInformation("Connection established successfully.\n");
                log.LogInformation("Server name:\t\t{0}", server.Name);

                try
                {
                    Database tabularModel = server.Databases.FindByName(data.TabularModelName.ToString());
                    string[] dimTables = data.DimTables;
                    string proccessPolicy = data.ProcessType;

                    if (tabularModel != null)
                    {
                        TableCollection modelTables = tabularModel.Model.Tables;
                        Partition partitionToProcess = tabularModel.Model.Tables.Find("VHF").Partitions.Find(data.Partition);

                        if (dimTables.Any())
                        {
                            List<Table> dimTablesToProcess = modelTables.ToList().Where(t => dimTables.Contains(t.Name)).ToList();
                            if (dimTablesToProcess.Any())
                            {
                                dimTablesToProcess.ForEach(t => t.RequestRefresh(RefreshType.ClearValues));
                                tabularModel.Model.SaveChanges();
                                dimTablesToProcess.ForEach(t => log.LogInformation($"{t.Name} -- state -- " +
                                    $"{t.Partitions.First().State} -- modTime -- {t.Partitions.First().ModifiedTime}"));
                                dimTablesToProcess.ForEach(t => t.RequestRefresh(RefreshType.Full));
                                tabularModel.Model.SaveChanges();
                                dimTablesToProcess.ForEach(t => log.LogInformation($"{t.Name} -- state -- " +
                                    $"{t.Partitions.First().State} -- modTime -- {t.Partitions.First().ModifiedTime}"));

                            }
                            else
                            {
                                log.LogError("Dimension Tables are not found in the server or the input is not correct!");
                                return new ObjectResult("Dimension Tables are not found in the server or the input is not correct!")
                                {
                                    StatusCode = (int?)System.Net.HttpStatusCode.NotFound
                                };
                            }
                        }
                        if (partitionToProcess != null)
                        {
                            partitionToProcess.RequestRefresh(RefreshType.ClearValues);
                            partitionToProcess.RequestRefresh(RefreshType.Full);
                        }
                    }
                    else
                    {
                        log.LogInformation("TabularModel not found!");
                        return new ObjectResult("TabularModel not found!")
                        {
                            StatusCode = (int?)System.Net.HttpStatusCode.NotFound
                        };
                    }
                    return new OkResult();

                }
                catch
                {
                    log.LogError("Invalid Request!");
                    return new ObjectResult("Invalid Request!")
                    {
                        StatusCode = (int?)System.Net.HttpStatusCode.BadRequest
                    };
                }



            }
        }
        public IActionResult MergeTables(string aasConnectionString, string sqlConnectionString, ILogger log, CubeModel cube)

        {
            if (string.IsNullOrWhiteSpace(aasConnectionString))
            {
                throw new ArgumentException($"'{nameof(aasConnectionString)}' cannot be null or whitespace.", nameof(aasConnectionString));
            }

            if (string.IsNullOrWhiteSpace(sqlConnectionString))
            {
                throw new ArgumentException($"'{nameof(sqlConnectionString)}' cannot be null or whitespace.", nameof(sqlConnectionString));
            }

            if (log is null)
            {
                throw new ArgumentNullException(nameof(log));
            }

            if (cube is null)
            {
                throw new ArgumentNullException(nameof(cube));
            }

            
            // Query - DQ.Partitionconfigurator to get the last max msgID
            string lastMaxMsgIdQuery = StaticTextData.lastMaxMsgId;
            string msgId = ReadConfigDetails(lastMaxMsgIdQuery,sqlConnectionString, "currentMaxKey");            

            // Query - DQ.Partitionconfigurator to get the current max msgID
            string currentMaxMsgId = StaticTextData.srsMaxMsgID;
            string maxMsgId = ReadConfigDetails(currentMaxMsgId, sqlConnectionString, "msgID");           

            //Query DQ.PartitionConfigurator to get the mQuery template
            string templateQuery = StaticTextData.TemplateQuery;
            string mQueryExpr = ReadConfigDetails(templateQuery, sqlConnectionString, "TemplateSourceQuery");

            Server server = new Server();
            server.Connect(aasConnectionString);
            log.LogInformation("Connection established successfully.\n");
            log.LogInformation("Server name:\t\t{0}", server.Name);

            Database tabularModel = server.Databases.FindByName(cube.TabularModelName.ToString());
            Table partitionedSRS = tabularModel.Model.Tables.Find("SRS");
            if (partitionedSRS == null)
            {
                throw new ArgumentNullException("Srs table name input error!");
            }
            /// <summary>
            /// Create new cold partition - daily.
            /// </summary>                
            List<Partition> partitionsToMerge = partitionedSRS.Partitions.ToList();
            Partition mergeSourcePartition = partitionsToMerge.First();
            partitionsToMerge.Remove(mergeSourcePartition);
            string name = "20220317" + "_" + DateTime.Now.ToString("yyyyMMdd");
            MPartitionSource coldPartitionSourceQuery = new MPartitionSource();
            coldPartitionSourceQuery.Expression = PartitionManager.MQueryBuilder(mQueryExpr, "0", msgId);
            PartitionManager.Merge(tabularModel, partitionedSRS, partitionsToMerge, mergeSourcePartition, name, coldPartitionSourceQuery);

            /// <summary>
            /// Create new hot partition - daily.
            /// </summary>
            Partition hotPartition = new Partition();
            string partName = DateTime.Now.ToString("yyyyMMdd");
            hotPartition.Name = partName;
            MPartitionSource newSource = new MPartitionSource();
            newSource.Expression = PartitionManager.MQueryBuilder(mQueryExpr, msgId, maxMsgId);
            hotPartition.Source = newSource;
            partitionedSRS.Partitions.Add(hotPartition);
            hotPartition.RequestRefresh(RefreshType.Full);
            tabularModel.Model.SaveChanges();
            server.Disconnect();

            //update maxID in PartitionConfig Table
            string updateQuery = StaticTextData.updateMaxId;
            WriteConfigDetails(updateQuery, sqlConnectionString, maxMsgId, log);

            return new ObjectResult($"Created merged partitions:\ncold - {mergeSourcePartition.Name} - msgI > 0 and msgId <= {msgId}" +
                $"                     \nhot - {hotPartition.Name} - msgId > {msgId} and msgId < {maxMsgId}");                
           
        }

        public IActionResult CreatePartitions()
        {
            return new OkResult();
        }

        //read config params from config tables
        string ReadConfigDetails(string query, string sqlConnectionString, string columnName)
        {
            SqlConnection cnn;
            cnn = new SqlConnection(sqlConnectionString);
            SqlDataReader dataReader;
            // Query - DQ.Partitionconfigurator to get the last max msgID            
            string result = "";
            cnn.Open();
            SqlCommand command = new SqlCommand(query, cnn);
            dataReader = command.ExecuteReader();
            while (dataReader.Read())
            {
                string queryResult = Convert.ToString(dataReader[columnName]);                
                result += queryResult;
            }
            cnn.Close();
            return result;
        }

        public IActionResult CreatePartitions(string connectionString, string sqlConnectionString, ILogger log, CubeModel data)
        {
            throw new NotImplementedException();
        }

        public IActionResult ProcessPartition(string connectionString, string sqlConnectionString, ILogger log, CubeModel data)
        {
            throw new NotImplementedException();
        }

        private void WriteConfigDetails(string query, string sqlConnectionString, string maxId, ILogger log)
        {
            SqlConnection cnn;
            cnn = new SqlConnection(sqlConnectionString);                    
            cnn.Open();
            SqlCommand command = new SqlCommand(query, cnn);
            command.Parameters.AddWithValue("@value", maxId);
            int result = command.ExecuteNonQuery();

            // Check Error
            if (result < 0)
            {
                log.LogInformation("Error inserting data into Database!");
                throw new Exception("Error inserting data into Database!");
            }
            cnn.Close();
        
        
        }

        
    }
}

