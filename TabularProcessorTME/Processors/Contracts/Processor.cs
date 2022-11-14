using AutomatedProcessingDataQuality.Models;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using TabularProcessorTME.Models;

namespace TabularProcessorTME.Processors.Contracts
{
    /// <summary>
    /// The main abstract Processor class with the common methods for all db instances
    /// </summary>
    public abstract class Processor
    {
        private SqlConnection _sqlCnn;
        private AnalysisServer _aasCnn;

        public SqlConnection sqlCnn
        {
            get { return _sqlCnn; }
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{nameof(sqlCnn)}' cannot be null or whitespace.", nameof(sqlCnn));
                }
                _sqlCnn = value;
            }
        }

        public AnalysisServer aasCnn
        {
            get { return _aasCnn; }
            set
            {
                if (value == null)
                {
                    throw new ArgumentException($"'{nameof(aasCnn)}' cannot be null or whitespace.", nameof(aasCnn));
                }
                _aasCnn = value;
            }
        }

        public readonly ILogger log;


        public Processor(SqlConnection sqlCnn, AnalysisServer aasCnn, ILogger _log)
        {
            this.sqlCnn = sqlCnn;
            this.aasCnn = aasCnn;
            this.log = _log;
        }
        /// <summary>
        /// Processes all the dimension tables in a tabular db
        /// </summary>
        /// <param name="cube"> CubeModel created from the req body. </param>
        /// <returns></returns>        
        public IActionResult ProcessDimTables(CubeModel cube)
        {
            aasCnn.ConnectAAS();
            log.LogInformation("Connection established successfully.\n");
            log.LogInformation("Server name:\t\t{0}", aasCnn.Name);

            try
            {
                Database tabularModel = aasCnn.Databases.FindByName(cube.TabularModelName.ToString());
                string[] dimTables = cube.DimTables;

                if (tabularModel != null)
                {
                    TableCollection modelTables = tabularModel.Model.Tables;
                    Partition partitionToProcess = tabularModel.Model.Tables.Find(cube.TableName).Partitions.Find(cube.Partition);

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
                            aasCnn.Disconnect();
                            aasCnn.Dispose();

                            dimTablesToProcess.ForEach(t => log.LogInformation($"{t.Name} -- state -- " +
                                $"{t.Partitions.First().State} -- modTime -- {t.Partitions.First().ModifiedTime}"));

                        }
                        else
                        {
                            log.LogInformation("Dimension Tables are not found in the server or the input is not correct!");
                            return new ObjectResult("Dimension Tables are not found in the server or the input is not correct!")
                            {
                                StatusCode = (int?)System.Net.HttpStatusCode.NotFound
                            };
                        }
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
                return new ObjectResult("Succesfully processed tables:")
                {
                    StatusCode = (int?)System.Net.HttpStatusCode.OK
                };

            }
            catch
            {
                log.LogInformation("Invalid Request - Check the name of the dimension tables!");
                return new ObjectResult("Invalid Request - Check the name of the dimension tables!")
                {
                    StatusCode = (int?)System.Net.HttpStatusCode.BadRequest
                };
            }



        }

        /// <summary>
        /// Processes single partition in the tabular db
        /// </summary>
        /// <param name="cube"> CubeModel created from the req body.</param>
        /// <returns></returns>
        public IActionResult ProcessPartition(CubeModel cube)
        {
            aasCnn.ConnectAAS();
            Database vhfTabularModel = aasCnn.Databases.FindByName(cube.TabularModelName);
            Table partitionedTable = vhfTabularModel.Model.Tables.Find(cube.TableName);
            Partition partitionToProcess = partitionedTable.Partitions.Find(cube.Partition);
            if (partitionToProcess == null)
            {
                log.LogInformation($"Partition - {cube.Partition} was not found. Check if the partition name is correct.");
                return new ObjectResult($"Partition - {cube.Partition} was not found. Check if the partition name is correct.")
                {
                    StatusCode = (int?)System.Net.HttpStatusCode.BadRequest
                };

                throw new ArgumentNullException($"Partition - {cube.Partition} was not found. Check if the partition name is correct.");
            }

            partitionToProcess.RequestRefresh(RefreshType.ClearValues);
            vhfTabularModel.Model.SaveChanges();
            log.LogInformation($"Table: {partitionedTable}, Partition: {partitionToProcess.Name} -- " +
                $"state -- {partitionToProcess.State} -- modTime -- {partitionToProcess.ModifiedTime}");

            partitionToProcess.RequestRefresh(RefreshType.Full);
            vhfTabularModel.Model.SaveChanges();
            log.LogInformation($"Table: {partitionedTable}, Partition: {partitionToProcess.Name} -- " +
                $"state -- {partitionToProcess.State} -- modTime -- {partitionToProcess.ModifiedTime}");
            aasCnn.Disconnect();

            return new ObjectResult($"Succesfully processed partition - {partitionedTable.Name}: {partitionToProcess.Name}.")
            {
                StatusCode = (int?)System.Net.HttpStatusCode.OK
            };
        }

        /// <summary>
        /// To be implemented by all the processors of the specific tabular db
        /// Merges the tables based on the business requirments of the data
        /// </summary>
        /// <param name="cube">CubeModel as per req body from the api call</param>
        /// <returns></returns>
        public abstract IActionResult MergeTables(CubeModel cube);
        
        public abstract IActionResult CreateAllPartitions(CubeModel cube);




    }
}
