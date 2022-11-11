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

      
        public  IActionResult ProcessDimTables(CubeModel data)
        {
            aasCnn.ConnectAAS();
            log.LogInformation("Connection established successfully.\n");
            log.LogInformation("Server name:\t\t{0}", aasCnn.Name);

            try
            {
                Database tabularModel = aasCnn.Databases.FindByName(data.TabularModelName.ToString());
                string[] dimTables = data.DimTables;

                if (tabularModel != null)
                {
                    TableCollection modelTables = tabularModel.Model.Tables;
                    Partition partitionToProcess = tabularModel.Model.Tables.Find(data.TableName).Partitions.Find(data.Partition);

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
    

        public abstract IActionResult MergeTables(CubeModel data);

        public abstract IActionResult CreatePartitions(CubeModel data);

        public abstract IActionResult ProcessPartition(CubeModel data);


    }
}
