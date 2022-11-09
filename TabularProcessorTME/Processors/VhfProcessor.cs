using AutomatedProcessingDataQuality.Models;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TabularProcessorTME.Processors
{
    public class VhfProcessor
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
                        if(partitionToProcess != null)
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
    }
}
