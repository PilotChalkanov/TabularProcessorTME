using AutomatedProcessingDataQuality.Models;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;

namespace TabularProcessorTME.Processors
{
    public interface IProcessor
    {
        public IActionResult ProcessDimTables(string connectionString, string sqlConnectionString, ILogger log, CubeModel data);
        public IActionResult MergeTables(string connectionString, string sqlConnectionString, ILogger log, CubeModel data);

        public IActionResult CreatePartitions(string connectionString, string sqlConnectionString, ILogger log, CubeModel data);

        public IActionResult ProcessPartition(string connectionString, string sqlConnectionString, ILogger log, CubeModel data);

                
    }
}
