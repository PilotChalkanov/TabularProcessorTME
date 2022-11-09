using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.AnalysisServices.Tabular;
using System.Linq;
using System.Collections.Generic;
using AutomatedProcessingDataQuality.Models;
using Microsoft.Extensions.Configuration;
using System.Data.SqlClient;
using processAAS;
using processAAS.Models;
using processAAS.Helpers;
using TabularProcessorTME.Processors;

namespace processAAS
{
    public static class ProcessOrchestrator
    {
        [FunctionName("ProcessOrchestrator")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            ConfigDatabaseConnectionInfo connectionInfo = new ConfigDatabaseConnectionInfo();
            connectionInfo.Server = Environment.GetEnvironmentVariable("SqlServerUrl");
            connectionInfo.Database = Environment.GetEnvironmentVariable("DataBase");
            connectionInfo.UserName = Environment.GetEnvironmentVariable("UserID");
            connectionInfo.Password = Environment.GetEnvironmentVariable("Password");

            string aasConnectionString = Environment.GetEnvironmentVariable("AnalysisServerURL");

            bool integratedAuth = false;
            string sqlConnectionString = SqlConnectionConfig.GetSqlConnectionString(connectionInfo, integratedAuth);

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            CubeModel cube = JsonConvert.DeserializeObject<CubeModel>(requestBody);


            if (cube.TabularModelName == "DataQuality_VHF")
            {
                VhfProcessor vhfProcessor = new VhfProcessor();
                if (vhfProcessor == null)
                {
                    throw new InvalidOperationException($"{nameof(vhfProcessor)} cannot be null");
                }

                return vhfProcessor.ProcessDimTables(aasConnectionString, sqlConnectionString, log, cube);
                
            }
            else if (cube.TabularModelName == "DataQualitySRS_PRD")
            {
                SrsProcessor srsProcessor = new SrsProcessor();
                if (srsProcessor == null)
                {
                    throw new InvalidOperationException($"{nameof(srsProcessor)} cannot be null");
                }

                return srsProcessor.Process(aasConnectionString, sqlConnectionString, log, cube);

            }
            else if (cube.TabularModelName == "DataQuality_UIO")
            {
                return new OkResult();
            }
            else
            {
                return new BadRequestResult();
            }


        }
    }
}
