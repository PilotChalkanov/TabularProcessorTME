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
using AutomatedProcessingDataQuality.Models;
using System.Data.SqlClient;
using processAAS.Models;
using processAAS.Helpers;
using TabularProcessorTME.Processors;
using TabularProcessorTME.Models;
using processAAS.StaticText;
using TabularProcessorTME.Helpers;

namespace processAAS
{
    /// <summary>
    /// The process orchestrator
    /// </summary>
    public class ProcessOrchestrator
    {

       
        /// <summary>
        /// vhf/dimensions endpoint - executes processing of VHF dim tables
        /// </summary>
        /// <param name="req"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        [FunctionName("VhfDimensionTablesProcessor")]
        public static async Task<IActionResult> ProcessVhfDimTables(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "vhf/dimensions")] HttpRequest req,
            ILogger log)
        {
            //Set Sql DB connection string
            SqlDBConnectionModel connectionInfo = new SqlDBConnectionModel(StaticTextData.sqlServerURL, StaticTextData.sqlDb, StaticTextData.sqlDbUserName, StaticTextData.sqlDbPassword);
            bool integratedAuth = false;
            // Sql connection stgring

            SqlConnection sqlConnection = DbConnectionConfig.GetSqlConnectionString(connectionInfo, integratedAuth);

            //Set SSAS DB connection string
            AasDBConnectionModel aasConnectionInfo = new AasDBConnectionModel(StaticTextData.aasServerUrl, StaticTextData.aasUserID, StaticTextData.aasPassword, StaticTextData.aasImpersonation);
            AnalysisServer aasConnection = DbConnectionConfig.GetAasConnectionString(aasConnectionInfo);


            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            CubeModel cube = JsonConvert.DeserializeObject<CubeModel>(requestBody);

            

            
            if (ValidateTabularModel.Validate(aasConnection,cube,log))
            {
                VhfProcessor vhfProcessor = new VhfProcessor(sqlConnection, aasConnection, log);
                if (vhfProcessor == null)
                {
                    throw new InvalidOperationException($"{nameof(vhfProcessor)} cannot be null");
                }

                if (cube.DimTables == null)
                {
                    throw new ArgumentNullException("DimTables cannot be null!");
                }                
                return vhfProcessor.ProcessDimTables(cube);

            }
            else
            {
                log.LogInformation("Not a valid AAS model! Please check the inputs!");                
                return new ObjectResult("Not a valid AAS model! Please check the inputs!")
                {
                    StatusCode = (int?)System.Net.HttpStatusCode.BadRequest
                };
                throw new ArgumentException("Not a valid AAS model! Please check the inputs!");

            }                           
    
        }


        [FunctionName("VhfPartitionProcessor")]
        public static async Task<IActionResult> ProcessVhfPartitions(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "vhf/partitions")] HttpRequest req,
            ILogger log)
        {
            //Set Sql DB connection string
            SqlDBConnectionModel connectionInfo = new SqlDBConnectionModel(StaticTextData.sqlServerURL, StaticTextData.sqlDb, StaticTextData.sqlDbUserName, StaticTextData.sqlDbPassword);
            bool integratedAuth = false;
            // Sql connection stgring
            SqlConnection sqlConnection = DbConnectionConfig.GetSqlConnectionString(connectionInfo, integratedAuth);

            //Set SSAS DB connection string
            AasDBConnectionModel aasConnectionInfo = new AasDBConnectionModel(StaticTextData.aasServerUrl, StaticTextData.aasUserID, StaticTextData.aasPassword, StaticTextData.aasImpersonation);
            AnalysisServer aasConnection = DbConnectionConfig.GetAasConnectionString(aasConnectionInfo);


            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            CubeModel cube = JsonConvert.DeserializeObject<CubeModel>(requestBody);



            if (ValidateTabularModel.Validate(aasConnection, cube, log))
            {
                VhfProcessor vhfProcessor = new VhfProcessor(sqlConnection, aasConnection, log);
                if (vhfProcessor == null)
                {
                    throw new InvalidOperationException($"{nameof(vhfProcessor)} cannot be null");
                }

                if (cube.Partition == null)
                {
                    throw new ArgumentNullException("Partition cannot be null!");
                }
                return vhfProcessor.ProcessPartition(cube);

            }
            else
            {
                log.LogInformation("Not a valid AAS model! Please check the inputs!");
                return new ObjectResult("Not a valid AAS model! Please check the inputs!")
                {
                    StatusCode = (int?)System.Net.HttpStatusCode.BadRequest
                };
                throw new ArgumentException("Not a valid AAS model! Please check the inputs!");

            }            

       }

                
        [FunctionName("SRSOrchestrator")]
        public static async Task<IActionResult> ProcesSRS(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "srs/dimensions")] HttpRequest req,
            ILogger log)

        {
            //Set Sql DB connection string
            SqlDBConnectionModel connectionInfo = new SqlDBConnectionModel(StaticTextData.sqlServerURL, StaticTextData.sqlDb, StaticTextData.sqlDbUserName, StaticTextData.sqlDbPassword);
            bool integratedAuth = false;
            
            // Sql connection stgring
            SqlConnection sqlConnection = DbConnectionConfig.GetSqlConnectionString(connectionInfo, integratedAuth);

            //Set SSAS DB connection string
            AasDBConnectionModel aasConnectionInfo = new AasDBConnectionModel(StaticTextData.aasServerUrl, StaticTextData.aasUserID, StaticTextData.aasPassword, StaticTextData.aasImpersonation);
           
            AnalysisServer aasConnection = DbConnectionConfig.GetAasConnectionString(aasConnectionInfo);


            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            CubeModel cube = JsonConvert.DeserializeObject<CubeModel>(requestBody);

            Server analysisServer = new Server();

            //    analysisServer.Connect(aasConnectionString);
            //    Database aasDB = analysisServer.Databases.FindByName(cube.TabularModelName);
            //    Table aasTable = null;
            //    if (analysisServer.Connected && aasDB != null)
            //    {
            //        aasTable = aasDB.Model.Tables.Find(cube.TableName);
            //    }
            //    if (cube.TabularModelName == "DataQualitySRS_PRD")
            //    {
            //        SrsProcessor srsProcessor = new SrsProcessor();
            //        if (srsProcessor == null)
            //        {
            //            throw new InvalidOperationException($"{nameof(srsProcessor)} cannot be null");
            //        }

            //        return srsProcessor.MergeTables(aasConnectionString, sqlConnectionString, log, cube);

            //    }
            //    else if (cube.TabularModelName == "DataQuality_UIO")
            //    {
            //        return new OkResult();
            //    }
            //    else
            //    {
            //        log.LogInformation("TabularModel not found!");
            //        return new ObjectResult("TabularModel not found!")
            //        {
            //            StatusCode = (int?)System.Net.HttpStatusCode.NotFound
            //        };
            //    }

            return new OkResult();
        }

    }
    }
