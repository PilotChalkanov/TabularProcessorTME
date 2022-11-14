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




            if (ValidateTabularModel.Validate(aasConnection, cube, log))
            {
                VhfProcessor vhfProcessor = new VhfProcessor(sqlConnection, aasConnection, log);
                if (vhfProcessor == null)
                {
                    throw new InvalidOperationException($"{nameof(vhfProcessor)} cannot be null");
                }

                if (cube.DimTables == null || cube.DimTables.Length < 1)
                {
                    throw new ArgumentNullException("DimTables cannot be null or empty list!");
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

        /// <summary>
        /// vhf/partitions endpoint - executes processing of VHF partition
        /// </summary>
        /// <param name="req"></param>
        /// <param name="log"></param>
        /// <returns></returns>
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

        /// <summary>
        /// vhf/partitions/create endpoint - creates all partitions on VHF table
        /// </summary>
        /// <param name="req"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        [FunctionName("VhfPartitionCreator")]
        public static async Task<IActionResult> CreateVhfPartitions(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "vhf/partitions/create")] HttpRequest req,
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

                if (cube.TableName == null)
                {
                    throw new ArgumentNullException("Table to be partitioned cannot be null!");
                }
                return vhfProcessor.CreateAllPartitions(cube);

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

        /// <summary>
        /// srs/dimensions endpoint - executes partitioning and merge of SRS table 
        /// </summary>
        /// <param name="req"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        [FunctionName("SRSDimensionTablesProcessor")]
        public static async Task<IActionResult> ProcesSrsDimensions(
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



            if (ValidateTabularModel.Validate(aasConnection, cube, log))
            {
                SrsProcessor srsProcessor = new SrsProcessor(sqlConnection, aasConnection, log);
                if (srsProcessor == null)
                {
                    throw new InvalidOperationException($"{nameof(srsProcessor)} cannot be null");
                }

                if (cube.DimTables == null || cube.DimTables.Length < 1)
                {
                    throw new ArgumentNullException("Dim Tables cannot be null or empty value!");
                }
                return srsProcessor.ProcessDimTables(cube);

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

        /// <summary>
        /// Processes a single partition of SRS Table
        /// </summary>
        /// <param name="req"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        [FunctionName("SRSPartitionProcessor")]
        public static async Task<IActionResult> ProcesSRS(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "srs/single_partition")] HttpRequest req,
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
                SrsProcessor srsProcessor = new SrsProcessor(sqlConnection, aasConnection, log);
                if (srsProcessor == null)
                {
                    throw new InvalidOperationException($"{nameof(srsProcessor)} cannot be null");
                }

                if (cube.TableName == null)
                {
                    throw new ArgumentNullException("Table value cannot be null!");
                }
                if (String.IsNullOrWhiteSpace(cube.Partition))
                {
                    throw new ArgumentNullException("Partition value cannot be null!");
                }
                return srsProcessor.ProcessPartition(cube);

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


        /// <summary>
        /// Merges and process incrementaly with daily granularity the SRS Table and partition
        /// </summary>
        /// <param name="req"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        [FunctionName("SRSMergeProcessor")]
        public static async Task<IActionResult> MergeSRS(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "srs/merge_process")] HttpRequest req,
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
                SrsProcessor srsProcessor = new SrsProcessor(sqlConnection, aasConnection, log);
                if (srsProcessor == null)
                {
                    throw new InvalidOperationException($"{nameof(srsProcessor)} cannot be null");
                }

                if (cube.TableName == null)
                {
                    throw new ArgumentNullException("Table value cannot be null!");
                }
                
                return srsProcessor.MergeTables(cube);

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


    }
}
