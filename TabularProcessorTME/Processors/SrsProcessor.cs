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


namespace processAAS
{
    public class SrsProcessor
    {
         public IActionResult Process(string aasConnectionString, string sqlConnectionString, ILogger log, CubeModel cube)
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

            /// <summary>
            /// Read the last msgId and m-query from the database.
            /// </summary>
            SqlConnection cnn;
            cnn = new SqlConnection(sqlConnectionString);            
            SqlDataReader dataReader;

            // Query - DQ.Partitionconfigurator to get the last max msgID
            string lastMaxMsgId = StaticTextData.currentMaxMsgID;
            string msgId = "";
            cnn.Open();
            SqlCommand command = new SqlCommand(lastMaxMsgId, cnn);
            dataReader = command.ExecuteReader();
            while (dataReader.Read())
            {
                string result = Convert.ToString(dataReader["currentMaxKey"]);
                log.LogInformation(result);
                msgId += result;
            }
            cnn.Close();

            // Query - DQ.Partitionconfigurator to get the current max msgID
            string currentMaxMsgId = StaticTextData.srsMsgID;
            string maxMsgId = "";
            cnn.Open();
            command = new SqlCommand(currentMaxMsgId, cnn);
            dataReader = command.ExecuteReader();
            while (dataReader.Read())
            {
                string result = Convert.ToString(dataReader["msgID"]);
                log.LogInformation(result);
                maxMsgId += result;
            }
            cnn.Close();

            //Query DQ.PartitionConfigurator to get the mQuery template
            string templateQuery = StaticTextData.TemplateQuery;
            string mQueryExpr = "";            
            cnn.Open();
            command = new SqlCommand(templateQuery, cnn);
            dataReader = command.ExecuteReader();
            
            while (dataReader.Read())            {

                mQueryExpr += Convert.ToString(dataReader["TemplateSourceQuery"]);              
                
            }
            cnn.Close();

            return SrsMergePartitions.SRSMerge(mQueryExpr, maxMsgId, msgId, aasConnectionString, log, cube);           
           
        }
    }
}
