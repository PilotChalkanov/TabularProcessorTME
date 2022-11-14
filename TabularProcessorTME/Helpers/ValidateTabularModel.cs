using AutomatedProcessingDataQuality.Models;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using TabularProcessorTME.Models;

namespace TabularProcessorTME.Helpers
{
    public class ValidateTabularModel

    {
        /// <summary>
        /// Validates a tabular model
        /// </summary>
        /// <param name="aasConnection"></param>
        /// <param name="cube"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        public static bool Validate(AnalysisServer aasConnection, CubeModel cube, ILogger log)
        {

            aasConnection.ConnectAAS();
            Database aasDB = aasConnection.Databases.FindByName(cube.TabularModelName);
            Table aasTable = new Table();
            if (aasConnection.Connected && aasDB != null)
            {
                aasTable = aasDB.Model.Tables.Find(cube.TableName);
            }
            else
            {
                log.LogInformation("TabularModel not found!");
                throw new ArgumentNullException("TabularModel not found!");
            }

            if (aasTable == null)
            {
                log.LogInformation("Table not found!");
                throw new ArgumentNullException("Table not found!");
            }
            aasConnection.Disconnect();
            return true;
        }
    }
}
