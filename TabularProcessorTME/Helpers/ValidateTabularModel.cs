using AutomatedProcessingDataQuality.Models;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.Extensions.Logging;
using System;
using TabularProcessorTME.Models;

namespace TabularProcessorTME.Helpers
{
    public class ValidateTabularModel

    {
        /// <summary>
        /// Validates a tabular model
        /// </summary>
        /// <param name="aasConnection">connection to analysis service</param>
        /// <param name="cube">the cube model</param>
        /// <param name="log">the logger for azure functions</param>
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
