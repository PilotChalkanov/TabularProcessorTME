using processAAS.Models;
using System;
using System.Collections.Generic;
using System.Text;
using TabularProcessorTME.Models;
using System.Data.SqlClient;
using Microsoft.AnalysisServices.Tabular;

namespace processAAS.Helpers
{
    /// <summary>
    /// TODO: DBConnectionConfigManager
    /// </summary>
    public class DbConnectionConfig  
    {
        /// <summary>
        /// Get SQLK conection string
        /// </summary>
        /// <param name="connectionInfo"></param>
        /// <param name="integratedAuth"></param>
        /// <returns></returns>
        public static SqlConnection  GetSqlConnectionString(SqlDBConnectionModel connectionInfo, bool integratedAuth)
        {
            string connectionString = $"Persist Security Info=False; " +
                                        $"User ID={connectionInfo.UserName};" +
                                        $"Password={ connectionInfo.Password};" +
                                        $"Initial Catalog = {connectionInfo.Database};" +
                                        $"Data Source = {connectionInfo.Server}; ";
            if (integratedAuth)
            {
                connectionString += "Integrated Security = true";
                return new SqlConnection(connectionString);
            }
            return new SqlConnection(connectionString);
        }

        /// <summary>
        /// Get AAS Connection String
        /// </summary>
        /// <param name="connectionInfo"></param>
        /// <returns></returns>
        public static AnalysisServer GetAasConnectionString(AasDBConnectionModel connectionInfo)
        {
            string connectionString = $"Provider=MSOLAP;Data Source=localhost";                

            // !!!! use this connection string with service principal !!!!

           //string newConnection =  $"Provider=MSOLAP;Data Source={connectionInfo.ServerUrl};User ID={connectionInfo.UserID};" +
           //     $"Password={connectionInfo.Password};Persist Security Info=True; Impersonation Level={connectionInfo.Impersonation};";


            return new AnalysisServer(connectionString);
        }
    }
}
