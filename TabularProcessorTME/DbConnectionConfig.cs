using processAAS.Models;
using System.Data.SqlClient;
using TabularProcessorTME.Models;
using Microsoft.Azure.Services.AppAuthentication;
using Microsoft.Extensions.Logging;
using System;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using Microsoft.AnalysisServices.Tabular;

namespace processAAS.Helpers
{
    /// <summary>
    /// Creates the connection strings for DWH DB and Azure Analysis Service Server
    /// </summary>
    public class DbConnectionConfig
    {
        /// <summary>
        /// Get the SQL conection string
        /// </summary>
        /// <param name="connectionInfo"></param>
        /// <param name="integratedAuth"></param>
        /// <returns></returns>
        public static SqlConnection GetSqlConnectionString(SqlDBConnectionModel connectionInfo, bool integratedAuth)
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
        /// !!! When the service principle is set - the connection string needs to be updated - see below
        /// </summary>
        /// <param name="connectionInfo"></param>
        /// <returns></returns>
        public static AnalysisServer GetAasConnectionString(AasDBConnectionModel connectionInfo)
        {
            var authContext = new AuthenticationContext("https://login.windows.net/" + connectionInfo.tenantId);

            ClientCredential cc = new ClientCredential(connectionInfo.clientId, connectionInfo.password);

            AuthenticationResult token = authContext.AcquireTokenAsync($"https://{connectionInfo.serverUrl}", cc).Result;

            var accessToken = token.AccessToken;

            string connectionString = $"Data Source=asazure://{connectionInfo.serverUrl}/{connectionInfo.serverEndpoint};User ID=;" +
                 $"Password={accessToken};Persist Security Info=True; Impersonation Level=Impersonate;";
                        
            return new AnalysisServer(connectionString);
        }
    }
}
