using processAAS.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace processAAS.Helpers
{
    public class SqlConnectionConfig
    {
        public static string GetSqlConnectionString(ConfigDatabaseConnectionInfo connectionInfo, bool integratedAuth)
        {
            string connectionString = $"Persist Security Info=False; " +
                                        $"User ID={connectionInfo.UserName};" +
                                        $"Password={ connectionInfo.Password};" +
                                        $"Initial Catalog = {connectionInfo.Database};" +
                                        $"Data Source = {connectionInfo.Server}; ";
            if (integratedAuth)
            {
                connectionString += "Integrated Security = true";
                return connectionString;
            }
            return connectionString;
        }

    }
}
