using System;
using System.Collections.Generic;
using System.Text;

namespace processAAS.Models
{
    public class SqlDBConnectionModel
    {
        public string Server { get; set; }
      
        public string Database { get; set; }
        
        public string UserName { get; set; }
        
        public string Password { get; set; }

        /// <summary>
        /// Initializes a new insance of <see cref="SqlDBConnectionModel"/> class.
        /// </summary>
        /// <param name="server">The server.</param>
        /// <param name="database"></param>
        /// <param name="userName"></param>
        /// <param name="password"></param>
        public SqlDBConnectionModel(string server, string database, string userName, string password)
        {
            Server = server;
            Database = database;
            UserName = userName;
            Password = password;
        }
    }
}
