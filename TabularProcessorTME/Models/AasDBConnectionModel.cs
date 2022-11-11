using System;
using System.Collections.Generic;
using System.Text;

namespace TabularProcessorTME.Models
{
    /// <summary>
    /// TODO:
    /// </summary>
    public class AasDBConnectionModel
    {
        public string ServerUrl { get; set; }
        

        public string UserID { get; set; } = "";

        public string Password { get; set; }

        public string Impersonation { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="serverUrl"></param>
        /// <param name="database"></param>
        /// <param name="userID"></param>
        /// <param name="password"></param>
        /// <param name="impersonation"></param>
        public AasDBConnectionModel(string serverUrl, string userID, string password, string impersonation)
        {
            ServerUrl = serverUrl;           
            UserID = userID;
            Password = password;
            Impersonation = impersonation;
        }

    }
}
