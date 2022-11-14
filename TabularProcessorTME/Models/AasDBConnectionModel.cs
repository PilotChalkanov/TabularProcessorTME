using System;
using System.Collections.Generic;
using System.Text;

namespace TabularProcessorTME.Models
{
    /// <summary>
    /// The model of the Analysis service DB
    /// </summary>
    public class AasDBConnectionModel
    {
        public string ServerUrl { get; set; }        

        public string UserID { get; set; } = "";

        public string Password { get; set; }

        public string Impersonation { get; set; }
        
        public AasDBConnectionModel(string serverUrl, string userID, string password, string impersonation)
        {
            ServerUrl = serverUrl;           
            UserID = userID;
            Password = password;
            Impersonation = impersonation;
        }

    }
}
