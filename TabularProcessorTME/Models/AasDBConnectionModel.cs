namespace TabularProcessorTME.Models
{
    /// <summary>
    /// The model of the Analysis service DB
    /// </summary>
    public class AasDBConnectionModel
    {
        public string serverUrl { get; set; }

        public string serverEndpoint { get; set; }

        public string clientId { get; set; }

        public string tenantId { get; set; } = "";

        public string password { get; set; }

        public string impersonation { get; set; }

        public AasDBConnectionModel(string serverUrl, string serverEndpoint, string clientId, string tenantId, string password, string impersonation)
        {
            this.serverUrl = serverUrl;
            this.serverEndpoint = serverEndpoint;
            this.clientId = clientId;
            this.tenantId = tenantId;
            this.password = password;
            this.impersonation = impersonation;
        }

    }
}
