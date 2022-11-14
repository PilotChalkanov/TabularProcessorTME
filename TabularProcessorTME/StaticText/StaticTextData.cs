using System;

namespace processAAS.StaticText
{
    /// <summary>
    /// All static texts
    /// </summary>
    public static class StaticTextData
    {
        #region Db conectin details
        public static string sqlServerURL => Environment.GetEnvironmentVariable("SqlServerUrl");
        public static string sqlDb => Environment.GetEnvironmentVariable("DataBase");
        public static string sqlDbUserName => Environment.GetEnvironmentVariable("UserID");

        public static string sqlDbPassword = Environment.GetEnvironmentVariable("Password");

        public static string aasServerUrl = Environment.GetEnvironmentVariable("AnalysisServerURL");

        public static string aasUserID = Environment.GetEnvironmentVariable("AasUserId");

        public static string aasPassword = Environment.GetEnvironmentVariable("AAS_TOKEN");

        public static string aasImpersonation = Environment.GetEnvironmentVariable("ImpersonationLevel");
        #endregion

        #region SQL queries
        public static string srsTemplateQuery => "SELECT TemplateSourceQuery FROM DQ.PartitionConfigurator  WHERE TableName = '{0}'";
        public static string srsMaxMsgID => "SELECT MAX(msgID) as msgID FROM SRSe2e.SRS_MIDDLEWARE_MSG_NEW";
        public static string lastMaxMsgId => "SELECT CurrentMaxKey FROM DQ.PartitionConfigurator WHERE TableName = 'SRS'";
        public static string updateMaxId => "UPDATE DQ.PartitionConfigurator SET CurrentMaxKey = @value WHERE TableName = 'SRS';";
        public static string updateProcessingRequired => "UPDATE DQ.VhfPartitionConfig; SET processingRequired = 0 WHERE partitionName = {0}";

        public static string vhfPartitionsInfo = "SELECT partitionName, keyLowerBoundary, keyUpperBoundary from dq.VhfPartitionConfig";

        public static string vhfTemplateQuery => "SELECT TemplateSourceQuery FROM DQ.PartitionConfigurator  WHERE TableName = {0}";
        #endregion
    }
}
