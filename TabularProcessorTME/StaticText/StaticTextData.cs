using System;
using System.Collections.Generic;
using System.Text;

namespace processAAS.StaticText
{
    public static class StaticTextData
    {
        public static string TemplateQuery => "SELECT TemplateSourceQuery FROM DQ.PartitionConfigurator  WHERE TableName = 'SRS'";
        public static string srsMsgID => "SELECT MAX(msgID) as msgID FROM SRSe2e.SRS_MIDDLEWARE_MSG_NEW";
        public static string currentMaxMsgID => "SELECT CurrentMaxKey FROM DQ.PartitionConfigurator WHERE TableName = 'SRS'";

        public static string updateProcessingRequired => "UPDATE DQ.VhfPartitionConfig; SET processingRequired = 0 WHERE partitionName = {0}";
    }
}
