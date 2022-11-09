using AutomatedProcessingDataQuality.Models;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace TabularProcessorTME.Helpers
{
    public class SrsMergePartitions
    {
        public static IActionResult SRSMerge(string mQueryExpr, string partitionKeyUpper, string partitionKeyLower, string aasConnectionString, ILogger log, CubeModel cube)
        {
            if (string.IsNullOrWhiteSpace(mQueryExpr))
            {
                throw new ArgumentException($"'{nameof(mQueryExpr)}'M Query cannot be null or whitespace.", nameof(mQueryExpr));
            }

            if (string.IsNullOrWhiteSpace(partitionKeyUpper))
            {
                throw new ArgumentException($"'{nameof(partitionKeyUpper)}' cannot be null.", nameof(partitionKeyUpper));
            }
            if (string.IsNullOrWhiteSpace(partitionKeyLower))
            {
                throw new ArgumentException($"'{nameof(partitionKeyLower)}' cannot be null.", nameof(partitionKeyLower));
            }
            if (cube == null)
            {
                throw new ArgumentException("Tabular model not initialized properly! ");
            }

            using (Server server = new Server())
            {

                server.Connect(aasConnectionString);
                log.LogInformation("Connection established successfully.\n");
                log.LogInformation("Server name:\t\t{0}", server.Name);

                Database tabularModel = server.Databases.FindByName(cube.TabularModelName.ToString());               
                Table partitionedSRS = tabularModel.Model.Tables.Find("SRS");
                if(partitionedSRS == null)
                {
                    throw new ArgumentNullException("Srs table name input error!");
                }
                /// <summary>
                /// Create new cold partition - daily.
                /// </summary>
                Partition coldPartition = new Partition();
                List<Partition> partitionsToMerge = partitionedSRS.Partitions.ToList();
                Partition mergeSourcePartition = partitionsToMerge.First();
                partitionsToMerge.Remove(mergeSourcePartition);
                string name = "20220317" + "_" + DateTime.Now.ToString("yyyyMMdd");
                MPartitionSource coldPartitionSourceQuery = new MPartitionSource();
                coldPartitionSourceQuery.Expression = PartitionManager.MQueryBuilder(mQueryExpr, "0", partitionKeyUpper);
                PartitionManager.Merge(tabularModel, partitionedSRS, partitionsToMerge, mergeSourcePartition, name, coldPartitionSourceQuery);
                
                /// <summary>
                /// Create new hot partition - daily.
                /// </summary>
                Partition hotPartition = new Partition();
                string partName = DateTime.Now.ToString("yyyyMMdd");
                hotPartition.Name = partName;
                MPartitionSource newSource = new MPartitionSource();
                newSource.Expression = PartitionManager.MQueryBuilder(mQueryExpr, partitionKeyLower, partitionKeyUpper);
                hotPartition.Source = newSource;
                partitionedSRS.Partitions.Add(hotPartition);
                hotPartition.RequestRefresh(RefreshType.Full);
                tabularModel.Model.SaveChanges();

                return new ObjectResult($"Created merged partitions:\ncold - {mergeSourcePartition.Name} - msgI > 0 and msgId <= {partitionKeyLower}" +
                    $"                     \nhot - {hotPartition.Name} - msgId > {partitionKeyLower} and msgId < {partitionKeyUpper}");
            }



           
        }
    }
}
