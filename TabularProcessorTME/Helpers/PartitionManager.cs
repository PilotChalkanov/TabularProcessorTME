using Microsoft.AnalysisServices.Tabular;
using System;
using System.Collections.Generic;

namespace TabularProcessorTME.Helpers
{

    public static class PartitionManager
    {
        /// <summary>
        /// Merge
        /// </summary>
        /// <param name="cubeTabular">Tabular model on which we make the modifications</param>        
        /// <param name="partitionsToMerge">List of partitions to be merged on the source partition</param>
        /// <param name="sourcePartition">The source partition on which we merge</param>
        /// <param name="partitionName">Name of the new merged partition</param>
        /// <param name="mPartitionQuery">The MQuery expression</param>
        public static void Merge(Database cubeTabular, List<Partition> partitionsToMerge, Partition sourcePartition,
                                        string partitionName, MPartitionSource mPartitionQuery)
        {
            sourcePartition.RequestMerge(partitionsToMerge);
            cubeTabular.Model.SaveChanges();
            sourcePartition.Name = partitionName;
            sourcePartition.Source = mPartitionQuery;
            cubeTabular.Model.SaveChanges();
        }

        /// <summary>
        /// builds the MQuery expresion of a partition or table
        /// </summary>
        /// <param name="mQueryExpr">The current mQuery template</param>
        /// <param name="lowerLimitValue">Lower key value used for partitioning</param>
        /// <param name="upperLimitValue">The upped key value used for partitiong</param>
        /// <returns></returns>
        public static string BuildMQuery(string mQueryExpr, string lowerLimitValue, string upperLimitValue)

        {
            if (string.IsNullOrWhiteSpace(mQueryExpr))
            {
                throw new ArgumentException($"'{nameof(mQueryExpr)}'M Query cannot be null or whitespace.", nameof(mQueryExpr));
            }

            if (string.IsNullOrWhiteSpace(lowerLimitValue))
            {
                throw new ArgumentException($"'{nameof(lowerLimitValue)}' cannot be null.", nameof(lowerLimitValue));
            }
            if (string.IsNullOrWhiteSpace(upperLimitValue))
            {
                throw new ArgumentException($"'{nameof(upperLimitValue)}' cannot be null.", nameof(upperLimitValue));
            }

            string newQuery = mQueryExpr.Replace("{0}", lowerLimitValue)
                                                 .Replace("{1}", upperLimitValue);
            return newQuery;
        }
    }
}
