using AutomatedProcessingDataQuality.Models;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Text;

namespace TabularProcessorTME.Helpers
{
    /// <summary>
    /// 
    /// </summary>
    public static class PartitionManager
    {
        /// <summary>
        /// Merge ..
        /// </summary>
        /// <param name="cubeTabular"></param>
        /// <param name="aasTable"></param>
        /// <param name="partitionsToMerge"></param>
        /// <param name="sourcePartition"></param>
        /// <param name="partitionName"></param>
        /// <param name="mPartitionQuery"></param>
        public static void Merge(Database cubeTabular, Table aasTable, List<Partition> partitionsToMerge, Partition sourcePartition,
                                        string partitionName, MPartitionSource mPartitionQuery)
        {
            sourcePartition.RequestMerge(partitionsToMerge);
            cubeTabular.Model.SaveChanges();
            sourcePartition.Name = partitionName;
            sourcePartition.Source = mPartitionQuery;
            cubeTabular.Model.SaveChanges();
        }

        /// <summary>
        /// Builds the MQuery expresion
        /// </summary>
        /// <param name="mQueryExpr"></param>
        /// <param name="lowerLimitValue"></param>
        /// <param name="upperLimitValue"></param>
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
