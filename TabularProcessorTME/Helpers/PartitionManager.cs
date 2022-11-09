using AutomatedProcessingDataQuality.Models;
using Microsoft.AnalysisServices.Tabular;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Text;

namespace TabularProcessorTME.Helpers
{
    public class PartitionManager
    {
        public static void Merge(Database cubeTabular, Table aasTable, List<Partition> partitionsToMerge, Partition sourcePartition,
                                        string partitionName, MPartitionSource mPartitionQuery)
        {

            sourcePartition.RequestMerge(partitionsToMerge);
            cubeTabular.Model.SaveChanges();
            sourcePartition.Name = partitionName;
            sourcePartition.Source = mPartitionQuery;
            cubeTabular.Model.SaveChanges();
            
        }
        public static string MQueryBuilder(string mQueryExpr, string lowerLimitValue, string upperLimitValue)

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
