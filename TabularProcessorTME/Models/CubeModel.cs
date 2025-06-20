﻿using System.ComponentModel.DataAnnotations;

namespace AutomatedProcessingDataQuality.Models
{
    /// <summary>
    /// The model of the analysis data base on which the processors will execute the jobs         
    /// </summary>    
    public class CubeModel
    {
        [Required(ErrorMessage = "TabularModel is required!")]
        public string TabularModelName { get; set; }
        [Required(ErrorMessage = "TableName is required!")]
        [MinLength(2, ErrorMessage = "Invalid table name!")]
        public string TableName { get; set; }
        public string[] DimTables { get; set; } = null;
        public string Partition { get; set; } = null;
        public int ProcessType { get; set; } = 0;

        public string PartitionQuery { get; set; }

    }
}
