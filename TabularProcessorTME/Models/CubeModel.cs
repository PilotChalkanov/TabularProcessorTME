using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Text;

namespace AutomatedProcessingDataQuality.Models
{
    
    public class CubeModel
    {
        [Required(ErrorMessage = "TabularModel is required!")]
        public string TabularModelName { get; set; }
        [Required(ErrorMessage = "TableName is required!")]
        [MinLength(2,ErrorMessage ="Invalid table name!")]
        public string TableName { get; set; }        
        public string[] DimTables { get; set; } = null;
        public string Partition { get; set; } = null;
        public int ProcessType { get; set; } = 0;

    }
}
