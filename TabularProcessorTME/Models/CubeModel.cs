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
        public string[] DimTables { get; set; }
        public string Partition { get; set; }
        public string ProcessType { get; set; }

    }
}
