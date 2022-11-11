using Microsoft.AnalysisServices.Tabular;
using System;
using System.Collections.Generic;
using System.Text;

namespace TabularProcessorTME.Models
{
    public class AnalysisServer:Server
    {
        public string aasString;
        public AnalysisServer(string aasString)
        {
            this.aasString = aasString;            
        }
        public void ConnectAAS()
        {
            this.Connect(aasString);
        }
        
    }
}
