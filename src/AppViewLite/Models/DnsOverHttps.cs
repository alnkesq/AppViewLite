using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{

    public class DnsOverHttpsResponse
    {
        public int Status { get; set; }
        public bool TC { get; set; }
        public bool RD { get; set; }
        public bool RA { get; set; }
        public bool AD { get; set; }
        public bool CD { get; set; }
        public DnsOverHttpsQuestion[] Question { get; set; }
        public DnsOverHttpsAnswer[] Answer { get; set; }
    }

    public class DnsOverHttpsQuestion
    {
        public string name { get; set; }
        public int type { get; set; }
    }

    public class DnsOverHttpsAnswer
    {
        public string name { get; set; }
        public int type { get; set; }
        public int TTL { get; set; }
        public string data { get; set; }
    }

}

