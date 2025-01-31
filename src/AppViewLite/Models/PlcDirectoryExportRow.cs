using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{

#nullable disable annotations

    public class PlcDirectoryEntry
    {
        public string did { get; set; }
        public PlcDirectoryOperation operation { get; set; }
        public string cid { get; set; }
        public bool nullified { get; set; }
        public DateTime createdAt { get; set; }
    }

    public class PlcDirectoryOperation
    {
        public string sig { get; set; }
        public object prev { get; set; }
        public string type { get; set; }
        public string handle { get; set; }
        public string service { get; set; }
        public string signingKey { get; set; }
        public string recoveryKey { get; set; }
        public PlcDirectoryService services { get; set; }
        public string[] alsoKnownAs { get; set; }
        public string[] rotationKeys { get; set; }
        public PlcDirectoryVerificationMethod verificationMethods { get; set; }
    }

    public class PlcDirectoryService
    {
        public PlcDirectoryPds atproto_pds { get; set; }
    }

    public class PlcDirectoryPds
    {
        public string type { get; set; }
        public string endpoint { get; set; }
    }

    public class PlcDirectoryVerificationMethod
    {
        public string atproto { get; set; }
    }



    public class DidWebRoot
    {
        public string[] context { get; set; }
        public string id { get; set; }
        public string[] alsoKnownAs { get; set; }
        public DidWebVerificationMethod[] verificationMethod { get; set; }
        public DidWebService[] service { get; set; }
    }

    public class DidWebVerificationMethod
    {
        public string id { get; set; }
        public string type { get; set; }
        public string controller { get; set; }
        public string publicKeyMultibase { get; set; }
    }

    public class DidWebService
    {
        public string id { get; set; }
        public string type { get; set; }
        public string serviceEndpoint { get; set; }
    }


}

