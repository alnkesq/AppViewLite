using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public class BlueskyLabel
    {
        public Plc LabelerPlc => LabelId.Labeler;
        public string LabelerDid;
        public string Name;
        public BlueskyLabelData? Data;
        public string? DisplayName => Data?.DisplayName;
        public string? DisplayNameOrFallback => DisplayName ?? Name;

        public LabelId LabelId;
    }
}

