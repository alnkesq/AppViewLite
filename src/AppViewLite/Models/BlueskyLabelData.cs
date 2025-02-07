using ProtoBuf;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    [ProtoContract]
    public class BlueskyLabelData
    {
        public BlueskyLabelData()
        { 
        
        }
        public BlueskyLabelData(string displayName, string description, BlueskyLabelSeverity severity = BlueskyLabelSeverity.None)
        {
            this.DisplayName = displayName;
            this.Description = description;
        }

        // treat as immutable! TryGetLabelData can return singletons.

        [ProtoMember(1)] public string? DisplayName;
        [ProtoMember(2)] public string? Description;
        [ProtoMember(3)] public BlueskyLabelSeverity Severity;
        [ProtoMember(4)] public BlueskyLabelBlur Blur;
        [ProtoMember(5)] public bool AdultOnly;
        [ProtoMember(6)] public BlueskyLabelDefaultSetting DefaultSetting;

        [ProtoMember(7)] public bool ReuseDefaultDefinition;

        [ProtoMember(8)] public bool Dummy;
    }

    public enum BlueskyLabelSeverity
    {
        Invalid,

        None,
        Inform,
        Alert,

    }
    public enum BlueskyLabelBlur
    {
        Invalid,

        None,
        Content,
        Media,
    }
    public enum BlueskyLabelDefaultSetting
    {
        Invalid,

        Ignore,
        Warn,
        Hide,
        Show,
    }
}

