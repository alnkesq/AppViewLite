using System;
using System.Collections.Generic;
using System.Text;

namespace AppViewLite.Models
{
    [Flags]
    public enum SelfLabelsEnum
    {
        None,
        Sexual = 1,
        Porn = 2,
        GraphicMedia = 4,
        Nudity = 8,
    }
}

