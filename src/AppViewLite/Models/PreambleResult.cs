using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AppViewLite.Models
{
    public readonly struct PreambleResult<T>
    {
        private readonly bool succeeded;
        private readonly T value;
        public PreambleResult(T value)
        {
            this.succeeded = true;
            this.value = value;
        }
        public readonly static PreambleResult<T> NeedsWrite = default;

        public bool Succeeded => succeeded;
        public T Value
        {
            get
            {
                if (!succeeded) throw new InvalidOperationException();
                return value;
            }
        }
        public static implicit operator PreambleResult<T>(T value) => new(value);
    }

    public readonly struct PreambleResult
    {
        private readonly bool succeeded;
        public PreambleResult()
        {
            this.succeeded = true;
        }
        public readonly static PreambleResult Successs = new();
        public readonly static PreambleResult NeedsWrite = default;

        public bool Succeeded => succeeded;

        public static PreambleResult<T> Create<T>(T value) => new(value);

    }
}

