using System;

namespace AppViewLite
{

    public readonly struct GenericNullable<T>
    {
        public GenericNullable(T value)
        {
            ArgumentNullException.ThrowIfNull(value);
            _value = value;
            _hasValue = true;
        }

        public readonly static GenericNullable<T> NullValue;

        public T Value
        {
            get
            {
                if (!_hasValue) throw new InvalidOperationException();
                return _value;
            }
        }

        public bool IsNull => !_hasValue;
        public bool HasValue => _hasValue;
        public T? ValueOrDefault => _hasValue ? _value : default;

        public T GetValueOrDefault(T defaultValue) => _hasValue ? _value : defaultValue;
        public T GetValueOrDefault() => _value;

        private readonly T _value;
        private readonly bool _hasValue;

        public override string? ToString()
        {
            if (!_hasValue) return "(null)";
            return _value!.ToString();
        }

        public static implicit operator GenericNullable<T>(T? value)
        {
            if (value is null) return default;
            return new(value);
        }
    }
}

