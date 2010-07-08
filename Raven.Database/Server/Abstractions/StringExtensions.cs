using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Database.Server.Abstractions
{
    public static class StringExtensions
    {
        public static Tuple<string, string> SplitPair(this string source, string separator)
        {
            if (!source.Contains(separator)) return Tuple.Create(source, string.Empty);

            int index = source.IndexOf(separator);

            if (index == 0) return Tuple.Create(string.Empty, source);

            if (index + 1 == source.Length) return Tuple.Create(source, string.Empty);

            return Tuple.Create(source.Substring(0, index), source.Substring(index + 1));
        }
    }
}
