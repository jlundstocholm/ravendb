using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Database.Server.Abstractions
{
    public static class TupleExtensions
    {
        public static void Do<T1, T2>(this Tuple<T1, T2> tuple, Action<T1, T2> action)
        {
            action(tuple.Item1, tuple.Item2);
        }
    }
}
