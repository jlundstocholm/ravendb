using System;
using System.IO;
using System.Text;

namespace Raven.ManagedStorage
{
    public class RavenReader : BinaryReader
    {
        public RavenReader(Stream input)
            : base(input, Encoding.UTF8)
        {
        }

        public int Read7BitInteger()
        {
            return Read7BitEncodedInt();
        }
    }
}