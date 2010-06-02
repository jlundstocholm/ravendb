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

        public int ReadInteger()
        {
            var buffer = new byte[4];
            Read(buffer, 0, 4);

            return BitConverter.ToInt32(buffer, 0);
        }
    }
}