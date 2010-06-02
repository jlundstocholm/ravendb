using System.IO;
using System.Text;

namespace Raven.ManagedStorage
{
    public class RavenWriter : BinaryWriter
    {
        public RavenWriter(Stream output)
            : base(output, Encoding.UTF8)
        {
        }

        public void Write7BitInteger(int value)
        {
            Write7BitEncodedInt(value);
        }

        public static int GetLengthPrefixedStringLength(string str)
        {
            int length = Encoding.UTF8.GetByteCount(str);

            return Get7BitEncodingLength(length) + length;
        }

        public static int Get7BitEncodingLength(int value)
        {
            int i = 1;
            while (value >= 0x80)
            {
                i++;
                value >>= 7;
            }

            return i;
        }
    }
}