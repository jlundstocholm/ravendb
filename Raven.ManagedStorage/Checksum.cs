using System.Security.Cryptography;

namespace Raven.ManagedStorage
{
    public static class Checksum
    {
        public const int ChecksumLength = 16;

        public static byte[] CalculateChecksum(byte[] data, int start, int length)
        {
            MD5 hash = MD5.Create();
            return hash.ComputeHash(data, start, length);
        }

        public static bool ChecksumMatch(byte[] checksum, byte[] data, int start, int length)
        {
            byte[] hash = CalculateChecksum(data, start, length);

            for (int i = 0; i < ChecksumLength; i++)
            {
                if (hash[i] != checksum[i])
                {
                    return false;
                }
            }

            return true;
        }
    }
}