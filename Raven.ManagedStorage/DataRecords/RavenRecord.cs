namespace Raven.ManagedStorage.DataRecords
{
    public class RavenRecord
    {
        public const int HeaderLength = sizeof(int) + sizeof(int);

        public int Length { get; set; }
        public long Position { get; set; }

        protected static void WriteHeader(RavenWriteStream stream, RecordType recordType, int length)
        {
            stream.Write(recordType);
            stream.Write(length);
        }
    }
}