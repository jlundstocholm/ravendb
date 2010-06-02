namespace Raven.ManagedStorage.DataRecords
{
    public class RavenCheckpoint : RavenRecord
    {
        public RavenCheckpoint(RavenReadStream reader)
        {
            IndexPosition = reader.ReadInt64();
        }

        public long IndexPosition { get; private set; }

        public static int Write(long indexPosition, RavenWriteStream stream)
        {
            WriteHeader(stream, RecordType.Checkpoint, 8);

            stream.Write(indexPosition);

            return 8;
        }
    }
}