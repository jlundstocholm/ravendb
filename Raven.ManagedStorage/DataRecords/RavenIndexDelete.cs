namespace Raven.ManagedStorage.DataRecords
{
    public class RavenIndexDelete : RavenRecord
    {
        public RavenIndexDelete(RavenReadStream reader)
        {
            Key = reader.ReadString();
            Id = reader.ReadInt64();
        }

        public string Key { get; private set; }
        public long Id { get; private set; }

        public static void Write(string key, long id, RavenWriteStream stream)
        {
            int length = RavenWriter.GetLengthPrefixedStringLength(key) +
                         sizeof(long);

            WriteHeader(stream, RecordType.IndexDelete, length);
            stream.Write(key);
            stream.Write(id);
        }
    }
}