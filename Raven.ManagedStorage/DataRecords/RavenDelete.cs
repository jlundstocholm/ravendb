namespace Raven.ManagedStorage.DataRecords
{
    public class RavenDelete : RavenRecord
    {
        public RavenDelete(RavenReadStream reader)
        {
            Key = reader.ReadString();
        }

        public string Key { get; private set; }

        public static void Write(string key, RavenWriteStream stream)
        {
            int length = RavenWriter.GetLengthPrefixedStringLength(key);
            WriteHeader(stream, RecordType.Delete, length);
            stream.Write(key);
        }
    }
}