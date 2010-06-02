namespace Raven.ManagedStorage.DataRecords
{
    public class RavenIndexPut : RavenRecord
    {
        public RavenIndexPut(RavenReadStream reader)
        {
            Key = reader.ReadString();
            Id = reader.ReadInt64();
            DataPosition = reader.ReadInt64();
            DataLength = reader.Read7BitInteger();
        }

        public long DataPosition { get; private set; }
        public int DataLength { get; private set; }
        public string Key { get; private set; }
        public long Id { get; private set; }

        public static void Write(string key, long id, long dataPosition, int dataLength, RavenWriteStream stream)
        {
            int length = RavenWriter.GetLengthPrefixedStringLength(key) +
                         sizeof(long) +
                         sizeof(long) +
                         RavenWriter.Get7BitEncodingLength(dataLength);

            WriteHeader(stream, RecordType.IndexPut, length);
            stream.Write(key);
            stream.Write(id);
            stream.Write(dataPosition);
            stream.Write7BitInteger(dataLength);
        }
    }
}