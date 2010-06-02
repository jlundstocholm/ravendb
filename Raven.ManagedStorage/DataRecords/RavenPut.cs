namespace Raven.ManagedStorage.DataRecords
{
    public class RavenPut : RavenRecord
    {
        public RavenPut(RavenReadStream reader)
        {
            Id = reader.ReadInt64();
            Document = new StorageSerializer(reader).Deserialize();
        }

        public RavenDocument Document { get; private set; }
        public long Id { get; private set; }

        public static int Write(long id, RavenDocument doc, RavenWriteStream stream)
        {
            byte[] buffer = new StorageSerializer(doc).Serialize();

            WriteHeader(stream, RecordType.Put, buffer.Length + sizeof(long));

            stream.Write(id);
            stream.Write(buffer, buffer.Length);

            return buffer.Length;
        }
    }
}