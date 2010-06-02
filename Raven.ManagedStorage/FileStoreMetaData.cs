namespace Raven.ManagedStorage
{
    public class FileStoreMetaData
    {
        public FileStoreMetaData(string key, long id, long position, int length)
        {
            Key = key;
            Id = id;
            Position = position;
            Length = length;
        }

        public string Key { get; private set; }
        public int Length { get; private set; }
        public long Position { get; private set; }
        public long Id { get; private set; }
    }
}