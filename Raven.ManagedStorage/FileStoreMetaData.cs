namespace Raven.ManagedStorage
{
    public class FileStoreMetaData
    {
        public FileStoreMetaData(string key, long id, long dataPosition, int length)
        {
            Key = key;
            Id = id;
            DataPosition = dataPosition;
            Length = length;
        }

        public string Key { get; private set; }
        public int Length { get; private set; }
        public long DataPosition { get; private set; }
        public long IndexPosition { get; set; }
        public long Id { get; private set; }
    }
}