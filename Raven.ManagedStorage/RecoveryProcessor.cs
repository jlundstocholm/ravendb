using System.IO;
using Raven.ManagedStorage.DataRecords;

namespace Raven.ManagedStorage
{
    internal class RecoveryProcessor
    {
        private readonly RavenFileStore _fileStore;
        private readonly RavenFileStoreIndex _index;
        private readonly string _indexPath;
        private readonly string _dataPath;

        public RecoveryProcessor(string path, RavenFileStore fileStore, RavenFileStoreIndex index)
        {
            _indexPath = Path.Combine(path, RavenFileStoreIndex.IndexStoreName);
            _dataPath = Path.Combine(path, RavenFileStore.DataStoreName);

            _fileStore = fileStore;
            _index = index;
        }

        public long Recover()
        {
            var lastGoodDataPositionFromIndex = PopulateIndex();

            var recoveryData = PopulateData(lastGoodDataPositionFromIndex);

            return recoveryData.LatestId;
        }

        private long PopulateIndex()
        {
            var lastGoodDataPosition = 0L;

            // Spin through the index file
            try
            {
                using (var readStream = new RavenReadStream(_indexPath, ReaderOptions.Sequential))
                {
                    while (!readStream.EndOfFile)
                    {
                        var indexRecord = readStream.ReadRecord(RecordType.IndexPut | RecordType.IndexDelete);

                        if (indexRecord == null) continue;

                        if (indexRecord is RavenIndexPut)
                        {
                            // TODO - FileStoreMetaData pretty much the same as RavenIndexPut - can reuse and save an alloc?
                            var put = (RavenIndexPut)indexRecord;

                            var info = new FileStoreMetaData(put.Key, put.Id, put.DataPosition, put.DataLength) { IndexPosition = put.Position };

                            _index.UpdateIndexWithoutPersistence(info);

                            if (put.DataPosition > lastGoodDataPosition) lastGoodDataPosition = put.DataPosition;
                        }
                        else if (indexRecord is RavenIndexDelete)
                        {
                            _index.RemoveFromIndexWithoutPersistence((RavenIndexDelete)indexRecord);

                        }
                    }
                }
            }
            catch (FileCorruptedException e)
            {
                // Need to reprocess everything in the data file from the last good index record
                _index.Truncate(e.LastGoodPosition);
            }

            return lastGoodDataPosition;
        }

        RecoveryResults PopulateData(long lastGoodDataPositionFromIndex)
        {
            // Spin round every record from now to end of file
            // and write to the index
            var id = 0L;
            var lastGoodPosition = 0L;

            try
            {
                using (var readStream = new RavenReadStream(_dataPath, ReaderOptions.Sequential))
                {
                    readStream.Seek(lastGoodDataPositionFromIndex, SeekOrigin.Begin);

                    while (!readStream.EndOfFile)
                    {
                        var record = readStream.ReadRecord(RecordType.Put | RecordType.Delete);
                        lastGoodPosition = readStream.Position;

                        if (record == null) continue;

                        id = ProcessRecoveryData(record, id);
                    }
                }
            }
            catch (FileCorruptedException e)
            {
                lastGoodPosition = e.LastGoodPosition;

                // End of file is corrupted; truncate it
                // TODO - should archive bad data for possible manual recovery
                _fileStore.Truncate(e.LastGoodPosition);

                // Get index to invalidate any records that pointed into the corrupt section
                _index.InvalidateRecordsPointingBeyond(e.LastGoodPosition);
            }

            return new RecoveryResults { LatestId = id };
        }

        public long ProcessRecoveryData(RavenRecord record, long currentLatestId)
        {
            if (record is RavenPut)
            {
                var putRecord = (RavenPut)record;

                _index.UpdateIndex(new FileStoreMetaData(putRecord.Document.Key, putRecord.Id,
                                                    putRecord.Position,
                                                    putRecord.Length));

                return putRecord.Id > currentLatestId ? putRecord.Id : currentLatestId;
            }
            
            if (record is RavenDelete)
            {
                var del = (RavenDelete)record;

                _index.RemoveFromIndex(del.Key);
            }

            return currentLatestId;
        }
    }

    public class RecoveryResults
    {
        public long LatestId { get; set; }
    }
}