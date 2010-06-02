using System;
using System.Collections.Generic;
using System.IO;
using Raven.ManagedStorage.DataRecords;

namespace Raven.ManagedStorage
{
    internal class RecoveryProcessor
    {
        private readonly RavenReadStream _readStream;
        private readonly long _lastCheckpointPosition;
        private readonly RavenFileStoreIndex _index;

        public RecoveryProcessor(RavenReadStream readStream, long lastCheckpointPosition, RavenFileStoreIndex index)
        {
            _readStream = readStream;
            _lastCheckpointPosition = lastCheckpointPosition;
            _index = index;
        }

        public RecoveryResults Recover()
        {
            _readStream.Seek(_lastCheckpointPosition, SeekOrigin.Begin);

            // Spin round every record from now to end of file
            // and write to the index (if the corresponding index record does not exist)
            var id = 0L;
            var lastGoodPosition = 0L;

            try
            {
                while (!_readStream.EndOfFile)
                {
                    var record = _readStream.ReadRecord(RecordType.Put | RecordType.Delete);
                    lastGoodPosition = _readStream.Position;

                    if (record == null) continue;

                    _index.ProcessRecoveryData(record);

                    if (record is RavenPut)
                    {
                        var putRecord = (RavenPut)record;

                        if (putRecord.Id > id)
                        {
                            id = putRecord.Id;
                        }
                    }
                }
            }
            catch (FileCorruptedException e)
            {
                lastGoodPosition = e.LastGoodPosition;
            }

            return new RecoveryResults {EndPosition = _readStream.Position, LatestId = id, LastGoodPosition = lastGoodPosition};
        }
    }

    public class RecoveryResults
    {
        public long LastGoodPosition { get; set; }
        public long LatestId { get; set; }
        public long EndPosition { get; set; }
    }
}