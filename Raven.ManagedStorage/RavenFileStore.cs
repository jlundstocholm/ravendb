using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.ManagedStorage.DataRecords;

namespace Raven.ManagedStorage
{
    public class RavenFileStore : IRavenDataStore
    {
        private const string DataStoreName = "data.raven";
        private const int CheckpointThreshold = 100;
        private readonly RavenFileStoreIndex _index;

        private readonly object _lockObject = new object();

        private ControlFile _controlFile;
        private string _dataFilePath;
        private RavenWriteStream _writeStream;

        [ThreadStatic]
        private static RavenThreadLocalReadStream _readStream;

        private int _writesSinceLastCheckpoint;
        private long _latestId;
        private readonly List<RavenThreadLocalReadStream> _readStreams = new List<RavenThreadLocalReadStream>();

        public RavenFileStore(string dataPath)
        {
            DataPath = dataPath;
            _dataFilePath = Path.Combine(DataPath, DataStoreName);

            CreateDataDirectory(dataPath);

            OpenDataFiles();

            _index = new RavenFileStoreIndex(this);

            PerformRecovery();
        }

        public string DataPath { get; private set; }

        #region IRavenDataStore Members

        public FileStoreMetaData WriteToStore(RavenDocument doc)
        {
            lock (_lockObject)
            {
                _latestId++;

                long position = _writeStream.Position;
                int length = RavenPut.Write(_latestId, doc, _writeStream);

                _writeStream.Flush();

                var docInfo = new FileStoreMetaData(doc.Key, _latestId, position, length);

                _index.UpdateIndex(docInfo);

                CheckpointIfRequired();

                return docInfo;
            }
        }

        public JsonDocument GetDocument(string key)
        {
            OpenReader();
            FileStoreMetaData info = _index.GetDocumentInfo(key);

            if (info == null)
            {
                return null;
            }

            RavenPut record;

            lock (_lockObject)
            {
                _readStream.GetStream(_dataFilePath).Seek(info.Position, SeekOrigin.Begin);
                record = (RavenPut)_readStream.GetStream(_dataFilePath).ReadRecord(RecordType.Put);
            }

            return new JsonDocument
                       {
                           DataAsJson = JObject.Parse(Encoding.UTF8.GetString(record.Document.Data)),
                           Etag = record.Document.ETag,
                           Key = record.Document.Key,
                           Metadata = JObject.Parse(record.Document.MetaData)
                       };
        }

        private void OpenReader()
        {
            if (_readStream == null)
            {
                _readStream = new RavenThreadLocalReadStream();
            }

            if (!_readStream.HasStream(_dataFilePath))
            {
                _readStreams.Add(_readStream);
                _readStream.Open(_dataFilePath);
            }
        }

        public static void Clear(string dataPath)
        {
            if (Directory.Exists(dataPath))
            {
                foreach (var file in Directory.EnumerateFiles(dataPath))
                {
                    File.Delete(file);
                }
            }
        }

        public void DeleteFromStore(string key)
        {
            lock (_lockObject)
            {
                RavenDelete.Write(key, _writeStream);

                _writeStream.Flush();

                _index.RemoveFromIndex(key);

                CheckpointIfRequired();
            }
        }

        public Guid? GetETag(string key)
        {
            // TODO - replace with in-memory index lookup
            JsonDocument doc = GetDocument(key);

            return doc != null ? (Guid?)doc.Etag : null;
        }

        public void Dispose()
        {
            foreach (var readStream in _readStreams)
            {
                readStream.Close(_dataFilePath);
            }
            _writeStream.Dispose();
            _index.Dispose();
            _controlFile.Dispose();

            GC.SuppressFinalize(this);
        }

        #endregion

        private void PerformRecovery()
        {
            SeekToLastCheckpoint();

            ProcessNonCheckpointedRecords();
        }

        private void ProcessNonCheckpointedRecords()
        {
            // Spin round every record from now to end of file
            // and write to the index (if the corresponding index record does not exist)
            var id = _controlFile.LatestId;

            while (!_readStream.GetStream(_dataFilePath).EndOfFile)
            {
                var record = _readStream.GetStream(_dataFilePath).ReadRecord(RecordType.Put | RecordType.Delete);

                if (record == null) continue;

                if (record is RavenPut)
                {
                    var putRecord = (RavenPut)record;

                    // TODO - updating index is causing duplicate index records.  Need to have index
                    // load itself first so that the UpdateIndex only writes *if* it has changed.  Just because
                    // we are after the last checkpoint doesn't mean that index isn't up to date
                    _index.UpdateIndex(new FileStoreMetaData(putRecord.Document.Key, putRecord.Id, putRecord.Position,
                                                             putRecord.Length));

                    if (putRecord.Id > id)
                    {
                        id = putRecord.Id;
                    }
                }
                else if (record is RavenDelete)
                {
                    var del = (RavenDelete)record;

                    _index.RemoveFromIndex(del.Key);
                }
            }

            if (_readStream.GetStream(_dataFilePath).Position != _controlFile.LastCheckpointPosition)
            {
                Checkpoint();
            }
        }

        private void SeekToLastCheckpoint()
        {
            var position = _controlFile.LastCheckpointPosition;

            if (position == 0)
            {
                position = FindLastCheckpoint();
            }

            _readStream.GetStream(_dataFilePath).Seek(position, SeekOrigin.Begin);
        }

        private long FindLastCheckpoint()
        {
            long position = 0;

            while (!_readStream.GetStream(_dataFilePath).EndOfFile)
            {
                RavenRecord record = _readStream.GetStream(_dataFilePath).ReadRecord(RecordType.Checkpoint);

                if (record != null)
                {
                    position = _readStream.GetStream(_dataFilePath).Position - record.Length;
                }
            }

            return position;
        }

        private void OpenDataFiles()
        {
            _writeStream = new RavenWriteStream(_dataFilePath);
            _writeStream.Seek(0, SeekOrigin.End);

            OpenReader();
            _controlFile = new ControlFile(DataPath);
        }

        private static void CreateDataDirectory(string dataPath)
        {
            if (!Directory.Exists(dataPath))
            {
                Directory.CreateDirectory(dataPath);
            }
        }

        ~RavenFileStore()
        {
            Dispose();
        }


        private void CheckpointIfRequired()
        {
            Interlocked.Increment(ref _writesSinceLastCheckpoint);

            if (_writesSinceLastCheckpoint > CheckpointThreshold)
            {
                Checkpoint();
            }
        }

        private void Checkpoint()
        {
            lock (_lockObject)
            {
                _index.Flush();

                var position = _writeStream.Position;

                RavenCheckpoint.Write(_index.Position, _writeStream);

                _controlFile.UpdateControlFile(position, _latestId);

                _writesSinceLastCheckpoint = 0;
            }
        }
    }
}