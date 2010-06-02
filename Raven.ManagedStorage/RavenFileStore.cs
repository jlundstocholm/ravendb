using System;
using System.Collections.Concurrent;
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

        private readonly object _lockObject = new object();

        private RavenFileStoreIndex _index;
        private ControlFile _controlFile;
        private readonly string _dataFilePath;
        private readonly string _dataDirectory;
        private RavenWriteStream _writeStream;
        private readonly ConcurrentQueue<RavenReadStream> _readStreams = new ConcurrentQueue<RavenReadStream>();

        private int _writesSinceLastCheckpoint;

        public RavenFileStore(string dataPath)
        {
            try
            {
                _dataDirectory = dataPath;
                _dataFilePath = Path.Combine(_dataDirectory, DataStoreName);

                CreateDataDirectory(dataPath);

                OpenDataFiles();

                PerformRecovery();
            }
            catch
            {
                Dispose();
                throw;
            }
        }


        public FileStoreMetaData WriteToStore(RavenDocument doc)
        {
            lock (_lockObject)
            {
                _controlFile.LatestId++;

                var position = _writeStream.Position;
                var length = RavenPut.Write(_controlFile.LatestId, doc, _writeStream);

                _writeStream.Flush();

                var docInfo = new FileStoreMetaData(doc.Key, _controlFile.LatestId, position, length);

                _index.UpdateIndex(docInfo);

                CheckpointIfRequired();

                return docInfo;
            }
        }

        public JsonDocument GetDocument(string key)
        {
            var info = _index.GetDocumentInfo(key);

            if (info == null)
            {
                return null;
            }

            var record = UseReader(readStream =>
                                       {
                                           readStream.Seek(info.DataPosition, SeekOrigin.Begin);
                                           return (RavenPut) readStream.ReadRecord(RecordType.Put);
                                       });

            return new JsonDocument
                       {
                           DataAsJson = JObject.Parse(Encoding.UTF8.GetString(record.Document.Data)),
                           Etag = record.Document.ETag,
                           Key = record.Document.Key,
                           Metadata = JObject.Parse(record.Document.MetaData)
                       };
        }

        private T UseReader<T>(Func<RavenReadStream, T> func)
        {
            RavenReadStream reader = null;

            try
            {
                reader = GetReader();

                return func(reader);
            }
            finally
            {
                ReleaseReader(reader);
            }
        }

        private RavenReadStream GetReader()
        {
            RavenReadStream reader;
            return _readStreams.TryDequeue(out reader) ? reader : new RavenReadStream(_dataFilePath, ReaderOptions.RandomAccess);
        }

        private void ReleaseReader(RavenReadStream reader)
        {
            _readStreams.Enqueue(reader);
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
            var doc = GetDocument(key);

            return doc != null ? (Guid?)doc.Etag : null;
        }

        public void Dispose()
        {
            if (_readStreams != null)
            {
                foreach (var readStream in _readStreams)
                {
                    readStream.Dispose();
                }
            }

            if (_writeStream != null) _writeStream.Dispose();
            if (_index != null) _index.Dispose();
            if (_controlFile != null) _controlFile.Dispose();

            GC.SuppressFinalize(this);
        }

        private void PerformRecovery()
        {
            using (var readStream = new RavenReadStream(_dataFilePath, ReaderOptions.Sequential))
            {
                var position = 0L;

                if (!_controlFile.IsValid)
                {
                    // If the control file is hosed, then we need to start clean
                    _index.Reset();
                }

                // Try to populate the index
                _index.Populate();

                if (_index.IsValid)
                {
                    if (_controlFile.IsValid)
                    {
                        position = _controlFile.LastCheckpointPosition;
                    }
                    else
                    {
                        position = FindLastCheckpoint(readStream);
                    }
                }
                else
                {
                    // Index was hosed.  Reset checkpoint data
                    _controlFile.Reset();
                    position = 0;
                }

                var recoveryData = new RecoveryProcessor(readStream, position, _index).Recover();

                if (recoveryData.LastGoodPosition != _writeStream.Length)
                {
                    // End of file is corrupted; truncate it
                    // TODO - should archive bad data for possible manual recovery
                    _writeStream.Truncate(recoveryData.LastGoodPosition);

                    // Get index to invalidate any records that pointed into the corrupt section
                    _index.InvalidateRecordsPointingBeyond(recoveryData.LastGoodPosition);
                }

                _writeStream.Seek(0, SeekOrigin.End);

                if (recoveryData.LatestId > _controlFile.LatestId)
                {
                    _controlFile.LatestId = recoveryData.LatestId;
                }

                if (recoveryData.EndPosition != _controlFile.LastCheckpointPosition)
                {
                    Checkpoint();
                }
            }
        }

        private long FindLastCheckpoint(RavenReadStream readStream)
        {
            long position = 0;

            while (!readStream.EndOfFile)
            {
                var record = readStream.ReadRecord(RecordType.Checkpoint);

                if (record != null)
                {
                    position = readStream.Position - record.Length;
                }
            }

            return position;
        }


        private void OpenDataFiles()
        {
            _writeStream = new RavenWriteStream(_dataFilePath, WriterOptions.NonBuffered);
            _controlFile = new ControlFile(_dataDirectory);
            _index = new RavenFileStoreIndex(_dataDirectory);
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

                _controlFile.UpdateControlFile(position);

                _writesSinceLastCheckpoint = 0;
            }
        }
    }
}