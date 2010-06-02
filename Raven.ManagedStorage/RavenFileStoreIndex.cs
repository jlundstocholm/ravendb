using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using Raven.ManagedStorage.DataRecords;

namespace Raven.ManagedStorage
{
    internal class RavenFileStoreIndex : IDisposable
    {
        private const string IndexStoreName = "index.raven";

        private readonly ConcurrentDictionary<string, FileStoreMetaData> _keyIndex = new ConcurrentDictionary<string, FileStoreMetaData>();
        private readonly ConcurrentDictionary<long, FileStoreMetaData> _idIndex = new ConcurrentDictionary<long, FileStoreMetaData>();
        private RavenWriteStream _writeStream;
        private readonly string _path;

        public RavenFileStoreIndex(string dataPath)
        {
            try
            {
                IsValid = true;
                _path = Path.Combine(dataPath, IndexStoreName);

                OpenFiles();
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }
        }

        private void OpenFiles()
        {
            if (!File.Exists(_path))
            {
                IsValid = false;
            }

            _writeStream = new RavenWriteStream(_path, WriterOptions.Buffered);
            _writeStream.Seek(0, SeekOrigin.End);
        }

        public void ProcessRecoveryData(RavenRecord record)
        {
            if (record is RavenPut)
            {
                var putRecord = (RavenPut)record;

                UpdateIndex(new FileStoreMetaData(putRecord.Document.Key, putRecord.Id,
                                                    putRecord.Position,
                                                    putRecord.Length));
            }
            else if (record is RavenDelete)
            {
                var del = (RavenDelete)record;

                RemoveFromIndex(del.Key);
            }
        }

        public long Position
        {
            get { return _writeStream.Position; }
        }

        public bool IsValid { get; private set; }

        public void Dispose()
        {
            CloseFiles();

            GC.SuppressFinalize(this);
        }

        private void CloseFiles()
        {
            if (_writeStream != null) _writeStream.Dispose();
        }

        ~RavenFileStoreIndex()
        {
            Dispose();
        }

        public void UpdateIndex(FileStoreMetaData info)
        {
            WriteToIndex(info);

            _keyIndex.AddOrUpdate(info.Key, info, (existingKey, existingValue) => info);
            _idIndex.AddOrUpdate(info.Id, info, (existingKey, existingValue) => info);
        }

        public void RemoveFromIndex(string key)
        {
            FileStoreMetaData doc;
            _keyIndex.TryRemove(key, out doc);

            if (doc != null)
            {
                _idIndex.TryRemove(doc.Id, out doc);
                RavenIndexDelete.Write(key, doc.Id, _writeStream);
            }

        }

        public FileStoreMetaData GetDocumentInfo(string key)
        {
            FileStoreMetaData info;

            return _keyIndex.TryGetValue(key, out info) ? info : null;
        }

        public FileStoreMetaData GetDocumentInfo(long id)
        {
            FileStoreMetaData info;

            return _idIndex.TryGetValue(id, out info) ? info : null;
        }

        public void Populate()
        {
            // Spin through the index file
            try
            {
                using (var readStream = new RavenReadStream(_path, ReaderOptions.Sequential))
                {
                    while (!readStream.EndOfFile)
                    {
                        var indexRecord = readStream.ReadRecord(RecordType.IndexPut | RecordType.IndexDelete);

                        if (indexRecord == null) continue;

                        if (indexRecord is RavenIndexPut)
                        {
                            // TODO - FileStoreMetaData pretty much the same as RavenIndexPut - can reuse and save an alloc?
                            var put = (RavenIndexPut) indexRecord;

                            var info = new FileStoreMetaData(put.Key, put.Id, put.DataPosition, put.DataLength)
                                           {IndexPosition = put.Position};

                            _keyIndex.AddOrUpdate(put.Key, info, (existingKey, existingValue) => info);
                            _idIndex.AddOrUpdate(put.Id, info, (existingKey, existingValue) => info);
                        }
                        else if (indexRecord is RavenIndexDelete)
                        {
                            var del = (RavenIndexDelete) indexRecord;
                            FileStoreMetaData doc;

                            _keyIndex.TryRemove(del.Key, out doc);
                            _idIndex.TryRemove(del.Id, out doc);
                        }
                    }
                }
            }
            catch (FileCorruptedException)
            {
                // Index file is hosed
                Reset();
            }
        }

        private void InvalidateRecord(long indexPosition)
        {
            _writeStream.Seek(indexPosition, SeekOrigin.Begin);
            _writeStream.Write(RecordType.Invalidated);
            _writeStream.Seek(0, SeekOrigin.End);
        }

        private void WriteToIndex(FileStoreMetaData docInfo)
        {
            docInfo.IndexPosition = _writeStream.Position;
            RavenIndexPut.Write(docInfo.Key, docInfo.Id, docInfo.DataPosition, docInfo.Length, _writeStream);
        }

        public void Flush()
        {
            _writeStream.Flush();
        }

        public void Reset()
        {
            CloseFiles();
            File.Delete(_path);
            OpenFiles();
            IsValid = false;
        }

        public void InvalidateRecordsPointingBeyond(long lastGoodPosition)
        {
            foreach (var key in _keyIndex.Where(key => key.Value.DataPosition >= lastGoodPosition))
            {
                FileStoreMetaData value;

                _keyIndex.TryRemove(key.Key, out value);
                _idIndex.TryRemove(key.Value.Id, out value);

                InvalidateRecord(key.Value.IndexPosition);
            }
        }
    }
}