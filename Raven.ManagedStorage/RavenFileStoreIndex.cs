using System;
using System.Collections.Concurrent;
using System.IO;
using System.Linq;
using Raven.ManagedStorage.DataRecords;

namespace Raven.ManagedStorage
{
    internal class RavenFileStoreIndex : IDisposable
    {
        public const string IndexStoreName = "index.raven";

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

            _writeStream = new RavenWriteStream(_path);
            _writeStream.Seek(0, SeekOrigin.End);
        }

        public long Position
        {
            get { return _writeStream.Position; }
        }

        public bool IsValid { get; private set; }

        public void Dispose()
        {
            CloseFiles();
        }

        private void CloseFiles()
        {
            if (_writeStream != null) _writeStream.Dispose();
        }


        public void UpdateIndex(FileStoreMetaData info)
        {
            WriteToIndex(info);

            UpdateIndexWithoutPersistence(info);
        }

        public void UpdateIndexWithoutPersistence(FileStoreMetaData info)
        {
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

        public void RemoveFromIndexWithoutPersistence(RavenIndexDelete del)
        {
            FileStoreMetaData doc;

            _keyIndex.TryRemove(del.Key, out doc);
            _idIndex.TryRemove(del.Id, out doc);
        }

        public void Truncate(long lastGoodPosition)
        {
            _writeStream.Truncate(lastGoodPosition);
            _writeStream.Seek(0, SeekOrigin.End);
        }
    }
}