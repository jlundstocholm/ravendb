using System;
using System.Collections.Concurrent;
using System.IO;
using Raven.ManagedStorage.DataRecords;

namespace Raven.ManagedStorage
{
    internal class RavenFileStoreIndex : IDisposable
    {
        private const string IndexStoreName = "index.raven";

        private readonly RavenFileStore _fileStore;

        private readonly ConcurrentDictionary<string, FileStoreMetaData> _keyIndex =
            new ConcurrentDictionary<string, FileStoreMetaData>();

        private readonly ConcurrentDictionary<long, FileStoreMetaData> _idIndex =
            new ConcurrentDictionary<long, FileStoreMetaData>();

        private readonly RavenWriteStream _writeStream;

        public RavenFileStoreIndex(RavenFileStore fileStore)
        {
            _fileStore = fileStore;

            _writeStream = new RavenWriteStream(Path.Combine(_fileStore.DataPath, IndexStoreName));
            _writeStream.Seek(0, SeekOrigin.End);

            Populate();
        }

        public long Position
        {
            get { return _writeStream.Position; }
        }

        public void Dispose()
        {
            _writeStream.Dispose();
            GC.SuppressFinalize(this);
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

        private void Populate()
        {
            // Spin through the index file
            using (var readStream = new RavenReadStream(Path.Combine(_fileStore.DataPath, IndexStoreName)))
            {
                while (!readStream.EndOfFile)
                {
                    RavenRecord indexRecord = readStream.ReadRecord(RecordType.IndexPut | RecordType.IndexDelete);

                    if (indexRecord != null)
                    {
                        if (indexRecord is RavenIndexPut)
                        {
                            // TODO - FileStoreMetaData pretty much the same as RavenIndexPut - can reuse and save an alloc?
                            var put = (RavenIndexPut) indexRecord;
                            var info = new FileStoreMetaData(put.Key, put.Id, put.DataPosition, put.DataLength);

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
                        else
                        {
                            throw new NotSupportedException("Unexpected index type");
                        }
                    }
                }
            }
        }

        private void WriteToIndex(FileStoreMetaData docInfo)
        {
            RavenIndexPut.Write(docInfo.Key, docInfo.Id, docInfo.Position, docInfo.Length, _writeStream);
        }

        public void Flush()
        {
            _writeStream.Flush();
        }
    }
}