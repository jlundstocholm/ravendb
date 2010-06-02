using System;
using System.IO;
using Raven.ManagedStorage.DataRecords;

namespace Raven.ManagedStorage
{
    public enum ReaderOptions
    {
        RandomAccess,
        Sequential
    }

    public class RavenReadStream : IDisposable
    {
        private readonly FileStream _fileStream;
        private readonly RavenReader _reader;

        public RavenReadStream(string fileName, ReaderOptions options)
        {
            _fileStream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite, 16, options == ReaderOptions.RandomAccess ? FileOptions.RandomAccess : FileOptions.SequentialScan);
            _reader = new RavenReader(_fileStream);
        }

        public long Position
        {
            get { return _fileStream.Position; }
        }

        public bool EndOfFile
        {
            get { return Position >= _fileStream.Length; }
        }

        public void Dispose()
        {
            _reader.Close();
            _fileStream.Close();

            GC.SuppressFinalize(this);
        }

        ~RavenReadStream()
        {
            Dispose();
        }

        public RavenRecord ReadRecord(RecordType type)
        {
            var position = _fileStream.Position;
            try
            {
                var recordType = ReadRecordType();
                var recordLength = ReadInt32();

                if ((recordType & type) != 0)
                {
                    return ReadCurrentRecord(recordType, recordLength);
                }

                SkipCurrentRecord(recordLength);
                return null;
            }
            catch (EndOfStreamException)
            {
                throw new FileCorruptedException(position);
            }
        }

        private void SkipCurrentRecord(int recordLength)
        {
            Seek(recordLength, SeekOrigin.Current);
        }

        private RavenRecord ReadCurrentRecord(RecordType type, int length)
        {
            long position = Position - RavenRecord.HeaderLength;
            RavenRecord record;

            switch (type)
            {
                case RecordType.Put:
                    record = new RavenPut(this);
                    break;
                case RecordType.IndexPut:
                    record = new RavenIndexPut(this);
                    break;
                case RecordType.IndexDelete:
                    record = new RavenIndexDelete(this);
                    break;
                case RecordType.Delete:
                    record = new RavenDelete(this);
                    break;
                default:
                    throw new NotSupportedException();
            }

            record.Length = length;
            record.Position = position;

            return record;
        }

        public void Seek(long position, SeekOrigin origin)
        {
            _fileStream.Seek(position, origin);
        }

        public void Read(byte[] buffer, int length)
        {
            var bytes = _reader.Read(buffer, 0, length);

            if (bytes < length)
            {
                throw new EndOfStreamException();
            }
        }

        public int ReadInt32()
        {
            return _reader.ReadInt32();
        }

        public RecordType ReadRecordType()
        {
            return (RecordType)ReadInt32();
        }

        public long ReadInt64()
        {
            return _reader.ReadInt64();
        }

        public string ReadString()
        {
            return _reader.ReadString();
        }

        public int Read7BitInteger()
        {
            return _reader.Read7BitInteger();
        }
    }
}