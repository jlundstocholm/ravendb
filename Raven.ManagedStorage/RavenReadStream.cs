using System;
using System.IO;
using Raven.ManagedStorage.DataRecords;

namespace Raven.ManagedStorage
{
    public class RavenReadStream : IDisposable
    {
        private readonly FileStream _fileStream;
        private readonly RavenReader _reader;

        public RavenReadStream(string fileName)
        {
            _fileStream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.Read, FileShare.ReadWrite, 16, FileOptions.RandomAccess);
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
            var recordType = ReadRecordType();
            var recordLength = ReadInt32();

            if ((recordType & type) != 0)
            {
                return ReadCurrentRecord(recordType, recordLength);
            }
            
            SkipCurrentRecord(recordLength);
            return null;
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
                case RecordType.Checkpoint:
                    record = new RavenCheckpoint(this);
                    break;
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
            _reader.Read(buffer, 0, length);
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