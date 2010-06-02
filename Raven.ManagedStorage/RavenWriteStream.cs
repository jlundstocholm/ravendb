using System;
using System.IO;
using Raven.ManagedStorage.DataRecords;

namespace Raven.ManagedStorage
{
    public enum WriterOptions
    {
        Buffered,
        NonBuffered
    }

    public class RavenWriteStream : IDisposable
    {
        private readonly FileStream _fileStream;
        private readonly RavenWriter _writer;

        public RavenWriteStream(string fileName)
        {
            _fileStream = new FileStream(fileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.Read, 16, FileOptions.WriteThrough);
            _writer = new RavenWriter(_fileStream);
        }

        public long Position
        {
            get { return _fileStream.Position; }
        }

        public bool EndOfFile
        {
            get { return Position >= _fileStream.Length; }
        }

        public long Length
        {
            get { return _fileStream.Length; }
        }

        public void Dispose()
        {
            _writer.Close();
            _fileStream.Close();

            GC.SuppressFinalize(this);
        }

        ~RavenWriteStream()
        {
            Dispose();
        }

        public void Flush()
        {
            _writer.Flush();
        }

        public void Write(int value)
        {
            _writer.Write(value);
        }

        public void Write(long value)
        {
            _writer.Write(value);
        }

        public void Write(RecordType recordType)
        {
            _writer.Write((int)recordType);
        }

        public void Write(string value)
        {
            _writer.Write(value);
        }

        public void Write7BitInteger(int value)
        {
            _writer.Write7BitInteger(value);
        }

        public void Write(byte[] buffer, int length)
        {
            _writer.Write(buffer, 0, length);
        }

        public void Seek(long position, SeekOrigin origin)
        {
            _fileStream.Seek(position, origin);
        }

        public void Truncate(long lastGoodPosition)
        {
            _fileStream.SetLength(lastGoodPosition);
        }
    }
}