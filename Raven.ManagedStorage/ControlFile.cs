using System;
using System.IO;

namespace Raven.ManagedStorage
{
    public class ControlFile : IDisposable
    {
        private const string ControlFileName = "control.raven";

        private readonly string _dataPath;
        private readonly object _lockObject = new object();
        private FileStream _fileStream;

        public ControlFile(string dataPath)
        {
            _dataPath = dataPath;
            IsValid = true;
            OpenFile();
        }

        public long LastCheckpointPosition { get; private set; }
        public long LatestId { get; set; }

        public bool IsValid { get; private set; }

        public void Dispose()
        {
            _fileStream.Close();

            GC.SuppressFinalize(this);
        }

        ~ControlFile()
        {
            Dispose();
        }

        private void OpenFile()
        {
            var path = Path.Combine(_dataPath, ControlFileName);

            if (!File.Exists(path))
            {
                IsValid = false;
            }

            _fileStream = File.Open(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

            var buffer = new byte[sizeof(long) + sizeof(long) + Checksum.ChecksumLength];
            _fileStream.Read(buffer, 0, buffer.Length);

            if (Checksum.ChecksumMatch(buffer, buffer, Checksum.ChecksumLength, sizeof(long) + sizeof(long)))
            {
                LastCheckpointPosition = BitConverter.ToInt64(buffer, Checksum.ChecksumLength);
                LatestId = BitConverter.ToInt64(buffer, Checksum.ChecksumLength + sizeof(long));
            }
            else
            {
                Reset();
            }
        }

        public void UpdateControlFile(long position)
        {
            lock (_lockObject)
            {
                var positionBytes = BitConverter.GetBytes(position);
                var idBytes = BitConverter.GetBytes(LatestId);

                var checkpointBytes = new byte[16];
                Array.Copy(positionBytes, checkpointBytes, sizeof(long));
                Array.Copy(idBytes, 0, checkpointBytes, sizeof(long), sizeof(long));

                var checkSum = Checksum.CalculateChecksum(checkpointBytes, 0, sizeof(long) + sizeof(long));

                _fileStream.Seek(0, SeekOrigin.Begin);
                _fileStream.Write(checkSum, 0, checkSum.Length);
                _fileStream.Write(checkpointBytes, 0, checkpointBytes.Length);
                _fileStream.Flush();

                IsValid = true;
            }
        }

        public void Reset()
        {
            LastCheckpointPosition = 0;
            LatestId = 0;
            UpdateControlFile(0);
            IsValid = false;
        }
    }
}