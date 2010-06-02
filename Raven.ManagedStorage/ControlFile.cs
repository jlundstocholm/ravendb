using System;
using System.IO;

namespace Raven.ManagedStorage
{
    // TODO - this code sucks.  Either switch to mimic RavenPut etc, or update to protocol buffers
    public class ControlFile : IDisposable
    {
        private const string ControlFileName = "control.raven";

        private readonly string _dataPath;
        private readonly object _lockObject = new object();
        private FileStream _fileStream;

        public ControlFile(string dataPath)
        {
            _dataPath = dataPath;
            OpenFile();
        }

        public long LastCheckpointPosition { get; private set; }
        public long LatestId { get; private set; }

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
            _fileStream = File.Open(Path.Combine(_dataPath, ControlFileName), FileMode.OpenOrCreate,
                                    FileAccess.ReadWrite, FileShare.None);

            var buffer = new byte[sizeof(long) + sizeof(long) + Checksum.ChecksumLength];
            _fileStream.Read(buffer, 0, buffer.Length);

            if (Checksum.ChecksumMatch(buffer, buffer, Checksum.ChecksumLength, sizeof(long) + sizeof(long)))
            {
                LastCheckpointPosition = BitConverter.ToInt64(buffer, Checksum.ChecksumLength);
                LatestId = BitConverter.ToInt64(buffer, Checksum.ChecksumLength + sizeof(long));
            }
            else
            {
                LastCheckpointPosition = 0;
                UpdateControlFile(0, 0);
            }
        }

        public void UpdateControlFile(long position, long nextId)
        {
            lock (_lockObject)
            {
                var positionBytes = BitConverter.GetBytes(position);
                var idBytes = BitConverter.GetBytes(nextId);

                var checkpointBytes = new byte[16];
                Array.Copy(positionBytes, checkpointBytes, sizeof(long));
                Array.Copy(idBytes, 0, checkpointBytes, sizeof(long), sizeof(long));

                var checkSum = Checksum.CalculateChecksum(checkpointBytes, 0, sizeof(long) + sizeof(long));

                _fileStream.Seek(0, SeekOrigin.Begin);
                _fileStream.Write(checkSum, 0, checkSum.Length);
                _fileStream.Write(checkpointBytes, 0, checkpointBytes.Length);
                _fileStream.Flush();
            }
        }
    }
}