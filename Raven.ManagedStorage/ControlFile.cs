using System;
using System.IO;

namespace Raven.ManagedStorage
{
    public class ControlInfo
    {
        public long LatestId { get; set; }
    }

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

            _fileStream = new FileStream(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 16, FileOptions.WriteThrough);
        }

        public ControlInfo GetControlData()
        {
            var buffer = new byte[sizeof(long) + Checksum.ChecksumLength];
            _fileStream.Read(buffer, 0, buffer.Length);

            if (Checksum.ChecksumMatch(buffer, buffer, Checksum.ChecksumLength, sizeof(long)))
            {
                return new ControlInfo
                           {
                               LatestId = BitConverter.ToInt64(buffer, Checksum.ChecksumLength)
                           };
            }
            
            return new ControlInfo();
        }

        public void UpdateControlFile(ControlInfo info)
        {
            lock (_lockObject)
            {
                var latestIdBytes = BitConverter.GetBytes(info.LatestId);

                var checkSum = Checksum.CalculateChecksum(latestIdBytes, 0, latestIdBytes.Length);

                _fileStream.Seek(0, SeekOrigin.Begin);
                _fileStream.Write(checkSum, 0, checkSum.Length);
                _fileStream.Write(latestIdBytes, 0, latestIdBytes.Length);
                _fileStream.Flush();
            }
        }
    }
}