using System;

namespace Raven.ManagedStorage
{
    public class FileCorruptedException : Exception
    {
        public long LastGoodPosition { get; private set; }

        public FileCorruptedException(long lastGoodPosition)
        {
            LastGoodPosition = lastGoodPosition;
        }
    }
}