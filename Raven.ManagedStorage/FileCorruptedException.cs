using System;
using System.Runtime.Serialization;

namespace Raven.ManagedStorage
{
	[Serializable]
	public class FileCorruptedException : Exception
	{
		public long LastGoodPosition { get; private set; }

		public FileCorruptedException(long lastGoodPosition)
		{
			LastGoodPosition = lastGoodPosition;
		}
		public FileCorruptedException()
		{
		}

		public FileCorruptedException(string message) : base(message)
		{
		}

		public FileCorruptedException(string message, Exception inner) : base(message, inner)
		{
		}

		protected FileCorruptedException(
			SerializationInfo info,
			StreamingContext context) : base(info, context)
		{
		}
	}
   
}