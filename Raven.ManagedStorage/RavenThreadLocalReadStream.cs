using System.Collections.Generic;

namespace Raven.ManagedStorage
{
    public class RavenThreadLocalReadStream
    {
        private readonly Dictionary<string, RavenReadStream> _streams = new Dictionary<string, RavenReadStream>();

        public RavenReadStream GetStream(string path)
        {
            return _streams[path];
        }

        public void Open(string path)
        {
            _streams.Add(path, new RavenReadStream(path));
        }

        public void Close(string path)
        {
            _streams[path].Dispose();
            _streams.Remove(path);
        }

        public bool HasStream(string path)
        {
            return _streams.ContainsKey(path);
        }
    }
}