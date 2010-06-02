using System;

namespace Raven.ManagedStorage
{
    public class RavenDocument
    {
        public string Key { get; set; }
        public Guid ETag { get; set; }
        public int Id { get; set; }
        public byte[] Data { get; set; }
        public string MetaData { get; set; }
    }
}
