using System;
using Raven.Database;

namespace Raven.ManagedStorage
{
    public interface IRavenDataStore : IDisposable
    {
        FileStoreMetaData WriteToStore(RavenDocument doc);
        JsonDocument GetDocument(string key);
        void DeleteFromStore(string key);
        Guid? GetETag(string key);
    }
}