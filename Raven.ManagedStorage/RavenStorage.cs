using System;
using System.Text;
using Raven.Database;
using Raven.Database.Exceptions;

namespace Raven.ManagedStorage
{
    public class RavenStorage : IDisposable
    {
        private readonly IRavenDataStore _store;

        public RavenStorage(IRavenDataStore store)
        {
            _store = store;
        }

        public void Dispose()
        {
            _store.Dispose();
        }

        public void AddDocument(string key, string data, Guid? etag, string metadata)
        {
            PerformVersionCheck(etag, key);

            var doc = new RavenDocument { Key = key, Data = Encoding.UTF8.GetBytes(data), MetaData = metadata, ETag = Guid.NewGuid() };

            _store.WriteToStore(doc);
        }

        private void PerformVersionCheck(Guid? etag, string key)
        {
        	if (etag == null)
        		return;
        	
			Guid? existingEtag = _store.GetETag(key);

        	if (existingEtag != null && existingEtag != etag)
        	{
        		throw new ConcurrencyException("PUT attempted on document '" + key +
        			"' using a non current etag")
        		{
        			ActualETag = etag.Value,
        			ExpectedETag = existingEtag.Value
        		};
        	}
        }

    	public JsonDocument DocumentByKey(string key)
        {
            return _store.GetDocument(key);
        }

        public void DeleteDocument(string key, Guid? etag)
        {
            PerformVersionCheck(etag, key);

            _store.DeleteFromStore(key);
        }
    }
}