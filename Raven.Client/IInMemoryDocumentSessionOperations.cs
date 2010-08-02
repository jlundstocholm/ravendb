﻿using System;
using System.Collections.Specialized;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;

namespace Raven.Client
{
	public interface IInMemoryDocumentSessionOperations : IDisposable
	{
		string StoreIdentifier { get; }
            
		void Store(object entity);        
        
		void Delete<T>(T entity);

		void Evict<T>(T entity);
        
		void Clear();
        
		bool UseOptimisticConcurrency { get; set; }

		bool AllowNonAuthoritiveInformation { get; set; }

		DocumentConvention Conventions { get; }

		int MaxNumberOfRequestsPerSession { get; set; }

		event EntityStored Stored;

		event EntityToDocument OnEntityConverted;

		JObject GetMetadataFor<T>(T instance);

		bool HasChanges { get; }

		bool HasChanged(object entity);
	}

	public interface IDocumentDeleteListener
	{
		void BeforeDelete(string key, object entityInstance, JObject metadata);
	}

	public interface IDocumentStoreListener
	{
		void BeforeStore(string key, object entityInstance, JObject metadata);
		void AfterStore(string key, object entityInstance, JObject metadata);
	}
}