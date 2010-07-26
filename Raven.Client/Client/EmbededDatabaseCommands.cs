﻿using System;
using System.Linq;
using System.Net;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Database.Storage;

namespace Raven.Client.Client
{
	public class EmbededDatabaseCommands : IDatabaseCommands
	{
		private readonly DocumentDatabase database;
		private readonly DocumentConvention convention;

		public EmbededDatabaseCommands(DocumentDatabase database, DocumentConvention convention)
		{
			this.database = database;
			this.convention = convention;
		}

		public DatabaseStatistics Statistics
		{
			get { return database.Statistics; }
		}

		public ITransactionalStorage TransactionalStorage
		{
			get { return database.TransactionalStorage; }
		}

		public IndexDefinitionStorage IndexDefinitionStorage
		{
			get { return database.IndexDefinitionStorage; }
		}

		public IndexStorage IndexStorage
		{
			get { return database.IndexStorage; }
		}

		#region IDatabaseCommands Members

		public JsonDocument Get(string key)
		{
			return database.Get(key, RavenTransactionAccessor.GetTransactionInformation());
		}

		public PutResult Put(string key, Guid? etag, JObject document, JObject metadata)
		{
			return database.Put(key, etag, document, metadata, RavenTransactionAccessor.GetTransactionInformation());
		}

	    public void Delete(string key, Guid? etag)
		{
			database.Delete(key, etag, RavenTransactionAccessor.GetTransactionInformation());
		}

		public IndexDefinition GetIndex(string name)
		{
			return database.GetIndexDefinition(name);
		}

		public string PutIndex(string name, IndexDefinition definition)
		{
		    return PutIndex(name, definition, false);
		}

        public string PutIndex(string name, IndexDefinition definition, bool overwrite)
        {
            if(overwrite == false && database.IndexStorage.Indexes.Contains(name))
                throw new InvalidOperationException("Cannot put index: " + name + ", index already exists"); 
            return database.PutIndex(name, definition);
        }

		public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinition<TDocument, TReduceResult> indexDef)
		{
			return PutIndex(name, indexDef.ToIndexDefinition(convention));
		}

        public string PutIndex<TDocument, TReduceResult>(string name, IndexDefinition<TDocument, TReduceResult> indexDef, bool overwrite)
        {
            return PutIndex(name, indexDef.ToIndexDefinition(convention), overwrite);
        }

		public QueryResult Query(string index, IndexQuery query)
		{
			return database.Query(index, query);
		}

		public void DeleteIndex(string name)
		{
			database.DeleteIndex(name);
		}

        public JsonDocument[] Get(string[] ids)
	    {
            return ids
				.Select(id => database.Get(id, RavenTransactionAccessor.GetTransactionInformation()))
                .Where(document => document != null)
                .ToArray();
	    }

		public BatchResult[] Batch(ICommandData[] commandDatas)
		{
			foreach (var commandData in commandDatas)
			{
				commandData.TransactionInformation = RavenTransactionAccessor.GetTransactionInformation();
			}
			return database.Batch(commandDatas);
		}

		public void Commit(Guid txId)
	    {
	        database.Commit(txId);
	    }

	    public void Rollback(Guid txId)
	    {
	        database.Rollback(txId);
	    }

		public byte[] PromoteTransaction(Guid fromTxId)
		{
			return database.PromoteTransaction(fromTxId);
		}

		public void StoreRecoveryInformation(Guid txId, byte[] recoveryInformation)
		{
			database.PutStatic("transactions/recoveryInformation/" + txId, null, recoveryInformation, new JObject());
		}

		public IDatabaseCommands With(ICredentials credentialsForSession)
	    {
	        return this;
	    }

		/// <summary>
		/// It seems that we can't promote a transaction inside the same process
		/// </summary>
		public bool SupportsPromotableTransactions
		{
			get { return false; }
		}

		public void DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale)
		{
			throw new NotSupportedException("SET based operations are only supported on the server version, since they are there to reduce remote calls");
		}

		public void UpdateByIndex(string indexName, IndexQuery queryToDelete, PatchRequest[] patchRequests, bool allowStale)
		{
			throw new NotSupportedException("SET based operations are only supported on the server version, since they are there to reduce remote calls");
		}

		#endregion

		public void Dispose()
		{
			database.Dispose();
		}

		public void SpinBackgroundWorkers()
		{
			database.SpinBackgroundWorkers();
		}

		public Attachment GetStatic(string name)
		{
			return database.GetStatic(name);
		}

		public void PutStatic(string name, Guid? etag, byte[] data, JObject metadata)
		{
			database.PutStatic(name, etag, data, metadata);
		}

		public void DeleteStatic(string name, Guid? etag)
		{
			database.DeleteStatic(name, etag);
		}
	}
}