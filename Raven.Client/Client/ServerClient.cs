using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Transactions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Bundles.Replication.Data;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Json;

namespace Raven.Client.Client
{
    public class ServerClient : IDatabaseCommands
	{
        private const string RavenReplicationDestinations = "Raven/Replication/Destinations";
        private DateTime lastReplicationUpdate = DateTime.MinValue;
        private readonly object replicationLock = new object();
	    private List<string> replicationDestinations = new List<string>();
        private readonly Dictionary<string, IntHolder> failureCounts = new Dictionary<string, IntHolder>();
	    private int requestCount;

	    private class IntHolder
	    {
	        public int Value;
	    }

	    private readonly string url;
		private readonly DocumentConvention convention;
	    private readonly ICredentials credentials;

	    public ServerClient(string url, DocumentConvention convention, ICredentials credentials)
		{
	        this.credentials = credentials;
	        this.url = url;
			this.convention = convention;
			OperationsHeaders = new NameValueCollection();
			UpdateReplicationInformationIfNeeded();
		}

        private void UpdateReplicationInformationIfNeeded()
        {
            if (lastReplicationUpdate.AddMinutes(5) > DateTime.Now)
                return;
            RefreshReplicationInformation();
        }

	    public void RefreshReplicationInformation()
	    {
	        lock (replicationLock)
	        {
               
	            lastReplicationUpdate = DateTime.Now;
	            var document = DirectGet(url, RavenReplicationDestinations);
	            failureCounts[url] = new IntHolder();// we just hit the master, so we can reset its failure count
	            if (document == null)
	                return;
	            var replicationDocument = document.DataAsJson.JsonDeserialization<ReplicationDocument>();
	            replicationDestinations = replicationDocument.Destinations.Select(x => x.Url).ToList();
	            foreach (var replicationDestination in replicationDestinations)
	            {
	                IntHolder value;
	                if(failureCounts.TryGetValue(replicationDestination, out value))
	                    continue;
	                failureCounts[replicationDestination] = new IntHolder();
	            }
	        }
	    }

	    #region IDatabaseCommands Members

		public NameValueCollection OperationsHeaders
		{
			get; set;
		}

    	public JsonDocument Get(string key)
		{
		    EnsureIsNotNullOrEmpty(key, "key");

		    return ExecuteWithReplication(u => DirectGet(u, key));
		}

	    private T ExecuteWithReplication<T>(Func<string, T> operation)
	    {
	        var currentRequest = Interlocked.Increment(ref requestCount);
	        T result;
	        var threadSafeCopy = replicationDestinations;
            if (ShouldExecuteUsing(url, currentRequest))
            {
                if (TryOperation(operation, url, true, out result))
                    return result;
                if (IsFirstFailure(url) && TryOperation(operation, url, threadSafeCopy.Count>0, out result))
                    return result;
                IncrementFailureCount(url);
            }

	        for (int i = 0; i < threadSafeCopy.Count; i++)
	        {
	            var replicationDestination = threadSafeCopy[i];
                if (ShouldExecuteUsing(replicationDestination, currentRequest) == false)
                    continue;
                if (TryOperation(operation, replicationDestination, true, out result))
                    return result;
                if (IsFirstFailure(url) && TryOperation(operation, replicationDestination, threadSafeCopy.Count > i + 1, out result))
                    return result;
                IncrementFailureCount(url);
	        }
            // this should not be thrown, but since I know the value of should...
            throw new InvalidOperationException(@"Attempted to conect to master and all replicas has failed, giving up.
There is a high probability of a network problem preventing access to all the replicas.
Failed to get in touch with any of the " + 1 + threadSafeCopy.Count + " Raven instances.");
	    }

	    private bool ShouldExecuteUsing(string operationUrl, int currentRequest)
	    {
            IntHolder value;
            if (failureCounts.TryGetValue(operationUrl, out value) == false)
                throw new KeyNotFoundException("BUG: Could not find failure count for " + operationUrl);
	        if (value.Value > 1000)
            {
                return currentRequest % 1000 == 0;
            }
            if (value.Value > 100)
            {
                return currentRequest % 100 == 0;
            }
            if (value.Value > 10)
            {
                return currentRequest % 10 == 0;
            }
            return true;
	    }

	    private bool IsFirstFailure(string operationUrl)
	    {
            IntHolder value;
            if (failureCounts.TryGetValue(operationUrl, out value) == false)
                throw new KeyNotFoundException("BUG: Could not find failure count for " + operationUrl);
	        return Thread.VolatileRead(ref value.Value) == 0;
	    }

	    private void IncrementFailureCount(string operationUrl)
	    {
	        IntHolder value;
            if (failureCounts.TryGetValue(operationUrl, out value) == false)
                throw new KeyNotFoundException("BUG: Could not find failure count for " + operationUrl);
	        Interlocked.Increment(ref value.Value);
	    }

	    private bool TryOperation<T>(Func<string, T> operation, string operationUrl, bool avoidThrowing, out T result)
	    {
	        try
	        {
	            result = operation(operationUrl);
	            ResetFailureCount(operationUrl);
	            return true;
	        }
	        catch(WebException e)
	        {
                if (avoidThrowing == false)
                    throw;
	            result = default(T);
                if (IsServerDown(e))
                    return false;
	            throw;
	        }
	    }

	    private  void ResetFailureCount(string operationUrl)
	    {
            IntHolder value;
            if (failureCounts.TryGetValue(operationUrl, out value) == false)
                throw new KeyNotFoundException("BUG: Could not find failure count for " + operationUrl);
            Thread.VolatileWrite(ref value.Value, 0);
	   
	    }

	    private static bool IsServerDown(WebException e)
	    {
	        return e.InnerException is SocketException;
	    }

	    private JsonDocument DirectGet(string serverUrl, string key)
	    {
	        var metadata = new JObject();
	        AddTransactionInformation(metadata);
            var request = HttpJsonRequest.CreateHttpJsonRequest(this, serverUrl + "/docs/" + key, "GET", metadata, credentials);
			request.AddOperationHeaders(OperationsHeaders);
	        try
	        {
	            return new JsonDocument
	            {
	                DataAsJson = JObject.Parse(request.ReadResponseString()),
					NonAuthoritiveInformation = request.ResponseStatusCode == HttpStatusCode.NonAuthoritativeInformation,
	                Key = key,
	                Etag = new Guid(request.ResponseHeaders["ETag"]),
					Metadata = request.ResponseHeaders.FilterHeaders(isServerDocument: false)
	            };
	        }
	        catch (WebException e)
	        {
	            var httpWebResponse = e.Response as HttpWebResponse;
                if (httpWebResponse == null)
                    throw;
	            if (httpWebResponse.StatusCode == HttpStatusCode.NotFound)
	                return null;
                if (httpWebResponse.StatusCode == HttpStatusCode.Conflict)
                {
                    var conflicts = new StreamReader(httpWebResponse.GetResponseStream());
                    var conflictsDoc = JObject.Load(new JsonTextReader(conflicts));
                    var conflictIds = conflictsDoc.Value<JArray>("Conflicts").Select(x=>x.Value<string>()).ToArray();

                    throw new ConflictException("Conflict detected on " + key +
                                                ", conflict must be resolved before the document will be accessible")
                    {
                        ConflictedVersionIds = conflictIds
                    };
                }
                throw;
	        }
	    }

	    private static void EnsureIsNotNullOrEmpty(string key, string argName)
		{
			if(string.IsNullOrEmpty(key))
				throw new ArgumentException("Key cannot be null or empty", argName);
		}

		public PutResult Put(string key, Guid? etag, JObject document, JObject metadata)
		{
            return ExecuteWithReplication(u => DirectPut(metadata, key, etag, document, u));
		}

	    private PutResult DirectPut(JObject metadata, string key, Guid? etag, JObject document, string operationUrl)
	    {
	        if (metadata == null)
	            metadata = new JObject();
	        var method = String.IsNullOrEmpty(key) ? "POST" : "PUT";
	        AddTransactionInformation(metadata);
	        if (etag != null)
	            metadata["ETag"] = new JValue(etag.Value.ToString());
	        var request = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/docs/" + key, method, metadata, credentials);
			request.AddOperationHeaders(OperationsHeaders);
	        request.Write(document.ToString());

	        string readResponseString;
	        try
	        {
	            readResponseString = request.ReadResponseString();
	        }
	        catch (WebException e)
	        {
	            var httpWebResponse = e.Response as HttpWebResponse;
	            if (httpWebResponse == null ||
	                httpWebResponse.StatusCode != HttpStatusCode.Conflict)
	                throw;
	            throw ThrowConcurrencyException(e);
	        }
	        return JsonConvert.DeserializeObject<PutResult>(readResponseString, new JsonEnumConverter());
	    }

	    private static void AddTransactionInformation(JObject metadata)
	    {
	    	var transactionInformation = RavenTransactionAccessor.GetTransactionInformation();
			if (transactionInformation == null)
				return;

			string txInfo = string.Format("{0}, {1}", transactionInformation.Id, transactionInformation.Timeout);
	        metadata["Raven-Transaction-Information"] = new JValue(txInfo);
	    }

	    public void Delete(string key, Guid? etag)
	    {
	        EnsureIsNotNullOrEmpty(key, "key");
	        ExecuteWithReplication<object>(u =>
	        {
	            DirectDelete(key, etag, u);
	            return null;
	        });
	    }

    	public IndexDefinition GetIndex(string name)
    	{
			EnsureIsNotNullOrEmpty(name, "name");
			return ExecuteWithReplication(u => DirectGetIndex(name, u));
    	}

    	private IndexDefinition DirectGetIndex(string indexName, string operationUrl)
    	{
			var httpJsonRequest = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/indexes/" + indexName +"?definition=yes", "GET", credentials);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
			string indexDefAsString;
			try
    		{
    			indexDefAsString = httpJsonRequest.ReadResponseString();
    		}
    		catch (WebException e)
    		{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse != null &&
					httpWebResponse.StatusCode == HttpStatusCode.NotFound)
					return null;
    			throw;
    		}
    		var indexDefResultAsJson = JObject.Load(new JsonTextReader(new StringReader(indexDefAsString)));
    		return convention.CreateSerializer().Deserialize<IndexDefinition>(
				new JTokenReader(indexDefResultAsJson["Index"])
				);
    	}

    	private void DirectDelete(string key, Guid? etag, string operationUrl)
	    {
	        var metadata = new JObject();
	        if (etag != null)
	            metadata.Add("ETag", new JValue(etag.Value.ToString()));
	        AddTransactionInformation(metadata);
            var httpJsonRequest = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/docs/" + key, "DELETE", metadata, credentials);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
	        try
	        {
	            httpJsonRequest.ReadResponseString();
	        }
	        catch (WebException e)
	        {
	            var httpWebResponse = e.Response as HttpWebResponse;
	            if (httpWebResponse == null ||
	                httpWebResponse.StatusCode != HttpStatusCode.Conflict)
	                throw;
	            throw ThrowConcurrencyException(e);
	        }
	    }

	    private static Exception ThrowConcurrencyException(WebException e)
	    {
	        using (var sr = new StreamReader(e.Response.GetResponseStream()))
	        {
	            var text = sr.ReadToEnd();
	            var errorResults = JsonConvert.DeserializeAnonymousType(text, new
	            {
	                url = (string) null,
	                actualETag = Guid.Empty,
	                expectedETag = Guid.Empty,
	                error = (string) null
	            });
	            return new ConcurrencyException(errorResults.error)
	            {
	                ActualETag = errorResults.actualETag,
	                ExpectedETag = errorResults.expectedETag
	            };
	        }
	    }

        public string PutIndex(string name, IndexDefinition definition)
        {
            return PutIndex(name, definition, false);
        }

        public string PutIndex(string name, IndexDefinition definition, bool overwrite)
        {
			EnsureIsNotNullOrEmpty(name, "name");

            string requestUri = url+"/indexes/"+name;
            try
            {
                var webRequest = (HttpWebRequest)WebRequest.Create(requestUri);
            	AddOperationHeaders(webRequest);
                webRequest.Method = "HEAD";
				webRequest.Credentials = credentials;

                webRequest.GetResponse().Close();
                if(overwrite == false)
                    throw new InvalidOperationException("Cannot put index: " + name + ", index already exists");
            }
            catch (WebException e)
            {
                var response = e.Response as HttpWebResponse;
                if (response == null || response.StatusCode != HttpStatusCode.NotFound)
                    throw;
            }

            var request = HttpJsonRequest.CreateHttpJsonRequest(this, requestUri, "PUT", credentials);
			request.AddOperationHeaders(OperationsHeaders);
			request.Write(JsonConvert.SerializeObject(definition, new JsonEnumConverter()));

			var obj = new {index = ""};
			obj = JsonConvert.DeserializeAnonymousType(request.ReadResponseString(), obj);
			return obj.index;
		}

    	private void AddOperationHeaders(HttpWebRequest webRequest)
    	{
    		foreach (string header in OperationsHeaders)
    		{
    			webRequest.Headers[header] = OperationsHeaders[header];
    		}
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
		    EnsureIsNotNullOrEmpty(index, "index");
            return ExecuteWithReplication(u => DirectQuery(index, query, u));
		}

	    private QueryResult DirectQuery(string index, IndexQuery query, string operationUrl)
	    {
            string path = query.GetIndexQueryUrl(operationUrl, index, "indexes");
	    	var request = HttpJsonRequest.CreateHttpJsonRequest(this, path, "GET", credentials);
			request.AddOperationHeaders(OperationsHeaders);
	    	var serializer = convention.CreateSerializer();
	        JToken json;
	    	try
	    	{
	    		using (var reader = new JsonTextReader(new StringReader(request.ReadResponseString())))
	    			json = (JToken)serializer.Deserialize(reader);
	    	}
	    	catch (WebException e)
	    	{
	    		var httpWebResponse = e.Response as HttpWebResponse;
				if(httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
					throw new InvalidOperationException("There is no index named: " + index);
	    		throw;
	    	}
	    	return new QueryResult
	        {
	            IsStale = Convert.ToBoolean(json["IsStale"].ToString()),
	            Results = json["Results"].Children().Cast<JObject>().ToArray(),
	            TotalResults =  Convert.ToInt32(json["TotalResults"].ToString())
	        };
	    }

    	public void DeleteIndex(string name)
		{
			EnsureIsNotNullOrEmpty(name, "name");
            var request = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/indexes/" + name, "DELETE", credentials);
			request.AddOperationHeaders(OperationsHeaders);
		    request.ReadResponseString();
		}

	    public JsonDocument[] Get(string[] ids)
	    {
	        return ExecuteWithReplication(u => DirectGet(ids, u));
	    }

	    private JsonDocument[] DirectGet(string[] ids, string operationUrl)
	    {
            var request = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/queries/", "POST", credentials);
			request.AddOperationHeaders(OperationsHeaders);
	        request.Write(new JArray(ids).ToString(Formatting.None));
	        var responses = JArray.Parse(request.ReadResponseString());

	        return (from doc in responses.Cast<JObject>()
	                let metadata = (JObject) doc["@metadata"]
	                let _ = doc.Remove("@metadata")
	                select new JsonDocument
	                {
	                    Key = metadata["@id"].Value<string>(),
	                    Etag = new Guid(metadata["@etag"].Value<string>()),
						NonAuthoritiveInformation = metadata.Value<bool>("Non-Authoritive-Information"),
	                    Metadata = metadata,
	                    DataAsJson = doc,
	                })
	            .ToArray();
	    }

	    public BatchResult[] Batch(ICommandData[] commandDatas)
	    {
            return ExecuteWithReplication(u => DirectBatch(commandDatas, u));
	    }

	    private BatchResult[] DirectBatch(IEnumerable<ICommandData> commandDatas, string operationUrl)
	    {
	        var metadata = new JObject();
	        AddTransactionInformation(metadata);
            var req = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/bulk_docs", "POST", metadata, credentials);
			req.AddOperationHeaders(OperationsHeaders);
	        var jArray = new JArray(commandDatas.Select(x => x.ToJson()));
	        req.Write(jArray.ToString(Formatting.None));

	        string response;
	        try
	        {
	            response = req.ReadResponseString();
	        }
	        catch (WebException e)
	        {
	            var httpWebResponse = e.Response as HttpWebResponse;
	            if (httpWebResponse == null ||
	                httpWebResponse.StatusCode != HttpStatusCode.Conflict)
	                throw;
	            throw ThrowConcurrencyException(e);
	        }
	        return JsonConvert.DeserializeObject<BatchResult[]>(response);
	    }

	    public void Commit(Guid txId)
	    {
	        ExecuteWithReplication<object>(u =>
	        {
	            DirectCommit(txId, u);
	            return null;
	        });
	    }

	    private void DirectCommit(Guid txId, string operationUrl)
	    {
            var httpJsonRequest = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/transaction/commit?tx=" + txId, "POST", credentials);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
	        httpJsonRequest.ReadResponseString();
	    }

	    public void Rollback(Guid txId)
	    {
            ExecuteWithReplication<object>(u =>
            {
                DirectRollback(txId, u);
                return null;
            });
	    }

    	public byte[] PromoteTransaction(Guid fromTxId)
    	{
			return ExecuteWithReplication(u => DirectPromoteTransaction(fromTxId, u));
		}

    	public void StoreRecoveryInformation(Guid txId, byte[] recoveryInformation)
    	{
			ExecuteWithReplication<object>(u =>
			{
				var webRequest = (HttpWebRequest)WebRequest.Create(u +"/static/transactions/recoveryInformation/" + txId);
				AddOperationHeaders(webRequest); 
				webRequest.Method = "PUT";
				webRequest.Credentials = credentials;
				webRequest.UseDefaultCredentials = true;
				
				using(var stream = webRequest.GetRequestStream())
				{
					stream.Write(recoveryInformation, 0, recoveryInformation.Length);
				}

				webRequest.GetResponse()
					.Close();

				return null;
			});
    	}

    	private byte[] DirectPromoteTransaction(Guid fromTxId, string operationUrl)
    	{
    		var webRequest = (HttpWebRequest)WebRequest.Create(operationUrl + "/transaction/promote?fromTxId=" + fromTxId);
			AddOperationHeaders(webRequest); 
			webRequest.Method = "POST";
    		webRequest.ContentLength = 0;
			webRequest.Credentials = credentials;
    		webRequest.UseDefaultCredentials = true;

			using(var response = webRequest.GetResponse())
			{
				using(var stream = response.GetResponseStream())
				{
					return stream.ReadData();
				}
			}
    	}

    	private void DirectRollback(Guid txId, string operationUrl)
	    {
            var httpJsonRequest = HttpJsonRequest.CreateHttpJsonRequest(this, operationUrl + "/transaction/rollback?tx=" + txId, "POST", credentials);
			httpJsonRequest.AddOperationHeaders(OperationsHeaders);
	        httpJsonRequest.ReadResponseString();
	    }

	    public IDatabaseCommands With(ICredentials credentialsForSession)
	    {
	        return new ServerClient(url, convention, credentialsForSession);
	    }

    	public bool SupportsPromotableTransactions
    	{
			get { return true; }
    	}

    	public void DeleteByIndex(string indexName, IndexQuery queryToDelete, bool allowStale)
    	{
			ExecuteWithReplication<object>(operationUrl =>
			{
				string path = queryToDelete.GetIndexQueryUrl(operationUrl, indexName, "bulk_docs") + "&allowStale=" + allowStale;
				var request = HttpJsonRequest.CreateHttpJsonRequest(this, path, "DELETE", credentials);
				request.AddOperationHeaders(OperationsHeaders);
				try
				{
					request.ReadResponseString();
				}
				catch (WebException e)
				{
					var httpWebResponse = e.Response as HttpWebResponse;
					if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException("There is no index named: " + indexName);
					throw;
				}
				return null;
			});
    	}

    	public void UpdateByIndex(string indexName, IndexQuery queryToUpdate, PatchRequest[] patchRequests, bool allowStale)
    	{

			ExecuteWithReplication<object>(operationUrl =>
			{
				string path = queryToUpdate.GetIndexQueryUrl(operationUrl, indexName, "bulk_docs") + "&allowStale=" + allowStale;
				var request = HttpJsonRequest.CreateHttpJsonRequest(this, path, "PATCH", credentials);
				request.AddOperationHeaders(OperationsHeaders);
				request.Write(new JArray(patchRequests.Select(x=>x.ToJson())).ToString(Formatting.Indented));
				try
				{
					request.ReadResponseString();
				}
				catch (WebException e)
				{
					var httpWebResponse = e.Response as HttpWebResponse;
					if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
						throw new InvalidOperationException("There is no index named: " + indexName);
					throw;
				}
				return null;
			});
    	}

    	#endregion
    }
}