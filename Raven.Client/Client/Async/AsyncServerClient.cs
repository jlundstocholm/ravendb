﻿using System;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Transactions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Document;
using Raven.Client.Exceptions;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Exceptions;

namespace Raven.Client.Client.Async
{
	public class AsyncServerClient : IAsyncDatabaseCommands
	{
		private readonly string url;
		private readonly ICredentials credentials;
		private readonly DocumentConvention convention;

		public AsyncServerClient(string url, DocumentConvention convention, ICredentials credentials)
		{
			this.url = url;
			this.convention = convention;
			this.credentials = credentials;
		}

		public void Dispose()
		{
		}

		public IAsyncResult BeginGet(string key, AsyncCallback callback, object state)
		{
			EnsureIsNotNullOrEmpty(key, "key");

			var metadata = new JObject();
			AddTransactionInformation(metadata);
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/docs/" + key, "GET", metadata, credentials);

			var asyncCallback = callback;
			if (callback != null)
				asyncCallback = ar => callback(new UserAsyncData(request, ar) {Key = key});

			var asyncResult = request.BeginReadResponseString(asyncCallback, state);
			return new UserAsyncData(request, asyncResult)
			{
				Key = key
			};
		}

		public JsonDocument EndGet(IAsyncResult result)
		{
			var asyncData = ((UserAsyncData)result);
			try
			{
				var responseString = asyncData.Request.EndReadResponseString(asyncData.Result);
				return new JsonDocument
				{
					DataAsJson = JObject.Parse(responseString),
					NonAuthoritiveInformation = asyncData.Request.ResponseStatusCode == HttpStatusCode.NonAuthoritativeInformation,
					Key = asyncData.Key,
					Etag = new Guid(asyncData.Request.ResponseHeaders["ETag"]),
					Metadata = asyncData.Request.ResponseHeaders.FilterHeaders(isServerDocument: false)
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
					var conflictIds = conflictsDoc.Value<JArray>("Conflicts").Select(x => x.Value<string>()).ToArray();

					throw new ConflictException("Conflict detected on " + asyncData.Key +
												", conflict must be resolved before the document will be accessible")
					{
						ConflictedVersionIds = conflictIds
					};
				}
				throw;
			}
		}

		public IAsyncResult BeginMultiGet(string[] keys, AsyncCallback callback, object state)
		{
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/queries/", "POST", credentials);
			var array = Encoding.UTF8.GetBytes(new JArray(keys).ToString(Formatting.None));
			var multiStepAsyncResult = new MultiStepAsyncResult(state, request);
			var asyncResult = request.BeginWrite(array, ContinueOperation, new Contiuation
			{
				Callback = callback,
				State = state,
				Request = request,
				MultiAsyncResult = multiStepAsyncResult
			});
			if (asyncResult.CompletedSynchronously)
			{
				ContinueOperation(asyncResult);
			}
	        return multiStepAsyncResult;
		}

		public JsonDocument[] EndMultiGet(IAsyncResult result)
		{
			EnsureNotError(result);

			var multiStepAsyncResult = ((MultiStepAsyncResult)result);
			multiStepAsyncResult.AsyncWaitHandle.Close();

			JArray responses;
			try
			{
				var responseString = multiStepAsyncResult.Request.EndReadResponseString(multiStepAsyncResult.Result);
				responses = JArray.Parse(responseString);
			}
			catch (WebException e)
			{
				var httpWebResponse = e.Response as HttpWebResponse;
				if (httpWebResponse == null ||
					httpWebResponse.StatusCode != HttpStatusCode.Conflict)
					throw;
				throw ThrowConcurrencyException(e);
			}

			return (from doc in responses.Cast<JObject>()
					let metadata = (JObject)doc["@metadata"]
					let _ = doc.Remove("@metadata")
					select new JsonDocument
					{
						Key = metadata["@id"].Value<string>(),
						Etag = new Guid(metadata["@etag"].Value<string>()),
						Metadata = metadata,
						NonAuthoritiveInformation = metadata.Value<bool>("Non-Authoritive-Information"),
						DataAsJson = doc,
					})
				.ToArray();
		}

		public IAsyncResult BeginQuery(string index, IndexQuery query, AsyncCallback callback, object state)
		{
			EnsureIsNotNullOrEmpty(index, "index");
			string path = query.GetIndexQueryUrl(url, index, "indexes");
			var request = HttpJsonRequest.CreateHttpJsonRequest(this, path, "GET", credentials);

			var asyncCallback = callback;
			if (callback != null)
				asyncCallback = ar => callback(new UserAsyncData(request, ar));

			var asyncResult = request.BeginReadResponseString(asyncCallback, state);
			return new UserAsyncData(request, asyncResult);
		}

		public QueryResult EndQuery(IAsyncResult result)
		{
			var userAsyncData = ((UserAsyncData)result);
			var responseString = userAsyncData.Request.EndReadResponseString(userAsyncData.Result);
			JToken json;
			using (var reader = new JsonTextReader(new StringReader(responseString)))
				json = (JToken)convention.CreateSerializer().Deserialize(reader);

			return new QueryResult
			{
				IsStale = Convert.ToBoolean(json["IsStale"].ToString()),
				Results = json["Results"].Children().Cast<JObject>().ToArray(),
				TotalResults = Convert.ToInt32(json["TotalResults"].ToString())
			};
		}

		public IAsyncResult BeginBatch(ICommandData[] commandDatas, AsyncCallback callback, object state)
		{
			var metadata = new JObject();
			AddTransactionInformation(metadata);
			var req = HttpJsonRequest.CreateHttpJsonRequest(this, url + "/bulk_docs", "POST", metadata, credentials);
			var jArray = new JArray(commandDatas.Select(x => x.ToJson()));
			var data = Encoding.UTF8.GetBytes(jArray.ToString(Formatting.None));
			var multiStepAsyncResult = new MultiStepAsyncResult(state, req);

			var asyncResult = req.BeginWrite(data, ContinueOperation, new Contiuation
			{
				Callback = callback,
				State = state,
				Request = req,
				MultiAsyncResult = multiStepAsyncResult
			});
			
			if (asyncResult.CompletedSynchronously)
			{
				ContinueOperation(asyncResult);
			}

			return multiStepAsyncResult;
		}

		public BatchResult[] EndBatch(IAsyncResult result)
		{
			EnsureNotError(result);

			var multiStepAsyncResult = ((MultiStepAsyncResult)result);
			multiStepAsyncResult.AsyncWaitHandle.Close();

			string response;
			try
			{
				response = multiStepAsyncResult.Request.EndReadResponseString(multiStepAsyncResult.Result);
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

		private static Exception ThrowConcurrencyException(WebException e)
		{
			using (var sr = new StreamReader(e.Response.GetResponseStream()))
			{
				var text = sr.ReadToEnd();
				var errorResults = JsonConvert.DeserializeAnonymousType(text, new
				{
					url = (string)null,
					actualETag = Guid.Empty,
					expectedETag = Guid.Empty,
					error = (string)null
				});
				return new ConcurrencyException(errorResults.error)
				{
					ActualETag = errorResults.actualETag,
					ExpectedETag = errorResults.expectedETag
				};
			}
		}

		private static void EnsureNotError(IAsyncResult result)
		{
			var exceptionAsyncData = result as WrapperAsyncData<Exception>;
			if (exceptionAsyncData != null)
				throw new InvalidOperationException("Async operation failed", exceptionAsyncData.Wrapped);

			var multiStep = result as MultiStepAsyncResult;

			if(multiStep != null && multiStep.Error != null)
				throw new InvalidOperationException("Async operation failed", multiStep.Error);
		}

		private static void ContinueOperation(IAsyncResult ar)
		{
			IAsyncResult asyncResult = null;
			var contiuation = ((Contiuation)ar.AsyncState);
			try
			{
				contiuation.Request.EndWrite(ar);
				asyncResult = contiuation.Request.BeginReadResponseString(CompleteOperation, contiuation);
				contiuation.MultiAsyncResult.Result = asyncResult;
				if (asyncResult.CompletedSynchronously)
				{
					CompleteOperation(asyncResult);
				}
			}
			catch (Exception e)
			{
				contiuation.MultiAsyncResult.Error = e;
				contiuation.MultiAsyncResult.Complete(); 
				if (asyncResult!=null && asyncResult.CompletedSynchronously)
					throw;
				if (contiuation.Callback == null)
					return;
				contiuation.Callback(new WrapperAsyncData<Exception>(contiuation.MultiAsyncResult, e));
			}
		}

		private static void CompleteOperation(IAsyncResult ar)
		{
			var contiuation = ((Contiuation)ar.AsyncState);
			contiuation.MultiAsyncResult.Complete();
			if (contiuation.Callback != null)
				contiuation.Callback(contiuation.MultiAsyncResult);
		}

		private class Contiuation
		{
			public AsyncCallback Callback { get; set; }
			public object State { get; set; }
			public HttpJsonRequest Request { get; set; }

			public MultiStepAsyncResult MultiAsyncResult { get; set; }
		}

		private class UserAsyncData : IAsyncResult
		{
			public IAsyncResult Result { get; private set; }
			public HttpJsonRequest Request { get; private set; }

			public bool IsCompleted
			{
				get { return Result.IsCompleted; }
			}

			public WaitHandle AsyncWaitHandle
			{
				get { return Result.AsyncWaitHandle; }
			}

			public object AsyncState
			{
				get { return Result.AsyncState; }
			}

			public bool CompletedSynchronously
			{
				get { return Result.CompletedSynchronously; }
			}

			public string Key { get; set; }

			public UserAsyncData(HttpJsonRequest request, IAsyncResult result)
			{
				Request = request;
				Result = result;
			}
		}

		private static void AddTransactionInformation(JObject metadata)
		{
			if (Transaction.Current == null)
				return;

			string txInfo = string.Format("{0}, {1}", Transaction.Current.TransactionInformation.DistributedIdentifier, TransactionManager.DefaultTimeout);
			metadata["Raven-Transaction-Information"] = new JValue(txInfo);
		}

		private static void EnsureIsNotNullOrEmpty(string key, string argName)
		{
			if (string.IsNullOrEmpty(key))
				throw new ArgumentException("Key cannot be null or empty", argName);
		}

		public class MultiStepAsyncResult : IAsyncResult
		{
			private readonly object state;
			private readonly HttpJsonRequest req;
			private readonly ManualResetEvent manualResetEvent;
			public IAsyncResult Result { get; set; }

			public MultiStepAsyncResult(object state, HttpJsonRequest req)
			{
				this.state = state;
				this.req = req;
				manualResetEvent = new ManualResetEvent(false);
			}

			public bool IsCompleted
			{
				get; set;
			}

			public WaitHandle AsyncWaitHandle
			{
				get { return manualResetEvent; }
			}

			public object AsyncState
			{
				get { return state; }
			}

			public bool CompletedSynchronously
			{
				get { return false; }
			}

			public HttpJsonRequest Request
			{
				get { return req; }
			}

			public Exception Error { get; set; }

			public void Complete()
			{
				manualResetEvent.Set();
			}
		}
	}
}