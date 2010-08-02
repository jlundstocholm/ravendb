using System;
using System.Collections.Generic;
using System.Net;
using Raven.Client.Client;
using Raven.Client.Client.Async;
using Raven.Client.Document.Async;
using System.Linq;

namespace Raven.Client.Document
{
	public class DocumentStore : IDocumentStore
	{
		private Func<IDatabaseCommands> databaseCommandsGenerator;
		public IDatabaseCommands DatabaseCommands
		{
			get
			{
				if (databaseCommandsGenerator == null)
					return null;
				return databaseCommandsGenerator();
			}
		}

		private Func<IAsyncDatabaseCommands> asyncDatabaseCommandsGenerator;
		public IAsyncDatabaseCommands AsyncDatabaseCommands
		{
			get
			{
				if (asyncDatabaseCommandsGenerator == null)
					return null;
				return asyncDatabaseCommandsGenerator();
			}
		}

		public event EventHandler<StoredEntityEventArgs> Stored;

		public DocumentStore()
		{
			Conventions = new DocumentConvention();
		}

		private string identifier;
		private IDocumentDeleteListener[] deleteListeners = new IDocumentDeleteListener[0];
		private IDocumentStoreListener[] storeListeners = new IDocumentStoreListener[0];
		private ICredentials credentials = CredentialCache.DefaultNetworkCredentials;

	    public ICredentials Credentials
	    {
	        get { return credentials; }
	        set { credentials = value; }
	    }

	    public string Identifier
		{
			get
			{
				return identifier ?? Url 
#if !CLIENT
					?? DataDirectory
#endif
;
			}
			set { identifier = value; }
		}
#if !CLIENT
		private Raven.Database.RavenConfiguration configuration;
		
		public Raven.Database.RavenConfiguration Configuration
		{
			get
			{
				if(configuration == null)
                    configuration = new Raven.Database.RavenConfiguration();
				return configuration;
			}
			set { configuration = value; }
		}

		public string DataDirectory
		{
			get
			{
				return Configuration == null ? null : Configuration.DataDirectory;
			}
			set
			{
				if (Configuration == null)
                    Configuration = new Raven.Database.RavenConfiguration();
				Configuration.DataDirectory = value;
			}
		}
#endif
		public string Url { get; set; }

		public DocumentConvention Conventions { get; set; }

		#region IDisposable Members

		public void Dispose()
		{
            Stored = null;
#if !CLIENT
			if (DocumentDatabase != null)
				DocumentDatabase.Dispose();
#endif
		}

		#endregion

        public IDocumentSession OpenSession(ICredentials credentialsForSession)
        {

            if (DatabaseCommands == null)
                throw new InvalidOperationException("You cannot open a session before initialising the document store. Did you forgot calling Initialise?");
            var session = new DocumentSession(this, storeListeners, deleteListeners);
			session.Stored += OnSessionStored;
            return session;
        }

		private void OnSessionStored(object entity)
		{
			var copy = Stored;
			if (copy != null)
				copy(this, new StoredEntityEventArgs
				{
					SessionIdentifier = Identifier, EntityInstance = entity
				});
		}

		public IDocumentStore RegisterListener(IDocumentStoreListener documentStoreListener)
		{
			storeListeners = storeListeners.Concat(new[] {documentStoreListener}).ToArray();
			return this;
		}

		public IDocumentSession OpenSession()
        {
            if(DatabaseCommands == null)
                throw new InvalidOperationException("You cannot open a session before initialising the document store. Did you forgot calling Initialise?");
            var session = new DocumentSession(this, storeListeners, deleteListeners);
			session.Stored += OnSessionStored;
            return session;
        }

#if !CLIENT
		public Raven.Database.DocumentDatabase DocumentDatabase { get; set; }
#endif

		public IDocumentStore Initialize()
		{
			try
			{
#if !CLIENT
				if (configuration != null)
				{
					DocumentDatabase = new Raven.Database.DocumentDatabase(configuration);
					DocumentDatabase.SpinBackgroundWorkers();
					databaseCommandsGenerator = () => new EmbededDatabaseCommands(DocumentDatabase, Conventions);
				}
				else
#endif
				{
					databaseCommandsGenerator = ()=>new ServerClient(Url, Conventions, credentials);
					asyncDatabaseCommandsGenerator = ()=>new AsyncServerClient(Url, Conventions, credentials);
				}
                if(Conventions.DocumentKeyGenerator == null)// don't overwrite what the user is doing
                {
                    var generator = new MultiTypeHiLoKeyGenerator(DatabaseCommands, 1024);
                    Conventions.DocumentKeyGenerator = entity => generator.GenerateDocumentKey(Conventions, entity);
                }
			}
			catch (Exception)
			{
				Dispose();
				throw;
			}

            return this;
		}

		public IDocumentStore RegisterListener(IDocumentDeleteListener deleteListener)
		{
			deleteListeners = deleteListeners.Concat(new[] {deleteListener}).ToArray();
			return this;
		}

#if !NET_3_5

		public IAsyncDocumentSession OpenAsyncSession()
		{
			if (DatabaseCommands == null)
				throw new InvalidOperationException("You cannot open a session before initialising the document store. Did you forgot calling Initialise?");
			if (AsyncDatabaseCommands == null)
				throw new InvalidOperationException("You cannot open an async session because it is not supported on embedded mode");

			var session = new AsyncDocumentSession(this, storeListeners, deleteListeners);
			session.Stored += OnSessionStored;
			return session;
		}
#endif
	}
}