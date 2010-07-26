﻿using System;
using Raven.Client.Client;
using Raven.Client.Document;

namespace Raven.Client
{
    public interface IDocumentStore : IDisposable
    {
		event Action<string, object> Stored;
		
		string Identifier { get; set; }

        IDocumentStore Initialize();

    	IDocumentStore RegisterListener(IDocumentDeleteListener deleteListener);

		IDocumentStore RegisterListener(IDocumentStoreListener documentStoreListener);

        IDocumentSession OpenSession();

        IDatabaseCommands DatabaseCommands { get; }

    	DocumentConvention Conventions { get; }
    }
}
