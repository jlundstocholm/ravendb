using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Newtonsoft.Json.Linq;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Linq;
using Raven.Database.Storage;
using Raven.Database.Storage.StorageActions;

namespace Raven.Database.Indexing
{
	public class SimpleIndex : Index
	{
		[CLSCompliant(false)]
		public SimpleIndex(Directory directory, string name, IndexDefinition indexDefinition)
			: base(directory, name, indexDefinition)
		{
		}

		public override void IndexDocuments(
			AbstractViewGenerator viewGenerator, 
			IEnumerable<object> documents,
			WorkContext context,
			IStorageActionsAccessor actions)
		{
			actions.Indexing.SetCurrentIndexStatsTo(name);
			var count = 0;
			Write(indexWriter =>
			{
				bool madeChanges = false;
				PropertyDescriptorCollection properties = null;
				var processedKeys = new HashSet<string>();
				var documentsWrapped = documents.Select((dynamic doc) =>
				{
					var documentId = doc.__document_id.ToString();
					if (processedKeys.Add(documentId) == false)
						return doc;
					madeChanges = true;
					context.IndexUpdateTriggers.Apply(trigger => trigger.OnIndexEntryDeleted(name, documentId));
					indexWriter.DeleteDocuments(new Term("__document_id", documentId));
					return doc;
				});
				foreach (var doc in RobustEnumeration(documentsWrapped, viewGenerator.MapDefinition, actions, context))
				{
					count++;

				    string newDocId;
				    IEnumerable<AbstractField> fields;
                    if (doc is DynamicJsonObject)
                        fields = ExtractIndexDataFromDocument((DynamicJsonObject) doc, out newDocId);
                    else
                        fields = ExtractIndexDataFromDocument(properties, doc, out newDocId);
				   
                    if (newDocId != null)
                    {
                        var luceneDoc = new Document();
                        luceneDoc.Add(new Field("__document_id", newDocId, Field.Store.YES, Field.Index.NOT_ANALYZED));

                    	madeChanges = true;
                        CopyFieldsToDocument(luceneDoc, fields);
                        context.IndexUpdateTriggers.Apply(trigger => trigger.OnIndexEntryCreated(name, newDocId, luceneDoc));
                        log.DebugFormat("Index '{0}' resulted in: {1}", name, luceneDoc);
                        indexWriter.AddDocument(luceneDoc);
                    }

					actions.Indexing.IncrementSuccessIndexing();
				}

				return madeChanges;
			});
			log.DebugFormat("Indexed {0} documents for {1}", count, name);
		}

        private IEnumerable<AbstractField> ExtractIndexDataFromDocument(DynamicJsonObject dynamicJsonObject, out string newDocId)
        {
            newDocId = dynamicJsonObject.GetDocumentId();
            return AnonymousObjectToLuceneDocumentConverter.Index(dynamicJsonObject.Inner, indexDefinition,
                                                                  Field.Store.NO);
        }

	    private IEnumerable<AbstractField> ExtractIndexDataFromDocument(PropertyDescriptorCollection properties, object doc, out string newDocId)
	    {
	        if (properties == null)
	        {
	            properties = TypeDescriptor.GetProperties(doc);
	        }
	        newDocId = properties.Find("__document_id", false).GetValue(doc) as string;
            return AnonymousObjectToLuceneDocumentConverter.Index(doc, properties, indexDefinition, Field.Store.NO);
	    }

	    private static void CopyFieldsToDocument(Document luceneDoc, IEnumerable<AbstractField> fields)
		{
			foreach (var field in fields)
			{
				luceneDoc.Add(field);
			}
		}

		public override void Remove(string[] keys, WorkContext context)
		{
			Write(writer =>
			{
				if (log.IsDebugEnabled)
				{
					log.DebugFormat("Deleting ({0}) from {1}", string.Format(", ", keys), name);
				}
                keys.Apply(key => context.IndexUpdateTriggers.Apply(trigger => trigger.OnIndexEntryDeleted(name, key)));
				writer.DeleteDocuments(keys.Select(k => new Term("__document_id", k)).ToArray());
				return true;
			});
		}
	}
}