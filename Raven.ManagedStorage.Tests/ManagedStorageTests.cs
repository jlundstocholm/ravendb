using System;
using System.IO;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Exceptions;
using Xunit;

namespace Raven.ManagedStorage.Tests
{
    public class ManagedStorageTests : IDisposable
    {
        [Fact]
        public void Can_put_a_document()
        {
            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                var key = Guid.NewGuid().ToString();
                const string data = @"{ 
                                '_id': 'ayende', 
                                'email': 'ayende@ayende.com', 
                                'projects': [ 
                                    'rhino mocks', 
                                    'nhibernate', 
                                    'rhino service bus', 
                                    'rhino divan db', 
                                    'rhino persistent hash table', 
                                    'rhino distributed hash table', 
                                    'rhino etl', 
                                    'rhino security', 
                                    'rampaging rhinos' 
                                ] 
                            }";

                const string metadata = "{ metadata : 1}";

                fs.AddDocument(key, data, null, metadata);
            }
        }

        [Fact]
        public void Can_put_and_get_a_document()
        {
            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                const string key = "test";
                const string data = "{data: 1}";
                const string metadata = "{ metadata: 1}";
                var etag = Guid.NewGuid();

                fs.AddDocument(key, data, etag, metadata);

                JsonDocument doc = fs.DocumentByKey(key);

                AssertDocumentsAreEqual(key, data, doc);
            }
        }

        [Fact]
        public void Can_put_and_get_a_document_across_instances()
        {
            const string key = "test";
            const string data = "{data: 1}";
            const string metadata = "{ metadata: 1}";

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                fs.AddDocument(key, data, null, metadata);
            }

            using (var fs2 = new RavenStorage(new RavenFileStore("data")))
            {
                var doc = fs2.DocumentByKey(key);

                Assert.NotNull(doc);
                AssertDocumentsAreEqual(key, data, doc);
            }
        }

        [Fact]
        public void Data_is_located_in_a_datastore()
        {
            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                const string key = "test";
                const string data = "{data: 1}";
                const string metadata = "{ metadata: 1}";
                var etag = Guid.NewGuid();

                fs.AddDocument(key, data, etag, metadata);

                using (var fs2 = new RavenStorage(new RavenFileStore("otherData")))
                {
                    JsonDocument doc = fs2.DocumentByKey(key);
                    Assert.Null(doc);
                }
            }
        }

        [Fact]
        public void Can_Put_And_Get_A_Document_If_Index_Deleted()
        {
            const string key = "test";
            const string data = "{data: 1}";
            const string metadata = "{ metadata: 1}";

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                // Add enough items to force a checkpoint record
                for (var i = 0; i < 100; i++)
                {
                    fs.AddDocument(key + i, data, null, metadata);
                }
            }

            File.Delete(Path.Combine("data", "index.raven"));

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                var doc = fs.DocumentByKey("test1");

                Assert.NotNull(doc);
                AssertDocumentsAreEqual("test1", data, doc);
            }
        }

        [Fact]
        public void Can_put_get_and_delete_a_document()
        {
            const string key = "test";
            const string data = "{data: 1}";
            const string metadata = "{ metadata: 1}";

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                fs.AddDocument(key, data, null, metadata);

                var doc = fs.DocumentByKey(key);

                AssertDocumentsAreEqual(key, data, doc);

                fs.DeleteDocument(key, doc.Etag);

                doc = fs.DocumentByKey(key);

                Assert.Null(doc);
            }
        }

        [Fact]
        public void Can_put_get_and_delete_a_document_across_stores()
        {
            const string key = "test";
            const string data = "{data: 1}";
            const string metadata = "{ metadata: 1}";
            Guid etag;

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                fs.AddDocument(key, data, null, metadata);

                var doc = fs.DocumentByKey(key);
                etag = doc.Etag;

                AssertDocumentsAreEqual(key, data, doc);
            }

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                fs.DeleteDocument(key, etag);

                var doc = fs.DocumentByKey(key);

                Assert.Null(doc);
            }

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                var doc = fs.DocumentByKey(key);

                Assert.Null(doc);
            }
        }

        [Fact]
        public void Can_put_multiple_documents_and_retrieve_them_correctly()
        {
            const string key1 = "test 1";
            const string data1 = "{data: 1}";
            const string metadata1 = "{ metadata: 1}";

            const string key2 = "test 2";
            const string data2 = "{data: 2}";
            const string metadata2 = "{ metadata: 2}";

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                fs.AddDocument(key1, data1, null, metadata1);
                fs.AddDocument(key2, data2, null, metadata2);

                var doc2 = fs.DocumentByKey(key2);
                var doc1 = fs.DocumentByKey(key1);

                AssertDocumentsAreEqual(key1, data1, doc1);
                AssertDocumentsAreEqual(key2, data2, doc2);
            }
        }

        [Fact]
        public void Can_put_multiple_versions_of_a_document_and_always_retrieve_latest()
        {
            const string key1 = "test 1";
            var data1 = "{data: 1}";
            var metadata1 = "{ metadata: 1}";

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                fs.AddDocument(key1, data1, null, metadata1);
                var doc1 = fs.DocumentByKey(key1);

                data1 = "{data: 2}";
                metadata1 = "{ metadata: 2}";

                fs.AddDocument(key1, data1, doc1.Etag, metadata1);

                doc1 = fs.DocumentByKey(key1);

                AssertDocumentsAreEqual(key1, data1, doc1);
            }
        }

        [Fact]
        public void Putting_a_new_version_of_a_document_with_the_wrong_etag_fails()
        {
            const string key1 = "test 1";
            var data1 = "{data: 1}";
            var metadata1 = "{ metadata: 1}";

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                fs.AddDocument(key1, data1, null, metadata1);

                data1 = "data 2";
                metadata1 = "{ metadata: 2}";

                Assert.Throws<ConcurrencyException>(() => fs.AddDocument(key1, data1, Guid.NewGuid(), metadata1));
            }
        }

        [Fact]
        public void Putting_a_new_version_of_a_document_with_a_null_etag_succeeds()
        {
            const string key1 = "test 1";
            string data1 = "{data: 1}";
            string metadata1 = "{ metadata: 1}";

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                fs.AddDocument(key1, data1, null, metadata1);

                data1 = "{data: 2}";
                metadata1 = "{ metadata: 2}";

                fs.AddDocument(key1, data1, null, metadata1);

                var doc = fs.DocumentByKey(key1);

                AssertDocumentsAreEqual(key1, data1, doc);
            }
        }

        [Fact]
        public void Deleting_a_document_with_the_wrong_etag_fails()
        {
            const string key1 = "test 1";
            const string data1 = "{data: 1}";
            const string metadata1 = "{ metadata: 1}";

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                fs.AddDocument(key1, data1, null, metadata1);

                Assert.Throws<ConcurrencyException>(() => fs.DeleteDocument(key1, Guid.NewGuid()));
            }
        }

        [Fact]
        public void Deleting_a_document_with_a_null_etag_succeeds()
        {
            const string key1 = "test 1";
            const string data1 = "{data: 1}";
            const string metadata1 = "{ metadata: 1}";

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                fs.AddDocument(key1, data1, null, metadata1);

                fs.DeleteDocument(key1, null);

                var doc = fs.DocumentByKey(key1);

                Assert.Null(doc);
            }
        }

        [Fact]
        public void Can_Recover_From_Corrupted_Data_File()
        {
            const string key1 = "test 1";
            const string data1 = "{data: 1}";
            const string metadata1 = "{ metadata: 1}";
            const string key2 = "test 2";
            const string data2 = "{data: 2}";
            const string metadata2 = "{ metadata: 2}";

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                fs.AddDocument(key1, data1, null, metadata1);
                fs.AddDocument(key2, data2, null, metadata2);
            }

            TruncateFile(Path.Combine("data", "data.raven"), 4);

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                var doc1 = fs.DocumentByKey(key1);
                var doc2 = fs.DocumentByKey(key2);

                AssertDocumentsAreEqual(key1, data1, doc1);
                Assert.Null(doc2);
            }
        }

        [Fact]
        public void Can_Recover_From_Corrupted_Index_File()
        {
            const string key1 = "test 1";
            const string data1 = "{data: 1}";
            const string metadata1 = "{ metadata: 1}";
            const string key2 = "test 2";
            const string data2 = "{data: 2}";
            const string metadata2 = "{ metadata: 2}";

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                fs.AddDocument(key1, data1, null, metadata1);
                fs.AddDocument(key2, data2, null, metadata2);
            }

            TruncateFile(Path.Combine("data", "index.raven"), 4);

            using (var fs = new RavenStorage(new RavenFileStore("data")))
            {
                var doc1 = fs.DocumentByKey(key1);
                var doc2 = fs.DocumentByKey(key2);

                AssertDocumentsAreEqual(key1, data1, doc1);
                AssertDocumentsAreEqual(key2, data2, doc2);
            }
        }


        /*
         * TODO
         * DocumentsById
         * DocumentKeys
         * FirstAndLastDocumentKeys
         * GetDocumentsCount
         * 
         * Tasks (Add, GetFirstTask, CompleteCurrentTask, DoesTasksExistsForIndex)
         * Attachements (Add, Delete, Get)
         * Checkpoint on dispose
         * Transactions
         * Critical Finalizers
         * Check MD5 hash
         * 
         * Stats (can store in control file and rebuild if necessary)
         *  How many docs
         *  How big
         *  Garbage size
         */

        public void Dispose()
        {
            CleanUp("data", "otherData");
        }

        private static void CleanUp(params string[] dataStores)
        {
            foreach (var store in dataStores)
            {
                RavenFileStore.Clear(store);
            }
        }

        private static void TruncateFile(string path, int bytesToTruncate)
        {
            var stream = File.Open(path, FileMode.Open);
            var buffer = new byte[stream.Length - bytesToTruncate];

            stream.Read(buffer, 0, buffer.Length);
            stream.Close();

            stream = File.Open(path, FileMode.Truncate);
            stream.Write(buffer, 0, buffer.Length);
            stream.Close();
        }


        private static void AssertDocumentsAreEqual(string key, string data, JsonDocument doc)
        {
            Assert.Equal(key, doc.Key);
            Assert.Equal(JObject.Parse(data).ToString(), doc.DataAsJson.ToString());
        }
    }


}