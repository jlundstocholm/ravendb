using System;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class Etag : BaseClientTest
	{
		[Fact]
		public void WhenSaving_ThenGetsEtag()
		{
			using (var store = NewDocumentStore())
			{
				var foo = new IndexWithTwoProperties.Foo {Id = Guid.NewGuid().ToString(), Value = "foo"};

				using (var session = store.OpenSession())
				{
					session.Store(foo);

					session.SaveChanges();

					var metadata = session.GetMetadataFor(foo);
					Assert.NotNull(metadata.Value<string>("@etag"));
				}

				using (var session = store.OpenSession())
				{
					var loaded = session.Load<IndexWithTwoProperties.Foo>(foo.Id);

					var metadata = session.GetMetadataFor(loaded);
					Assert.NotNull(metadata.Value<string>("@etag"));

				}
			}
		}

	}
}