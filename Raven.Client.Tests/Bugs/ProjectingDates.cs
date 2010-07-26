using System;
using System.Linq;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Tests.Document;
using Raven.Database.Indexing;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class ProjectingDates : BaseClientTest
	{
		[Fact]
		public void CanSaveCachedVery()
		{
			using(var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("Regs",
												new IndexDefinition<Registration, Registration>
				                                {
				                                	Map = regs => from reg in regs
																  select new { reg.RegisteredAt },
													Stores = { { x => x.RegisteredAt, FieldStorage.Yes } }
				                                });

				using(var session = store.OpenSession())
				{
					session.Store(new Registration
					{
						RegisteredAt = new DateTime(2010, 1, 1)
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var registration = session.LuceneQuery<Registration>("Regs")
						.SelectFields<Registration>("RegisteredAt")
						.WaitForNonStaleResults()
						.First();
					Assert.Equal(new DateTime(2010, 1, 1), registration.RegisteredAt);
					Assert.Null(registration.Id);
				}
			}
		}

		public class Registration
		{
			public string Id { get; set; }
			public DateTime RegisteredAt { get; set; }
		}
	}
}