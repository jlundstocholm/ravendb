using System;
using Raven.Database;
using Raven.Database.Server;

namespace Raven.Server
{
	public class RavenDbServer : IDisposable
	{
		private readonly DocumentDatabase database;
		private readonly HttpServer server;

		public DocumentDatabase Database
		{
			get { return database; }
		}

		public HttpServer Server
		{
			get { return server; }
		}

		public RavenDbServer(RavenConfiguration settings)
		{
			settings.LoadLoggingSettings();
			database = new DocumentDatabase(settings);
			database.SpinBackgroundWorkers();
			server = new HttpServer(settings, database);
			server.Start();
		}

		#region IDisposable Members

		public void Dispose()
		{
			server.Dispose();
			database.Dispose();
		}

		#endregion

		public static void EnsureCanListenToWhenInNonAdminContext(int port)
		{
			if (CanStartHttpListener(port))
				return;

			var exit = TryGrantingHttpPrivileges(port);

			if (CanStartHttpListener(port) == false)
				Console.WriteLine("Failed to grant rights for listening to http, exit code: " + exit);
		}

		private static void GetArgsForHttpAclCmd(int port, out string args, out string cmd)
		{
			if (Environment.OSVersion.Version.Major > 5)
			{
				cmd = "netsh";
				args = string.Format(@"http add urlacl url=http://+:{0}/ user=""{1}""", port,
				                     WindowsIdentity.GetCurrent().Name);
			}
			else
			{
				cmd = "httpcfg";
				args = string.Format(@"set urlacl /u http://+:{0}/ /a D:(A;;GX;;;""{1}"")", port,
				                     WindowsIdentity.GetCurrent().User);
			}
		}

		private static bool CanStartHttpListener(int port)
		{
		    return true;
			try
			{
				var httpListener = new HttpListener();
				httpListener.Prefixes.Add("http://+:" + port + "/");
				httpListener.Start();
				httpListener.Stop();
				return true;
			}
			catch (HttpListenerException e)
			{
				if (e.ErrorCode != 5) //access denies
					throw;
			}
			return false;
		}

		private static int TryGrantingHttpPrivileges(int port)
		{
			string args;
			string cmd;
			GetArgsForHttpAclCmd(port, out args, out cmd);

			Console.WriteLine("Trying to grant rights for http.sys");
			try
			{
				Console.WriteLine("runas {0} {1}", cmd, args);
				var process = Process.Start(new ProcessStartInfo
				{
					Verb = "runas",
					Arguments = args,
					FileName = cmd,
				});
				process.WaitForExit();
				return process.ExitCode;
			}
			catch (Exception)
			{
				return -144;
			}
		}
	}
}