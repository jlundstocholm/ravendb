using System;
using System.Collections.Generic;
using System.Configuration.Install;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.Principal;
using System.ServiceProcess;
using log4net.Appender;
using log4net.Config;
using log4net.Filter;
using log4net.Layout;
using Raven.Database;
using Raven.Database.Server;
using System.Threading;

namespace Raven.Server
{
    internal class Program
    {
        private static string _dataDirectory;
        private static int _port;

        private static void Main(string[] args)
        {
            try
            {
                _dataDirectory = args[0];
                _port = int.Parse(args[1]);
                RunInDebugMode();
            }
            catch (ReflectionTypeLoadException e)
            {
                WriteLog(e.ToString());
                Console.WriteLine(e);
                foreach (var loaderException in e.LoaderExceptions)
                {
                    Console.WriteLine("- - - -");
                    Console.WriteLine(loaderException);
                    WriteLog("- - - -");
                    WriteLog(loaderException.ToString());
                }
                Thread.Sleep(1000);
                Environment.Exit(-1);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                WriteLog(e.ToString());
                Thread.Sleep(1000);
                Environment.Exit(-1);
            }
        }

        private static void RunInDebugMode()
        {
            //var consoleAppender = new ConsoleAppender
            //{
            //    Layout = new PatternLayout(PatternLayout.DefaultConversionPattern),
            //};
            //consoleAppender.AddFilter(new LoggerMatchFilter
            //{
            //    AcceptOnMatch = true,
            //    LoggerToMatch = typeof(HttpServer).FullName
            //});
            //consoleAppender.AddFilter(new DenyAllFilter());
            //BasicConfigurator.Configure(consoleAppender);
            var ravenConfiguration = new RavenConfiguration
                                         {
                                             DataDirectory = _dataDirectory,
                                             Port = _port,
                                             AnonymousUserAccessMode = AnonymousUserAccessMode.All
                                         };

        private static void RunInDebugMode(AnonymousUserAccessMode? anonymousUserAccessMode)
        {
            var consoleAppender = new ConsoleAppender
            {
                Layout = new PatternLayout(PatternLayout.DefaultConversionPattern),
            };
            consoleAppender.AddFilter(new LoggerMatchFilter
            {
                AcceptOnMatch = true,
                LoggerToMatch = typeof(HttpServer).FullName
            });
            consoleAppender.AddFilter(new DenyAllFilter());
            BasicConfigurator.Configure(consoleAppender);
            var ravenConfiguration = new RavenConfiguration();
            NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(ravenConfiguration.Port);
            if (anonymousUserAccessMode.HasValue)
                ravenConfiguration.AnonymousUserAccessMode = anonymousUserAccessMode.Value;

            using (new RavenDbServer(ravenConfiguration))
            {
                Console.WriteLine("Raven is ready to process requests.");
                Console.WriteLine("Data directory: {0}, Port: {1}", ravenConfiguration.DataDirectory, ravenConfiguration.Port);
                WriteLog("Raven is ready to process requests.");
                WriteLog("Data directory: {0}, Port: {1}", ravenConfiguration.DataDirectory, ravenConfiguration.Port);
                while (true)
                {
                    Thread.Sleep(60000);
                }
            }
        }

        private static void WriteLog(string format, params object[] args)
        {
            using (var writer = new StreamWriter(Path.Combine(_dataDirectory, "log.txt"), true))
            {
                writer.WriteLine(format, args);
            }
        }
    }
}