using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Net.Sockets;
using System.Diagnostics;

namespace Raven.Database.Server.Abstractions
{
    public class TcpHttpListener
    {
        private readonly RavenConfiguration _configuration;
        private readonly Subject<IHttpContext> _requests = new Subject<IHttpContext>();
        private readonly TcpListener _listener;

        public TcpHttpListener(RavenConfiguration configuration)
        {
            _configuration = configuration;
            _listener = new TcpListener(new IPAddress(0L), _configuration.Port);
        }

        public IObservable<IHttpContext> Requests
        {
            get { return _requests; }
        }

        public void Start()
        {
            _listener.Start();
            _listener.BeginAcceptTcpClient(AcceptClient, null);
        }

        public void Stop()
        {
            _listener.Stop();
        }

        private void AcceptClient(IAsyncResult asyncResult)
        {
            try
            {
                var client = _listener.EndAcceptTcpClient(asyncResult);
                _listener.BeginAcceptTcpClient(AcceptClient, null);
                _requests.OnNext(new TcpHttpContext(client, _configuration));
            }
            catch (ObjectDisposedException)
            {
                //NoOp - happens after calling TcpListener.Stop
            }
            catch (Exception ex)
            {
                Trace.WriteLine(ex.ToString());
            }
        }
    }
}
