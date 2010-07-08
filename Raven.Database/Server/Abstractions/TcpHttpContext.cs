using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.Security.Principal;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Abstractions
{
    public class TcpHttpContext : IHttpContext
    {
        private readonly RavenConfiguration _configuration;
        private readonly Socket _socket;
        private readonly TcpHttpRequest _request;
        private readonly TcpHttpResponse _response;

        public TcpHttpContext(Socket socket, RavenConfiguration configuration)
        {
            _socket = socket;
            _configuration = configuration;
            _request = new TcpHttpRequest(_socket);
            _response = new TcpHttpResponse(_socket);
        }

        public RavenConfiguration Configuration
        {
            get { return _configuration; }
        }

        public IHttpResponse Response
        {
            get { return _response; }
        }

        public IHttpRequest Request
        {
            get { return _request; }
        }

        public IPrincipal User
        {
            get
            {
                return new GenericPrincipal(WindowsIdentity.GetAnonymous(), new string[0]);
            }
        }

        public void FinalizeResponse()
        {
            _socket.Shutdown(SocketShutdown.Both);
            _socket.Close();
        }
    }
}
