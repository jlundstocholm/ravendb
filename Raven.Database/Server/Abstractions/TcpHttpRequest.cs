using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.Sockets;
using System.IO;
using System.Collections.Specialized;
using System.Web;
using System.Diagnostics;
using System.Threading;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Abstractions
{
    public class TcpHttpRequest : IHttpRequest
    {
        private readonly byte[] _bytes;
        private readonly object _sync = new object();
        private readonly Socket _socket;
        private NameValueCollection _headers = new NameValueCollection();
        private RequestLine _requestLine;
        private MemoryStream _stream;
        private bool _initialized;

        public TcpHttpRequest(Socket socket)
        {
            _bytes = new byte[socket.Available];
            _socket = socket;
            socket.Receive(_bytes);
            Initialize(_bytes);
        }

        public NameValueCollection Headers
        {
            get
            {
                return _headers;
            }
        }

        public Stream InputStream
        {
            get
            {
                return _stream;
            }
        }

        public NameValueCollection QueryString
        {
            get
            {
                return _requestLine.QueryString;
            }
        }

        public string HttpMethod
        {
            get
            {
                return _requestLine.HttpMethod;
            }
        }

        public Uri Url { get; private set; }

        public string RawUrl { get; private set; }

        private void Initialize(byte[] bytes)
        {
            var data = Encoding.UTF8.GetString(bytes);
            using (var reader = new StringReader(data))
            {
                RawUrl = _socket.LocalEndPoint.Serialize().ToString();
                _requestLine = new RequestLine(reader.ReadLine());
                _headers = new HttpHeaderParser(reader).Parse();

                if (!_headers.AllKeys.Contains("Expect"))
                {
                    _stream = new MemoryStream(Encoding.UTF8.GetBytes(reader.ReadToEnd()));
                }
                else
                {
                    _socket.Send(Encoding.UTF8.GetBytes("HTTP/1.1 100 Continue\r\n\r\n"));
                    while (!_socket.Poll(1000, SelectMode.SelectRead))
                    {
                        Thread.Sleep(10);
                    }
                    if (_socket.Available > 0)
                    {
                        var content = new byte[_socket.Available];
                        _socket.Receive(content, _socket.Available, SocketFlags.None);
                        _stream = new MemoryStream(content);
                    }
                    //_socket.Shutdown(SocketShutdown.Send);
                    //_socket.Close();
                }
            }


        }
    }
}
