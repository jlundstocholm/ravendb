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
        private readonly TcpClient _client;
        private readonly int _port;
        private NameValueCollection _headers = new NameValueCollection();
        private RequestLine _requestLine;
        private MemoryStream _stream;

        public TcpHttpRequest(TcpClient client, int port)
        {
            _client = client;
            _port = port;
            Initialize(GetIncomingData(1000));
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

        public Uri Url { get { return new Uri(RawUrl); } }

        public string RawUrl { get { return string.Format("http://localhost:{0}{1}", _port, _requestLine.Path); } }

        private void Initialize(byte[] bytes)
        {
            var data = Encoding.UTF8.GetString(bytes);
            using (var reader = new StringReader(data))
            {
                _requestLine = new RequestLine(reader.ReadLine());

                _headers = new HttpHeaderParser(reader).Parse();

                if (!_headers.AllKeys.Contains("Expect"))
                {
                    _stream = new MemoryStream(Encoding.UTF8.GetBytes(reader.ReadToEnd()));
                }
                else
                {
                    ProcessExpectContinue();
                    _headers.Remove("Expect");
                }
            }
        }

        private void ProcessExpectContinue()
        {
            var bytes = Encoding.UTF8.GetBytes("HTTP/1.1 100 Continue\r\n\r\n");
            _client.GetStream().Write(bytes, 0, bytes.Length);
            _stream = new MemoryStream(GetIncomingData(1000));
        }

        private byte[] GetIncomingData(int timeout)
        {
            if (!_client.GetStream().DataAvailable)
            {
                if (Timeout(timeout))
                {
                    var buffer = Encoding.UTF8.GetBytes("COME ON THEN");
                    _client.GetStream().Write(buffer, 0, buffer.Length);
                    if (Timeout(timeout))
                    {
                        return new byte[0];
//                        throw new Exception("No data");
                    }
                }
            }
            using (var ms = new MemoryStream())
            {
                while (_client.GetStream().DataAvailable)
                {
                    ms.WriteByte((byte)_client.GetStream().ReadByte());
                }
                return ms.ToArray();
            }
        }

        private bool Timeout(int timeout)
        {
            int elapsed = 0;
            while (elapsed < timeout && _client.Available == 0)
            {
                elapsed += 10;
                Thread.Sleep(10);
            }
            return (elapsed >= timeout);
        }
    }
}
