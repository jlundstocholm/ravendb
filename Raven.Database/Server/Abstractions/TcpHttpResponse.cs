using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Specialized;
using System.IO;
using System.Net.Sockets;
using System.Diagnostics;

namespace Raven.Database.Server.Abstractions
{
    public class TcpHttpResponse : IHttpResponse
    {
        private readonly Socket _socket;
        private readonly NameValueCollection _headers = new NameValueCollection();
        private readonly MemoryStream _outputStream = new MemoryStream();

        public TcpHttpResponse(Socket socket)
        {
            _socket = socket;
        }

        public NameValueCollection Headers
        {
            get { return _headers; }
        }

        public Stream OutputStream
        {
            get { return _outputStream; }
        }

        public long ContentLength64 { get; set; }

        public int StatusCode { get; set; }

        public string StatusDescription { get; set; }

        public string ContentType { get; set; }

        public void Redirect(string url)
        {
            StatusCode = 302;
            StatusDescription = "Found";
            ContentType = "text/html";
            Headers.Add("Location", "http://www.google.com/");
            Close();
        }

        public void Write(string data)
        {
            using (var writer = new StreamWriter(_outputStream))
            {
                writer.Write(data);
            }
        }

        public void Close()
        {
            try
            {
                _socket.Send(GetBytes().ToArray());
                _socket.Shutdown(SocketShutdown.Both);
                _socket.Close();

            }
            catch (Exception)
            {
            }
        }

        private IEnumerable<byte> GetBytes()
        {
            var outputBytes = _outputStream.ToArray();
            _outputStream.Close();
            int contentLength = Encoding.UTF8.GetString(outputBytes).Length;
            return Encoding.UTF8.GetBytes(GetHeaderString(contentLength)).Concat(outputBytes);
        }

        private string GetHeaderString(int contentLength)
        {
            using (var writer = new StringWriter())
            {
                writer.WriteLine("HTTP/1.1 {0} {1}", StatusCode, StatusDescription);
                writer.WriteLine("Server: TcpHttpWrapper 1.0");
                writer.WriteLine("Content-Length: {0}", contentLength);
                writer.WriteLine("Content-Type: {0}", ContentType);
                foreach (var key in Headers.AllKeys)
                {
                    writer.WriteLine("{0}: {1}", key, Headers[key]);
                }
                writer.WriteLine();

                return writer.ToString();
            }
        }
    }
}
