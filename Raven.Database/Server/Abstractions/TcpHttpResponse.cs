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
        private readonly TcpClient _client;
        private readonly Func<Stream, Stream> _responseFilter;
        private readonly NameValueCollection _headers = new NameValueCollection();
        private readonly MemoryStream _outputStream = new MemoryStream();

        public TcpHttpResponse(TcpClient client) : this(client, null)
        {
        }

        public TcpHttpResponse(TcpClient client, Func<Stream, Stream> responseFilter)
        {
            _client = client;
            _responseFilter = responseFilter;
            StatusCode = 200;
            StatusDescription = "OK";
            ContentType = "text/text";

            Trace.WriteLine("TcpHttpResponse created...");
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
            Headers.Add("Location", url);
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
            byte[] buffer;
            try
            {
                buffer = GetBytes().ToArray();
                _client.GetStream().Write(buffer, 0, buffer.Length);
                Trace.WriteLine("TcpHttpResponse closed...");
            }
            catch (Exception ex)
            {
                Trace.WriteLine("TcpHttpResponse failed...");
                Trace.WriteLine(ex.ToString());
            }
            finally
            {
                _client.Close();
            }
        }

        private IEnumerable<byte> GetBytes()
        {

            byte[] outputBytes;

            if (_responseFilter == null)
            {
                outputBytes = _outputStream.ToArray();
            }
            else
            {
                using (var filteredStream = _responseFilter(_outputStream))
                {
                    outputBytes = new byte[filteredStream.Length];
                    filteredStream.Read(outputBytes, 0, (int) filteredStream.Length);
                }
            }
            _outputStream.Close();

            int contentLength = outputBytes.Length;
            bool chunked = !ContentType.StartsWith("image");

            var result = Encoding.UTF8.GetBytes(GetHeaderString(contentLength, chunked)).AsEnumerable();
            
            if (chunked)
            {
                result = result.Concat(Encoding.UTF8.GetBytes(contentLength.ToString("X") + "\r\n"))
                    .Concat(outputBytes)
                    .Concat(Encoding.UTF8.GetBytes("\r\n0\r\n\r\n"));
            }
            else
            {
                result = result.Concat(outputBytes);
            }

            return result;
        }

        private string GetHeaderString(int contentLength, bool chunked)
        {
            using (var writer = new StringWriter())
            {
                writer.WriteLine("HTTP/1.1 {0} {1}", StatusCode, StatusDescription);
//                writer.WriteLine("Server: TcpHttpWrapper 1.0");
                writer.WriteLine("Content-Type: {0}", ContentType);
                if (chunked)
                {
                    writer.WriteLine("Transfer-Encoding: chunked");
                }
                else
                {
                    writer.WriteLine("Content-Length: {0}", contentLength);
                }
                foreach (var key in Headers.AllKeys)
                {
                    writer.WriteLine("{0}: {1}", key, Headers[key]);
                }
                writer.WriteLine();

                return writer.ToString();
            }
        }

        private int FindStringContentLength(ref byte[] bytes)
        {
            string text;
            using (var memoryStream = new MemoryStream(bytes))
            {
                using (var reader = new StreamReader(memoryStream))
                {
                    text = reader.ReadToEnd();
                }
            }

            bytes = Encoding.UTF8.GetBytes(text);
            return text.Length;
        }
    }
}
