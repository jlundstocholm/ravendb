using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Collections.Specialized;

namespace Raven.Database.Server.Abstractions
{
    public class HttpHeaderParser
    {
        private readonly TextReader _reader;

        public HttpHeaderParser(TextReader reader)
        {
            _reader = reader;
        }

        public NameValueCollection Parse()
        {
            var headers = new NameValueCollection();
            string headerLine;
            while (!string.IsNullOrWhiteSpace(headerLine = _reader.ReadLine()))
            {
                var bits = headerLine.SplitPair(":");
                headers.Add(bits.Item1, bits.Item2.Trim());
            }

            return headers;
        }
    }
}
