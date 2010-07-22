using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using System.Text;
using System.Web;

namespace Raven.Database.Server.Abstractions
{
    public class RequestLine
    {
        private readonly string _httpMethod;
        private string _path;
        private NameValueCollection _queryString;
        private readonly string _protocol;

        public RequestLine(string source)
        {
            var bits = source.Split(' ');
            _httpMethod = bits[0];
            bits[1].SplitPair("?").Do((path, queryString) =>
            {
                _path = path;
                while (_path.StartsWith("//"))
                {
                    _path = _path.Substring(1);
                }
                _queryString = HttpUtility.ParseQueryString(queryString);
            });
            _protocol = bits[2];

        }

        public string Protocol
        {
            get { return _protocol; }
        }

        public NameValueCollection QueryString
        {
            get { return _queryString; }
        }

        public string Path
        {
            get { return _path; }
        }

        public string HttpMethod
        {
            get { return _httpMethod; }
        }
    }
}
