using System;
using System.IO;

namespace Raven.ManagedStorage
{
    public class StorageSerializer
    {
        private const int GuidLength = 16;

        private readonly RavenDocument _document;
        private readonly RavenReadStream _reader;

        public StorageSerializer(RavenReadStream reader)
        {
            _reader = reader;
        }

        public StorageSerializer(RavenDocument document)
        {
            _document = document;
        }

        public RavenDocument Deserialize()
        {
            RavenDocument doc = DeserializeDocument(_reader);

            // TODO - read & checBk hash
            return doc;
        }

        public byte[] Serialize()
        {
            int length = GetDocumentLength() + Checksum.ChecksumLength;

            var buffer = new byte[length];
            var ms = new MemoryStream(buffer);
            var writer = new RavenWriter(ms);

            SerializeDocument(writer);

            writer.Write(Checksum.CalculateChecksum(buffer, 0, (int)ms.Position));

            return buffer;
        }

        private int GetDocumentLength()
        {
            return RavenWriter.GetLengthPrefixedStringLength(_document.Key) +
                   GuidLength +
                   RavenWriter.Get7BitEncodingLength(_document.Id) +
                   RavenWriter.Get7BitEncodingLength(_document.Data.Length) +
                   _document.Data.Length +
                   RavenWriter.GetLengthPrefixedStringLength(_document.MetaData);
        }

        private void SerializeDocument(RavenWriter writer)
        {
            writer.Write(_document.Key);
            writer.Write(_document.ETag.ToByteArray());
            writer.Write7BitInteger(_document.Id);
            writer.Write7BitInteger(_document.Data.Length);
            writer.Write(_document.Data);
            writer.Write(_document.MetaData);
        }

        private RavenDocument DeserializeDocument(RavenReadStream reader)
        {
            var doc = new RavenDocument();

            doc.Key = reader.ReadString();

            // TODO - add read/write Guid to RavenStream
            var guidBuf = new byte[16];
            reader.Read(guidBuf, 16);
            doc.ETag = new Guid(guidBuf);

            doc.Id = reader.Read7BitInteger();

            int dataLength = reader.Read7BitInteger();
            // TODO - why hold doc as byte[]? Why not string?
            doc.Data = new byte[dataLength];
            reader.Read(doc.Data, doc.Data.Length);

            doc.MetaData = reader.ReadString();

            // Read MD5
            reader.Read(guidBuf, 16);

            return doc;
        }
    }
}