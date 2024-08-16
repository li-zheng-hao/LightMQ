using System;
using System.Collections.Generic;
using LightMQ.Transport;
using Newtonsoft.Json;
using Xunit;

namespace MessageExtensionTests
{
    public class MessageExtensionTests
    {
        private Message message;

        public MessageExtensionTests()
        {
            message = new Message();
        }

        [Fact]
        public void TestSetHeader()
        {
            var header = new Dictionary<string, string>
            {
                { "Key1", "Value1" },
                { "Key2", "Value2" }
            };

            message.SetHeader(header);

            var expectedHeader = JsonConvert.SerializeObject(header);
            Assert.Equal(expectedHeader, message.Header);
        }

        [Fact]
        public void TestAddHeader()
        {
            message.SetHeader(new Dictionary<string, string> { { "Key1", "Value1" } });

            message.AddHeader("Key2", "Value2");

            var header = message.GetHeader();
            Assert.NotNull(header);
            Assert.Equal(2, header.Count);
            Assert.Equal("Value1", header["Key1"]);
            Assert.Equal("Value2", header["Key2"]);
        }
        [Fact]
        public void TestAddHeader2()
        {
            message.AddHeader("Key2", "Value2");

            var header = message.GetHeader();
            Assert.NotNull(header);
            Assert.Single(header);
            Assert.Equal("Value2", header["Key2"]);
        }

        [Fact]
        public void TestGetHeader()
        {
            var header = new Dictionary<string, string>
            {
                { "Key1", "Value1" }
            };

            message.SetHeader(header);

            var retrievedHeader = message.GetHeader();
            Assert.NotNull(retrievedHeader);
            Assert.Equal("Value1", retrievedHeader["Key1"]);
        }
    }
}