using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using Funq;
using Olaf;
using ServiceStack.Messaging;
using ServiceStack.Redis;
using ServiceStack.Redis.Messaging;
using ServiceStack.ServiceHost;
using ServiceStack.Text;

namespace TestClient
{
	public class MessageContext<TRequest>
	{
		public TRequest Request { get; set; }
		public int MessageId { get; set; }
	}
	public class Hello
	{
		public string Name { get; set; }
	}
	public class HelloResponse
	{
		public string Result { get; set; }
	}

	public class HelloService : IService<MessageContext<Hello>>
	{
		public object Execute(MessageContext<Hello> req)
		{
			Console.WriteLine("Service got a message:" + req.Dump());
			return new HelloResponse {Result = "Hello, " + req.Request.Name};
		}
	}

	public class Runner
	{
		public void Run()
		{
			var redisFactory = new PooledRedisClientManager("localhost:6379");

			var svc1 = CreateHost(redisFactory, "channel1");
			var svc2 = CreateHost(redisFactory, "channel1");
			var svc3 = CreateHost(redisFactory, "channel2");

			var cli1 = CreateClient();

			var msg1 = new MessageContext<Hello>
			{
				MessageId = 1,
				Request = new Hello { Name = "message1, channel1" }
			};
			var msg2 = new MessageContext<Hello>
			{
				MessageId = 2,
				Request = new Hello { Name = "message2, channel2" }
			};
			var msg3 = new MessageContext<Hello>
			{
				MessageId = 3,
				Request = new Hello { Name = "message3, channel1" }
			};

			cli1.PublishToChannel("channel1", msg1);
			cli1.PublishToChannel("channel2", msg2);
			cli1.PublishToChannel("channel1", msg3);
			var response = cli1.PublishToChannelAndWait<MessageContext<Hello>, HelloResponse>("channel1", msg3, TimeSpan.FromMilliseconds(1000));

			Console.WriteLine("response=" + response.Dump());
			
			Console.ReadLine();
		}

		private static ResponsiveChannelFriendlyRedisMqClient CreateClient()
		{
			var clientRedisFactory = new PooledRedisClientManager("localhost:6379");
			var clientHost = new RedisMqHost(clientRedisFactory);

			return new ResponsiveChannelFriendlyRedisMqClient(clientHost);
		}

		private static IMessageService CreateHost(PooledRedisClientManager redisFactory, string channel)
		{
			var mqHost = new RedisMqHost(redisFactory);

			mqHost.RegisterHandlerToChannel<MessageContext<Hello>>(channel, m => new HelloService().Execute(m.GetBody()));

			new Thread(mqHost.Start).Start(); //Starts listening for messages
			return mqHost;
		}
	}

	public class Program
	{
		static void Main(string[] args)
		{
			new Runner().Run();
		}
	}
}
