using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
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
			Console.WriteLine("Service got a message:" + req.MessageId);
			Console.WriteLine("Service got a message:" + req.Request.Name);
			return new HelloResponse {Result = "Hello, " + req.Request.Name};
		}
	}

	class Program
	{
		static void Main(string[] args)
		{
			var container = new Funq.Container();
			container.Register(c => new HelloService());
			var redisFactory = new PooledRedisClientManager("localhost:6379");

			var svc1 = CreateHost(redisFactory, container, "channel1");
			var svc2 = CreateHost(redisFactory, container, "channel1");
			var svc3 = CreateHost(redisFactory, container, "channel2");

			var cli1 = CreateClient();

			var msg = new MessageContext<Hello>
			{
				MessageId = 7,
				Request = new Hello { Name = "Client 1" }
			};

			cli1.PublishToChannel("channel2", msg);

			Console.ReadLine();
		}

		private static IMessageQueueClient CreateClient()
		{
			var clientRedisFactory = new PooledRedisClientManager("localhost:6379");
			var redisClient = clientRedisFactory.GetClient();
			var clientHost = new RedisMqHost(clientRedisFactory);
			var mqClient = clientHost.CreateMessageQueueClient();


			return mqClient;
		}

		private static IMessageService CreateHost(PooledRedisClientManager redisFactory, Container container, string channel)
		{
			var mqHost = new RedisMqHost(redisFactory);

			mqHost.RegisterHandlerToChannel<MessageContext<Hello>>(channel, m =>
				{
					Console.Out.WriteLine("channel=" + channel);
					container.Resolve<HelloService>().Execute(m.GetBody());
					return null;
				});

			mqHost.Start(); //Starts listening for messages
			return mqHost;
		}
	}

}
