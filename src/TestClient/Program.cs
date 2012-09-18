using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Messaging;
using ServiceStack.Redis;
using ServiceStack.Redis.Messaging;
using ServiceStack.ServiceHost;

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

			container.Register<HelloService>(c => new HelloService());

			var redisFactory = new PooledRedisClientManager("localhost:6379");
			var mqHost = new RedisMqHost(redisFactory);

			mqHost.RegisterHandler<MessageContext<Hello>>(m =>
				{
					container.Resolve<HelloService>().Execute(m.GetBody());
					return null;
				});

			mqHost.Start(); //Starts listening for messages

			var clientRedisFactory = new PooledRedisClientManager("localhost:6379");
			var clientHost = new RedisMqHost(clientRedisFactory);
			var mqClient = clientHost.CreateMessageQueueClient();

			var msg = new MessageContext<Hello>
				{
					MessageId = 7, Request = new Hello {Name = "Client 1"}
				};
			mqClient.Publish(msg);

			Console.ReadLine();
		}
	}
}
