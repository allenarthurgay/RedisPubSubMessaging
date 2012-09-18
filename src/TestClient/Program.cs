using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using ServiceStack.Redis;
using ServiceStack.Redis.Messaging;
using ServiceStack.ServiceHost;

namespace TestClient
{
	public class Hello
	{
		public string Name { get; set; }
	}
	public class HelloResponse
	{
		public string Result { get; set; }
	}

	public class HelloService : IService<Hello>
	{
		public object Execute(Hello req)
		{
			Console.WriteLine("Service got a message:" + req.Name);
			return new HelloResponse {Result = "Hello, " + req.Name};
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

			mqHost.RegisterHandler<Hello>(m =>
			{
				container.Resolve<HelloService>().Execute(m.GetBody());
				return null;
			});

			mqHost.Start(); //Starts listening for messages

			var clientRedisFactory = new PooledRedisClientManager("localhost:6379");
			var clientHost = new RedisMqHost(clientRedisFactory);
			var mqClient = clientHost.CreateMessageQueueClient();
			mqClient.Publish(new Hello { Name = "Client 1" });

			Console.ReadLine();
		}
	}
}
