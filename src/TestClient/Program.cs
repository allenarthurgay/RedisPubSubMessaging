using System;
using System.Collections.Generic;
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
			//var msg2 = new MessageContext<Hello>
			//{
			//	MessageId = 21,
			//	Request = new Hello { Name = "Client 1" }
			//};

			cli1.PublishToChannel("channel2", msg);
			//cli1.PublishToChannelAndWait<MessageContext<Hello>, HelloResponse>("channel2", msg2, callback: response => Console.Out.WriteLine("response=" + response.Result));

			Console.ReadLine();
		}

		private static ResponsiveChannelFriendlyRedisMqClient CreateClient()
		{
			var clientRedisFactory = new PooledRedisClientManager("localhost:6379");
			var clientHost = new RedisMqHost(clientRedisFactory);

			return new ResponsiveChannelFriendlyRedisMqClient(clientHost);
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

	public class ResponsiveChannelFriendlyRedisMqClient : IMessageQueueClient
	{
		private RedisMqHost _host;
		private IMessageQueueClient _innerClient;


		public ResponsiveChannelFriendlyRedisMqClient(RedisMqHost host)
		{
			if (host == null) throw new ArgumentNullException("host");
			_host = host;

			_innerClient = _host.CreateMessageQueueClient();
		}

		private static string BuildChannelName<T>(string channel)
		{
			return channel + "_" + QueueNames<T>.In;
		}

		public void PublishToChannel<T>(string channel, T messageBody)
		{
			IMessage msg = typeof(IMessage).IsAssignableFrom(typeof(T)) ? (IMessage)messageBody : new Message<T>(messageBody);

			Publish(BuildChannelName<T>(channel), msg.ToBytes());
		}

		//public void PublishToChannelAndWait<TRequest, TResponse>(string channel, TRequest messageBody, Action<TResponse> callback = null, TimeSpan? timeOut = null)
		//{
		//	if (channel == null) throw new ArgumentNullException("channel");

		//	var msg = typeof (IMessage).IsAssignableFrom(typeof (TRequest))
		//				  ? (IMessage) messageBody
		//				  : new Message<TRequest>(messageBody);

		//	//msg.ReplyTo = "hostKey:" + Guid.NewGuid().ToString("N");

		//	PublishToChannel(channel, msg);

		//	new Thread(() =>
		//		{
		//			var message = Get(msg.ReplyTo, timeOut).ToMessage<TResponse>();

		//			if (callback != null)
		//			{
		//				callback(message.GetBody());
		//			}
		//		}).Start();
		//}

		public void Publish<T>(T messageBody)
		{
			_innerClient.Publish(messageBody);
		}

		public void Publish<T>(IMessage<T> message)
		{
			_innerClient.Publish(message);
		}

		public void Publish(string queueName, byte[] messageBytes)
		{
			_innerClient.Publish(queueName, messageBytes);
		}

		public void Notify(string queueName, byte[] messageBytes)
		{
			_innerClient.Notify(queueName, messageBytes);
		}

		public byte[] Get(string queueName, TimeSpan? timeOut)
		{
			return _innerClient.Get(queueName, timeOut);
		}

		public byte[] GetAsync(string queueName)
		{
			return _innerClient.GetAsync(queueName);
		}

		public string WaitForNotifyOnAny(params string[] channelNames)
		{
			return _innerClient.WaitForNotifyOnAny(channelNames);
		}

		public void Dispose()
		{
			_innerClient.Dispose();
		}
	}

}
