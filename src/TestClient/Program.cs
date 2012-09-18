using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Funq;
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

	public static class Extensions
	{
		static FieldInfo fieldInfo = typeof(RedisMqHost).GetField("handlerMap", BindingFlags.Instance | BindingFlags.NonPublic);

		public static void PublishToChannel<T>(this IMessageQueueClient client, string channel, T messageBody)
		{
			IMessage msg = typeof (IMessage).IsAssignableFrom(typeof (T)) ? (IMessage) messageBody : new Message<T>(messageBody);

			client.Publish(channel + "_" + QueueNames<T>.In, msg.ToBytes());
		}

		public static void RegisterHandlerToChannel<T>(this RedisMqHost mqHost, string channel, Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx = null)
		{
			var handlerMap = (Dictionary<Type, IMessageHandlerFactory>)fieldInfo.GetValue(mqHost);

			if (handlerMap.ContainsKey(typeof(T)))
			{
				throw new ArgumentException("Message handler has already been registered for type: " + typeof(T).Name);
			}

			handlerMap[typeof(T)] = mqHost.CreateMessageHandlerFactory(channel, processMessageFn, processExceptionEx);
		}
		
		private static MessageHandlerPerChannelFactory<T> CreateMessageHandlerFactory<T>(this RedisMqHost mqHost, string channel,
			Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx)
		{
			return new MessageHandlerPerChannelFactory<T>(mqHost, channel, processMessageFn, processExceptionEx)
			{
				RequestFilter = mqHost.RequestFilter,
				ResponseFilter = mqHost.ResponseFilter,
				RetryCount = mqHost.RetryCount,
			};
		}
	}

    public class MessageHandlerPerChannelFactory<T>
        : IMessageHandlerFactory
    {
        public const int DefaultRetryCount = 2; //Will be a total of 3 attempts
        private readonly IMessageService messageService;
	    private readonly string _channel;

	    public Func<IMessage, IMessage> RequestFilter { get; set; }
        public Func<object, object> ResponseFilter { get; set; }

        private readonly Func<IMessage<T>, object> processMessageFn;
        private readonly Action<IMessage<T>, Exception> processExceptionFn;
        public int RetryCount { get; set; }
		
        public MessageHandlerPerChannelFactory(IMessageService messageService, string channel, Func<IMessage<T>, object> processMessageFn, Action<IMessage<T>, Exception> processExceptionEx = null)
        {
            if (messageService == null)
                throw new ArgumentNullException("messageService");

	        if (channel == null) 
				throw new ArgumentNullException("channel");

	        if (processMessageFn == null)
                throw new ArgumentNullException("processMessageFn");

            this.messageService = messageService;
	        _channel = channel;
	        this.processMessageFn = processMessageFn;
            this.processExceptionFn = processExceptionEx;
            this.RetryCount = DefaultRetryCount;
        }

        public IMessageHandler CreateMessageHandler()
        {
	        var handler = MessageHandlerImpl();

	        handler.ProcessQueueNames = handler.ProcessQueueNames.Select(name => _channel + "_" + name).ToArray();

	        return handler;
        }

		private MessageHandler<T> MessageHandlerImpl()
	    {
		    if (this.RequestFilter == null && this.ResponseFilter == null)
		    {
			    return new MessageHandler<T>(messageService, processMessageFn,
			                                 processExceptionFn, this.RetryCount);
		    }

		    return new MessageHandler<T>(messageService, msg =>
			    {
				    if (this.RequestFilter != null)
					    msg = (IMessage<T>) this.RequestFilter(msg);

				    var result = this.processMessageFn(msg);

				    if (this.ResponseFilter != null)
					    result = this.ResponseFilter(result);

				    return result;
			    },
		                                 processExceptionFn, this.RetryCount);
	    }
    }
}
