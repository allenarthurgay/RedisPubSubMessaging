using System;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;

namespace Olaf
{
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
			var msg = CoerceToIMessage(messageBody);

			Publish(BuildChannelName<T>(channel), msg.ToBytes());
		}

		private static IMessage CoerceToIMessage<T>(T messageBody)
		{
			if (typeof (IMessage).IsAssignableFrom(typeof (T)))
			{
				return (IMessage) messageBody;
			}

			return new Message<T>(messageBody);
		}

		public TResponse PublishToChannelAndWait<TRequest, TResponse>(string channel, TRequest messageBody, TimeSpan? timeOut = null, Action<IMessage<TResponse>> fondleResponse = null)
		{
			if (channel == null) throw new ArgumentNullException("channel");

			var msg = CoerceToIMessage(messageBody);

			msg.ReplyTo = "hostKey:" + Guid.NewGuid().ToString("N");

			Publish(BuildChannelName<TRequest>(channel), msg.ToBytes());
			
			var bytes = Get(msg.ReplyTo, timeOut);
			if(bytes == null)
			{
				return default(TResponse);
			}
			
			var message = bytes.ToMessage<TResponse>();
			if(message.Error != null)
			{
				throw new RemoteMqException(message.Error);
			}

			if(fondleResponse != null)
			{
				fondleResponse(message);
			}

			return message.GetBody();
		}

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