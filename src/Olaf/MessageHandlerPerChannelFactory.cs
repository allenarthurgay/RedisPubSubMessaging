using System;
using System.Linq;
using ServiceStack.Messaging;

namespace Olaf
{
	public class MessageHandlerPerChannelFactory<T> : IMessageHandlerFactory
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
					msg = (IMessage<T>)this.RequestFilter(msg);

				var result = this.processMessageFn(msg);

				if (this.ResponseFilter != null)
					result = this.ResponseFilter(result);

				return result;
			},
										 processExceptionFn, this.RetryCount);
		}
	}
}
