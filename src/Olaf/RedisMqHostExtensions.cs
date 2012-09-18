using System;
using System.Collections.Generic;
using System.Reflection;
using ServiceStack.Messaging;
using ServiceStack.Redis.Messaging;

namespace Olaf
{
	public static class RedisMqHostExtensions
	{
		static FieldInfo fieldInfo = typeof(RedisMqHost).GetField("handlerMap", BindingFlags.Instance | BindingFlags.NonPublic);

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
}