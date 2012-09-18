using ServiceStack.Messaging;

namespace Olaf
{
	public static class MessageQueueClientExtensions
	{
		public static void PublishToChannel<T>(this IMessageQueueClient client, string channel, T messageBody)
		{
			IMessage msg = typeof(IMessage).IsAssignableFrom(typeof(T)) ? (IMessage)messageBody : new Message<T>(messageBody);

			client.Publish(channel + "_" + QueueNames<T>.In, msg.ToBytes());
		}
	}
}