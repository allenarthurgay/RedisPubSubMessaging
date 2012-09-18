using System;
using ServiceStack.Messaging;

namespace Olaf
{
	public class RemoteMqException : Exception
	{
		public MessageError Message { get; set; }

		public RemoteMqException(MessageError message) : base(message.Message)
		{
			Message = message;
		}
	}
}