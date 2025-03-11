#region Copyright & License

// Copyright © 2024 - 2025 Aprico Consultants
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#endregion

using System;
using System.Collections.Generic;
using System.Linq;
using Azure.Messaging.ServiceBus;
using Be.Stateless.Extensions;

namespace Aprico.Messaging.ServiceBus.Extensions;

internal static class ServiceBusMessageExtensions
{
	internal static IEnumerable<ServiceBusMessage> Validate(this IEnumerable<ServiceBusMessage> messages, int messageSize)
	{
		return messages.Select(m => m.Validate(messageSize));
	}

	/// <summary>
	/// Validates the essential properties of a <see cref="ServiceBusMessage"/> to ensure it meets outbox constraints and
	/// broker requirements.
	/// </summary>
	/// <param name="message">The <see cref="ServiceBusMessage"/> to validate.</param>
	/// <param name="messageSize">The maximum allowed size of the message body, in bytes.</param>
	/// <returns>The validated <see cref="ServiceBusMessage"/> instance.</returns>
	/// <exception cref="InvalidOperationException">
	/// Thrown if: <list type="bullet">
	/// <item><description><c>MessageId</c> is null, empty, or not a valid <see cref="Guid"/>.</description></item>
	/// <item>
	/// <description><c>ApplicationProperties</c> is missing the required <c>MessageBodyType</c> property or it's empty.</description>
	/// </item> <item><description><c>ApplicationProperties</c> is missing the required <c>Timestamp</c> property.</description></item>
	/// <item><description>The <c>Body</c> size exceeds <paramref name="messageSize"/>.</description></item>
	/// </list>
	/// </exception>
	/// <remarks>
	/// The validation currently only checks the size of the message body. Consider including serialized application
	/// properties in total size validation to match broker limits more precisely.
	/// </remarks>
	internal static ServiceBusMessage Validate(this ServiceBusMessage message, int messageSize)
	{
		// @formatter:wrap_chained_method_calls chop_if_long
		if (message.MessageId.IsNullOrEmpty()) throw new InvalidOperationException($"{nameof(ServiceBusMessage)} does not provide a value for property '{nameof(ServiceBusMessage.MessageId)}'.");

		if (!Guid.TryParse(message.MessageId, out _))
			throw new InvalidOperationException(
				$"{nameof(ServiceBusMessage)} {{ {nameof(ServiceBusMessage.MessageId)} : '{message.MessageId}' }}'s {nameof(ServiceBusMessage.MessageId)} is not a valid {nameof(Guid)} .");

		if (!message.ApplicationProperties.AsReadOnly().TryGetMessageBodyType(out var messageBodyType) || messageBodyType.IsNullOrEmpty())
			throw new InvalidOperationException(
				$"{nameof(ServiceBusMessage)} {{ {nameof(ServiceBusMessage.MessageId)} : '{message.MessageId}' }}'s {nameof(ServiceBusMessage.ApplicationProperties)} do not provide a value for property '{ApplicationPropertyNames.MessageBodyType}'.");

		if (!message.ApplicationProperties.AsReadOnly().TryGetTimestamp(out _))
			throw new InvalidOperationException(
				$"{nameof(ServiceBusMessage)} {{ {nameof(ServiceBusMessage.MessageId)} : '{message.MessageId}' }}'s {nameof(ServiceBusMessage.ApplicationProperties)} do not provide a value for property '{ApplicationPropertyNames.Timestamp}'.");

		if (message.Body.ToMemory().Length > messageSize)
			throw new InvalidOperationException(
				$"{nameof(ServiceBusMessage)} {{ {nameof(ServiceBusMessage.MessageId)} : '{message.MessageId}' }}'s {nameof(ServiceBusMessage.Body)} is too large.");

		// TODO ?? take message.ApplicationProperties.ToJson().ToMemory().Length into account ??
		// TODO watch out for performance and risk of multiple serializations of ApplicationProperties
		// if (message.Body.ToMemory().Length + message.ApplicationProperties.ToJson().ToMemory().Length > messageSize)
		// 	throw new InvalidOperationException($"{nameof(ServiceBusMessage)} {{ {nameof(ServiceBusMessage.MessageId)} : '{message.MessageId}' }} is too large.");

		return message;
	}
}
