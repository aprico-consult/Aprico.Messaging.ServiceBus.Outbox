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
using System.Linq;
using System.Security.Cryptography;
using Aprico.Messaging.ServiceBus.Dummies;
using Aprico.Messaging.ServiceBus.Outbox.Settings;
using Aprico.Messaging.ServiceBus.Xml;
using Azure.Messaging.ServiceBus;

namespace Aprico.Messaging.ServiceBus.Extensions;

public abstract class ServiceBusMessageExtensionsFixture
{
	#region Nested Type: ValidateEnumerableOfServiceBusMessages

	public class ValidateEnumerableOfServiceBusMessages : ServiceBusMessageExtensionsFixture
	{
		[Fact]
		public void ReturnsMessagesWhenValid()
		{
			var message1 = new ServiceBusMessageAssembler().Assemble(
				new XmlDummy {
					Name = "Test"
				});
			var message2 = new ServiceBusMessageAssembler().Assemble(
				new XmlDummy {
					Name = "Test"
				});
			ServiceBusMessage[] messages = [message1, message2];

			var sut = messages.Validate(OutboxSettings.DEFAULT_MAX_MESSAGE_SIZE);

			sut.ToList()
				.Should()
				.BeEquivalentTo([message1, message2]);
		}

		[Fact]
		public void ThrowsWhenAnyMessageIsNotValid()
		{
			var message1 = new ServiceBusMessageAssembler().Assemble(
				new XmlDummy {
					Name = "Test"
				});
			var message2 = new ServiceBusMessage();
			ServiceBusMessage[] messages = [message1, message2];

			var sut = messages.Validate(OutboxSettings.DEFAULT_MAX_MESSAGE_SIZE);

			Invoking(() => sut.ToList())
				.Should()
				.ThrowExactly<InvalidOperationException>()
				.WithMessage($"{nameof(ServiceBusMessage)} does not provide a value for property '{nameof(ServiceBusMessage.MessageId)}'.");
		}
	}

	#endregion

	#region Nested Type: ValidateIndividualServiceBusMessage

	public class ValidateIndividualServiceBusMessage : ServiceBusMessageExtensionsFixture
	{
		[Fact]
		public void ReturnsMessageWhenValid()
		{
			var message = new ServiceBusMessageAssembler().Assemble(
				new XmlDummy {
					Name = "Test"
				});

			message.Validate(OutboxSettings.DEFAULT_MAX_MESSAGE_SIZE)
				.Should()
				.BeSameAs(message);
		}

		[Fact(Skip = "TODO")]
		public void ThrowsWhenMessageBodyAndHeadersAreTooLarge()
		{
			Assert.Fail();
		}

		[Fact]
		public void ThrowsWhenMessageBodyIsTooLarge()
		{
			var message = new ServiceBusMessageAssembler().Assemble(
				new XmlDummy {
					Name = RandomNumberGenerator.GetHexString(OutboxSettings.DEFAULT_MAX_MESSAGE_SIZE)
				});
			Invoking(() => message.Validate(OutboxSettings.DEFAULT_MAX_MESSAGE_SIZE))
				.Should()
				.ThrowExactly<InvalidOperationException>()
				.WithMessage($"{nameof(ServiceBusMessage)} {{ {nameof(ServiceBusMessage.MessageId)} : '{message.MessageId}' }}'s {nameof(ServiceBusMessage.Body)} is too large.");
		}

		[Fact]
		public void ThrowsWhenMessageIdIsNotValidGuid()
		{
			var message = new ServiceBusMessage {
				MessageId = "id"
			};
			Invoking(() => message.Validate(OutboxSettings.DEFAULT_MAX_MESSAGE_SIZE))
				.Should()
				.ThrowExactly<InvalidOperationException>()
				.WithMessage(
					$"{nameof(ServiceBusMessage)} {{ {nameof(ServiceBusMessage.MessageId)} : '{message.MessageId}' }}'s {nameof(ServiceBusMessage.MessageId)} is not a valid {nameof(Guid)} .");
		}

		[Fact]
		public void ThrowsWhenMessageIsMissingId()
		{
			var message = new ServiceBusMessage();
			Invoking(() => message.Validate(OutboxSettings.DEFAULT_MAX_MESSAGE_SIZE))
				.Should()
				.ThrowExactly<InvalidOperationException>()
				.WithMessage($"{nameof(ServiceBusMessage)} does not provide a value for property '{nameof(ServiceBusMessage.MessageId)}'.");
		}

		[Fact]
		public void ThrowsWhenMessageIsMissingMessageBodyType()
		{
			var message = new ServiceBusMessage {
				MessageId = $"{Guid.NewGuid()}"
			};
			Invoking(() => message.Validate(OutboxSettings.DEFAULT_MAX_MESSAGE_SIZE))
				.Should()
				.ThrowExactly<InvalidOperationException>()
				.WithMessage(
					$"{nameof(ServiceBusMessage)} {{ {nameof(ServiceBusMessage.MessageId)} : '{message.MessageId}' }}'s {nameof(ServiceBusMessage.ApplicationProperties)} do not provide a value for property '{ApplicationPropertyNames.MessageBodyType}'.");
		}

		[Fact]
		public void ThrowsWhenMessageIsMissingTimestamp()
		{
			var message = new ServiceBusMessage {
				MessageId = $"{Guid.NewGuid()}"
			};
			message.ApplicationProperties.Add(ApplicationPropertyNames.MessageBodyType, "body-type");
			Invoking(() => message.Validate(OutboxSettings.DEFAULT_MAX_MESSAGE_SIZE))
				.Should()
				.ThrowExactly<InvalidOperationException>()
				.WithMessage(
					$"{nameof(ServiceBusMessage)} {{ {nameof(ServiceBusMessage.MessageId)} : '{message.MessageId}' }}'s {nameof(ServiceBusMessage.ApplicationProperties)} do not provide a value for property '{ApplicationPropertyNames.Timestamp}'.");
		}
	}

	#endregion
}
