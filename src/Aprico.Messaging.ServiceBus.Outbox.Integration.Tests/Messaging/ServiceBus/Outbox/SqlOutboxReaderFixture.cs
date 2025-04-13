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
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Aprico.AutoFixture.Xunit2;
using Aprico.Messaging.ServiceBus.Dummies;
using Aprico.Messaging.ServiceBus.Outbox.Settings;
using Aprico.Messaging.ServiceBus.Xml;
using Aprico.Xunit;
using AutoFixture.AutoMoq;
using Microsoft.Extensions.Options;

namespace Aprico.Messaging.ServiceBus.Outbox;

[Collection(nameof(OutboxTestDbFixture))]
[SuppressMessage("Design", "CA1063:Implement IDisposable Correctly")]
public class SqlOutboxReaderFixture : IClassFixture<OutboxTestDbFixture>, IDisposable
{
	#region Setup/Teardown

	public SqlOutboxReaderFixture(OutboxTestDbFixture outboxTestDbFixture)
	{
		_outboxTestDbFixture = outboxTestDbFixture;
		_connection = _outboxTestDbFixture.CreateConnection();
		_connection.Open();
		_transaction = _connection.BeginTransaction();
	}

	[SuppressMessage("Usage", "CA1816:Dispose methods should call SuppressFinalize")]
	public void Dispose()
	{
		_transaction.Dispose();
		_connection.Dispose();
		_outboxTestDbFixture.ClearMessages();
	}

	#endregion

	[Theory]
	[AutoData<AutoMoqCustomization>]
	public async Task DequeueMessagesByOldestSubject(SqlOutboxReader sut, string oldestSubject, string newestSubject)
	{
		var newestMessage = new ServiceBusMessageAssembler().Assemble(
			new XmlDummy {
				Name = $"{Guid.NewGuid():D}"
			},
			timestamp: DateTime.UtcNow);
		await _outboxTestDbFixture.InsertMessage(newestSubject, newestMessage);
		var oldestMessage = new ServiceBusMessageAssembler().Assemble(
			new XmlDummy {
				Name = $"{Guid.NewGuid():D}"
			},
			timestamp: DateTime.UtcNow.AddMinutes(value: -10));
		await _outboxTestDbFixture.InsertMessage(oldestSubject, oldestMessage);

		var (dequeuedSubject, dequeuedMessages) = await sut.DequeueAsync(_transaction);

		dequeuedSubject.Should()
			.Be(oldestSubject);
		dequeuedMessages.Should()
			.HaveCount(expected: 1)
			.And.Subject.Single()
			.MessageId.Should()
			.Be(oldestMessage.MessageId);
	}

	[Theory]
	[AutoData<AutoMoqCustomization>]
	public async Task DequeueMessagesByOldestSubjectButSkipOverSizedMessage(SqlOutboxReader sut, string oldestSubject, string newestSubject)
	{
		var newestMessage = new ServiceBusMessageAssembler().Assemble(
			new XmlDummy {
				Name = $"{Guid.NewGuid():D}"
			},
			timestamp: DateTime.UtcNow);
		await _outboxTestDbFixture.InsertMessage(newestSubject, newestMessage);
		var oldestMessage = new ServiceBusMessageAssembler().Assemble(
			new XmlDummy {
				Name = RandomNumberGenerator.GetHexString(OutboxSettings.DEFAULT_MAX_MESSAGE_SIZE + 1024)
			},
			timestamp: DateTime.UtcNow.AddMinutes(value: -10));
		await _outboxTestDbFixture.InsertMessage(oldestSubject, oldestMessage);

		var (dequeuedSubject, dequeuedMessages) = await sut.DequeueAsync(_transaction);

		dequeuedSubject.Should()
			.Be(newestSubject);
		dequeuedMessages.Should()
			.HaveCount(expected: 1)
			.And.Subject.Single()
			.MessageId.Should()
			.Be(newestMessage.MessageId);
	}

	[Theory]
	[AutoData<AutoMoqCustomization>]
	public async Task DequeueReturnsEmptyWhenStoreIsEmpty(SqlOutboxReader sut)
	{
		var dequeueResult = await sut.DequeueAsync(_transaction);

		dequeueResult.Should()
			.NotBeNull();
		dequeueResult.Subject.Should()
			.NotBeNull()
			.And.BeEmpty();
		dequeueResult.Messages.Should()
			.NotBeNull()
			.And.BeEmpty();
	}

	[Theory]
	[AutoData<AutoMoqCustomization>]
	public async Task DequeueReturnsMessagesUpToDefaultMessageCount(SqlOutboxReader sut, string subject)
	{
		for (var i = 0; i < OutboxSettings.DEFAULT_MAX_DEQUEUE_COUNT + 1; i++)
		{
			var message = new ServiceBusMessageAssembler().Assemble(
				new XmlDummy {
					Name = $"{Guid.NewGuid():D}"
				});
			await _outboxTestDbFixture.InsertMessage(subject, message);
		}

		var (dequeuedSubject, dequeuedMessages) = await sut.DequeueAsync(_transaction);

		dequeuedSubject.Should()
			.Be(subject);
		dequeuedMessages.Should()
			.HaveCount(OutboxSettings.DEFAULT_MAX_DEQUEUE_COUNT);
	}

	[Theory]
	[AutoData<AutoMoqCustomization>]
	public async Task DequeueReturnsMessagesUpToMaxDequeueSize(SqlOutboxReader sut, string subject)
	{
		const int PAYLOAD_SPLIT_FACTOR = 3;
		for (var i = 0; i < PAYLOAD_SPLIT_FACTOR; i++)
		{
			var message = new ServiceBusMessageAssembler().Assemble(
				new XmlDummy {
					Name = RandomNumberGenerator.GetHexString(OutboxSettings.DEFAULT_MAX_DEQUEUE_SIZE / PAYLOAD_SPLIT_FACTOR)
				});
			await _outboxTestDbFixture.InsertMessage(subject, message);
		}

		var (firstDequeuedSubject, firstDequeuedMessages) = await sut.DequeueAsync(_transaction);
		var (secondDequeuedSubject, secondDequeuedMessages) = await sut.DequeueAsync(_transaction);

		firstDequeuedSubject.Should()
			.Be(subject);
		firstDequeuedMessages.Should()
			// subtracting 1 because actual payload size includes headers, which reduces the number of fully acceptable messages
			.HaveCount(PAYLOAD_SPLIT_FACTOR - 1);
		secondDequeuedSubject.Should()
			.Be(subject);
		secondDequeuedMessages.Should()
			.HaveCount(expected: 1);
	}

	[Theory]
	[AutoData<AutoMoqCustomization>]
	public async Task DequeueReturnsMessagesUpToMessageCount(IOptions<OutboxSettings> settings, string subject)
	{
		settings.Value.MaxDequeueCount = 5;
		var sut = new SqlOutboxReader(settings);
		for (var i = 0; i < 9; i++)
		{
			var message = new ServiceBusMessageAssembler().Assemble(
				new XmlDummy {
					Name = $"{Guid.NewGuid():D}"
				});
			await _outboxTestDbFixture.InsertMessage(subject, message);
		}

		var (dequeuedSubject, dequeuedMessages) = await sut.DequeueAsync(_transaction);

		dequeuedSubject.Should()
			.Be(subject);
		dequeuedMessages.Should()
			.HaveCount(settings.Value.MaxDequeueCount);
	}

	[Theory]
	[AutoData<AutoMoqCustomization>]
	public async Task DequeueReturnsMessageWithRightProperties(SqlOutboxReader sut, string subject)
	{
		var originalMessage = new ServiceBusMessageAssembler().Assemble(
			new XmlDummy {
				Name = $"{Guid.NewGuid():D}"
			});
		await _outboxTestDbFixture.InsertMessage(subject, originalMessage);

		var (dequeuedSubject, dequeuedMessages) = await sut.DequeueAsync(_transaction);

		dequeuedSubject.Should()
			.Be(subject);
		var dequeuedMessage = dequeuedMessages.Should()
			.HaveCount(expected: 1)
			.And.Subject.Single();
		/* TODO message.Subject
Gets or sets an application specific subject.
Value:
The application specific subject.
Remarks:
This property enables the application to indicate the purpose of the message to the receiver in a standardized fashion,
similar to an email subject line. The mapped AMQP property is "subject".
		 */
		// TODO dequeuedMessage.Subject.Should()
		// 	.Be(originalMessage.Subject);
		dequeuedMessage.MessageId.Should()
			.Be(originalMessage.MessageId);
		dequeuedMessage.ApplicationProperties.Should()
			.BeEquivalentTo(originalMessage.ApplicationProperties);
		dequeuedMessage.Body.ToArray()
			.Should()
			.BeEquivalentTo(originalMessage.Body.ToArray());
	}

	[Theory]
	[AutoData<AutoMoqCustomization>]
	public async Task DequeueSkipsOverSizedMessages(SqlOutboxReader sut, string subject)
	{
		var message = new ServiceBusMessageAssembler().Assemble(
			new XmlDummy {
				Name = RandomNumberGenerator.GetHexString(OutboxSettings.DEFAULT_MAX_MESSAGE_SIZE + 1024)
			});
		await _outboxTestDbFixture.InsertMessage(subject, message);

		var (dequeuedSubject, dequeuedMessages) = await sut.DequeueAsync(_transaction);

		dequeuedSubject.Should()
			.NotBeNull()
			.And.BeEmpty();
		dequeuedMessages.Should()
			.NotBeNull()
			.And.BeEmpty();
	}

	private readonly OutboxTestDbFixture _outboxTestDbFixture;
	private readonly DbConnection _connection;
	private readonly DbTransaction _transaction;
}
