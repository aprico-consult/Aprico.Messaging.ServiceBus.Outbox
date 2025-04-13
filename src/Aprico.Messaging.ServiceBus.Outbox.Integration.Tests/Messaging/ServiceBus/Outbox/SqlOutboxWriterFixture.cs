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
using System.Security.Cryptography;
using System.Threading.Tasks;
using Aprico.AutoFixture.Xunit2;
using Aprico.Messaging.ServiceBus.Dummies;
using Aprico.Messaging.ServiceBus.Extensions;
using Aprico.Messaging.ServiceBus.Outbox.Settings;
using Aprico.Messaging.ServiceBus.Xml;
using Aprico.Xunit;
using AutoFixture.AutoMoq;
using Azure.Messaging.ServiceBus;

namespace Aprico.Messaging.ServiceBus.Outbox;

[Collection(nameof(OutboxTestDbFixture))]
[SuppressMessage("Design", "CA1063:Implement IDisposable Correctly")]
public class SqlOutboxWriterFixture : IClassFixture<OutboxTestDbFixture>, IDisposable
{
	#region Setup/Teardown

	public SqlOutboxWriterFixture(OutboxTestDbFixture outboxTestDbFixture)
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
	public async Task EnqueueMultipleMessages(SqlOutboxWriter sut, string subject)
	{
		ServiceBusMessage[] messages = [
			new ServiceBusMessageAssembler().Assemble(
				new XmlDummy {
					Name = $"{Guid.NewGuid():D}"
				}),
			new ServiceBusMessageAssembler().Assemble(
				new XmlDummy {
					Name = $"{Guid.NewGuid():D}"
				})
		];

		await sut.EnqueueAsync(_transaction, subject, messages);
		await _transaction.CommitAsync();

		var row = _outboxTestDbFixture.SingleMessageRow(messages[0].MessageId);
		row[nameof(Message.Id)]
			.Should()
			.Be(Guid.Parse(messages[0].MessageId));
		row[nameof(Message.Subject)]
			.Should()
			.Be(subject);
		row[nameof(Message.Headers)]
			.Should() // @formatter:wrap_chained_method_calls chop_if_long
			.Be(messages[0].ApplicationProperties.ToJson().ToString());
		row[nameof(Message.Body)]
			.Should() //
			.Be(messages[0].Body.ToString());
		row[nameof(Message.Timestamp)]
			.Should() //
			.Be(messages[0].GetTimestamp()); // @formatter:wrap_chained_method_calls restore

		row = _outboxTestDbFixture.SingleMessageRow(messages[1].MessageId);
		row[nameof(Message.Subject)]
			.Should()
			.Be(subject);
	}

	[Theory]
	[AutoData<AutoMoqCustomization>]
	public async Task EnqueueMultipleMessagesThrowsForOverSizedMessage(SqlOutboxWriter sut, string subject)
	{
		ServiceBusMessage[] messages = [
			new ServiceBusMessageAssembler().Assemble(
				new XmlDummy {
					Name = RandomNumberGenerator.GetHexString(OutboxSettings.DEFAULT_MAX_MESSAGE_SIZE + 1024)
				})
		];

		await Invoking(() => sut.EnqueueAsync(_transaction, subject, messages))
			.Should()
			.ThrowAsync<InvalidOperationException>();
	}

	[Theory]
	[AutoData<AutoMoqCustomization>]
	public async Task EnqueueSingleMessage(SqlOutboxWriter sut, string subject)
	{
		var message = new ServiceBusMessageAssembler().Assemble(
			new XmlDummy {
				Name = $"{Guid.NewGuid():D}"
			});

		await sut.EnqueueAsync(_transaction, subject, message);
		await _transaction.CommitAsync();

		var row = _outboxTestDbFixture.SingleMessageRow(message.MessageId);
		row.ItemArray.Should()
			.HaveCount(expected: 5); // because sql statement is select * from outbox.Messages
		row[nameof(Message.Id)]
			.Should()
			.Be(Guid.Parse(message.MessageId));
		row[nameof(Message.Subject)]
			.Should()
			.Be(subject);
		row[nameof(Message.Headers)]
			.Should() // @formatter:wrap_chained_method_calls chop_if_long
			.Be(message.ApplicationProperties.ToJson().ToString()); // @formatter:wrap_chained_method_calls restore
		row[nameof(Message.Body)]
			.Should()
			.Be(message.Body.ToString());
		row[nameof(Message.Timestamp)]
			.Should()
			.Be(message.GetTimestamp());
	}

	[Theory]
	[AutoData<AutoMoqCustomization>]
	public async Task EnqueueSingleMessageThrowsForOverSizedMessage(SqlOutboxWriter sut, string subject)
	{
		var message = new ServiceBusMessageAssembler().Assemble(
			new XmlDummy {
				Name = RandomNumberGenerator.GetHexString(OutboxSettings.DEFAULT_MAX_MESSAGE_SIZE + 1024)
			});

		await Invoking(() => sut.EnqueueAsync(_transaction, subject, message))
			.Should()
			.ThrowAsync<InvalidOperationException>();
	}

	private readonly OutboxTestDbFixture _outboxTestDbFixture;
	private readonly DbConnection _connection;
	private readonly DbTransaction _transaction;
}
