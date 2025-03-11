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
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Aprico.Messaging.ServiceBus.Data.Extensions;
using Aprico.Messaging.ServiceBus.Extensions;
using Aprico.Messaging.ServiceBus.Outbox.Settings;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;

namespace Aprico.Messaging.ServiceBus.Outbox;

/// <summary>A transactional outbox client for enqueuing <see cref="ServiceBusMessage"/> instances into the outbox store.</summary>
/// <remarks>
/// <para>
/// This implementation validates each message before enqueueing to ensure it complies with required metadata and size
/// constraints based on the configured limits. It supports both single and bulk enqueue operations within the scope of an existing
/// <see cref="DbTransaction"/>.
/// </para>
/// <para>Note that messages enqueued in this way are only persisted and must be dispatched later by a background worker.</para>
/// </remarks>
/// <seealso cref="OutboxStore"/>
/// <seealso cref="ServiceBusMessage"/>
public class OutboxClient : IOutbox<ServiceBusMessage>
{
	public OutboxClient(IOptions<OutboxSettings> settings)
	{
		ArgumentNullException.ThrowIfNull(settings);
		_settings = settings.Value;
	}

	#region IOutbox<ServiceBusMessage> Members

	/// <summary>Enqueues a single <see cref="ServiceBusMessage"/> into the outbox as part of the specified database transaction.</summary>
	/// <param name="transaction">The database transaction in which the message will be enqueued.</param>
	/// <param name="destinationAggregate">The name of the destination aggregate that should receive the message.</param>
	/// <param name="message">The <see cref="ServiceBusMessage"/> to enqueue.</param>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A task representing the asynchronous enqueue operation.</returns>
	/// <exception cref="ArgumentNullException">Thrown if <paramref name="transaction"/> or <paramref name="message"/> is <c>null</c>.</exception>
	/// <exception cref="ArgumentException">Thrown if <paramref name="destinationAggregate"/> is null or empty.</exception>
	/// <exception cref="InvalidOperationException">Thrown if the message is invalid or could not be persisted to the outbox.</exception>
	/// <remarks>
	/// The message is validated before enqueueing to ensure it includes required metadata and does not exceed the configured
	/// size limit.
	/// </remarks>
	/// <seealso cref="ServiceBusMessage"/>
	/// <seealso cref="OutboxSettings"/>
	/// <seealso cref="OutboxStore"/>
	public async Task EnqueueAsync(DbTransaction transaction, string destinationAggregate, ServiceBusMessage message, CancellationToken cancellationToken = default)
	{
		ArgumentNullException.ThrowIfNull(transaction);
		ArgumentException.ThrowIfNullOrEmpty(destinationAggregate);
		ArgumentNullException.ThrowIfNull(message);

		await using var command = transaction.CreateEnqueuingCommand(destinationAggregate, message.Validate(_settings.MaxMessageSize));
		var affectedRowCount = await command.ExecuteNonQueryAsync(cancellationToken);
		if (affectedRowCount != 1) throw new InvalidOperationException($"{nameof(ServiceBusMessage)} enqueueing failure, {affectedRowCount} rows have been inserted.");
	}

	/// <summary>
	/// Enqueues multiple <see cref="ServiceBusMessage"/> instances into the outbox as part of the specified database
	/// transaction.
	/// </summary>
	/// <param name="transaction">The database transaction in which the messages will be enqueued.</param>
	/// <param name="destinationAggregate">The name of the destination aggregate that should receive the messages.</param>
	/// <param name="messages">The collection of <see cref="ServiceBusMessage"/> instances to enqueue.</param>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A task representing the asynchronous enqueue operation.</returns>
	/// <exception cref="ArgumentNullException">Thrown if <paramref name="transaction"/> or <paramref name="messages"/> is <c>null</c>.</exception>
	/// <exception cref="ArgumentException">Thrown if <paramref name="destinationAggregate"/> is null or empty.</exception>
	/// <exception cref="InvalidOperationException">Thrown if one or more messages are invalid.</exception>
	/// <remarks>
	/// Each message is validated before enqueueing to ensure it includes the required metadata and does not exceed the
	/// configured size limit.
	/// </remarks>
	/// <seealso cref="ServiceBusMessage"/>
	public async Task EnqueueAsync(DbTransaction transaction, string destinationAggregate, IEnumerable<ServiceBusMessage> messages, CancellationToken cancellationToken = default)
	{
		ArgumentNullException.ThrowIfNull(transaction);
		ArgumentException.ThrowIfNullOrEmpty(destinationAggregate);
		ArgumentNullException.ThrowIfNull(messages);

		using var sqlBulkCopy = transaction.CreateSqlBulkCopy();
		using var dataReader = messages.Validate(_settings.MaxMessageSize)
			.AsDataReader(destinationAggregate);
		await sqlBulkCopy.WriteToServerAsync(dataReader, cancellationToken);
	}

	#endregion

	private readonly OutboxSettings _settings;
}
