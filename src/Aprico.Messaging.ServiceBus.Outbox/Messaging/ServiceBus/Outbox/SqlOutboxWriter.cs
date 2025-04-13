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
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Aprico.Messaging.Outbox;
using Aprico.Messaging.ServiceBus.Extensions;
using Aprico.Messaging.ServiceBus.Outbox.Data.Extensions;
using Aprico.Messaging.ServiceBus.Outbox.Settings;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;

namespace Aprico.Messaging.ServiceBus.Outbox;

/// <summary>A transactional outbox backed by a SQL Server store for enqueuing <see cref="ServiceBusMessage"/> instances.</summary>
/// <remarks>
/// <para>
/// This implementation is part of the transactional outbox pattern and allows application-layer code to persist
/// <see cref="ServiceBusMessage"/> instances into a SQL Server–backed store as part of the current database transaction.
/// </para>
/// <para>
/// Each message is validated during enqueueing to ensure it contains the required metadata and does not exceed the
/// configured size constraints. Both single and bulk operations are supported within the scope of an existing
/// <see cref="DbTransaction"/>.
/// </para>
/// <para>
/// Messages enqueued in this way are only persisted in the outbox store and must later be dispatched by a background
/// delivery service via the messaging broker.
/// </para>
/// </remarks>
/// <seealso cref="IOutboxWriter{TMessage}"/>
/// <seealso cref="OutboxSettings"/>
/// <seealso cref="ServiceBusMessage"/>
/// <seealso cref="SqlOutboxReader"/>
[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global", Justification = "Public API.")]
public class SqlOutboxWriter : IOutboxWriter<ServiceBusMessage>
{
	public SqlOutboxWriter(IOptions<OutboxSettings> settings)
	{
		ArgumentNullException.ThrowIfNull(settings);
		_settings = settings.Value;
	}

	#region IOutboxWriter<ServiceBusMessage> Members

	/// <summary>Enqueues a single <see cref="ServiceBusMessage"/> into the outbox as part of the specified database transaction.</summary>
	/// <param name="transaction">The database transaction that the enqueue operation will participate in.</param>
	/// <param name="subject">The subject or topic to which pertain the messages.</param>
	/// <param name="message">The <see cref="ServiceBusMessage"/> to enqueue.</param>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A task representing the asynchronous enqueue operation.</returns>
	/// <exception cref="ArgumentNullException">Thrown if <paramref name="transaction"/> or <paramref name="message"/> is <c>null</c>.</exception>
	/// <exception cref="ArgumentException">Thrown if <paramref name="subject"/> is null or empty.</exception>
	/// <exception cref="InvalidOperationException">Thrown if the message is invalid or could not be persisted to the outbox.</exception>
	/// <remarks>
	/// The message is validated during enqueueing to ensure it contains the required metadata and does not exceed the
	/// configured size limit.
	/// </remarks>
	/// <seealso cref="IOutboxWriter{TMessage}"/>
	/// <seealso cref="OutboxSettings"/>
	/// <seealso cref="ServiceBusMessage"/>
	/// <seealso cref="SqlOutboxReader"/>
	public async Task EnqueueAsync(DbTransaction transaction, string subject, ServiceBusMessage message, CancellationToken cancellationToken = default)
	{
		ArgumentNullException.ThrowIfNull(transaction);
		ArgumentException.ThrowIfNullOrEmpty(subject);
		ArgumentNullException.ThrowIfNull(message);

		await using var command = transaction.CreateEnqueuingCommand(subject, message.Validate(_settings.MaxMessageSize));
		var affectedRowCount = await command.ExecuteNonQueryAsync(cancellationToken);
		if (affectedRowCount != 1) throw new InvalidOperationException($"{nameof(ServiceBusMessage)} enqueueing failure, {affectedRowCount} rows have been inserted.");
	}

	/// <summary>
	/// Enqueues multiple <see cref="ServiceBusMessage"/> instances into the outbox as part of the specified database
	/// transaction.
	/// </summary>
	/// <param name="transaction">The database transaction that the enqueue operation will participate in.</param>
	/// <param name="subject">The subject or topic to which pertain the collection of messages.</param>
	/// <param name="messages">The collection of <see cref="ServiceBusMessage"/> instances to enqueue.</param>
	/// <param name="cancellationToken">A token to cancel the asynchronous operation.</param>
	/// <returns>A task representing the asynchronous enqueue operation.</returns>
	/// <exception cref="ArgumentNullException">Thrown if <paramref name="transaction"/> or <paramref name="messages"/> is <c>null</c>.</exception>
	/// <exception cref="ArgumentException">Thrown if <paramref name="subject"/> is null or empty.</exception>
	/// <exception cref="InvalidOperationException">Thrown if one or more messages are invalid.</exception>
	/// <remarks>
	/// Each message is validated during enqueueing to ensure it contains the required metadata and does not exceed the
	/// configured size limit.
	/// </remarks>
	/// <seealso cref="IOutboxWriter{TMessage}"/>
	/// <seealso cref="OutboxSettings"/>
	/// <seealso cref="ServiceBusMessage"/>
	/// <seealso cref="SqlOutboxReader"/>
	public async Task EnqueueAsync(DbTransaction transaction, string subject, IEnumerable<ServiceBusMessage> messages, CancellationToken cancellationToken = default)
	{
		ArgumentNullException.ThrowIfNull(transaction);
		ArgumentException.ThrowIfNullOrEmpty(subject);
		ArgumentNullException.ThrowIfNull(messages);

		using var bulkEnqueuingCommand = transaction.CreateBulkEnqueuingCommand();
		using var dataReader = messages.Validate(_settings.MaxMessageSize)
			.AsDataReader(subject);
		await bulkEnqueuingCommand.WriteToServerAsync(dataReader, cancellationToken);
	}

	#endregion

	private readonly OutboxSettings _settings;
}
