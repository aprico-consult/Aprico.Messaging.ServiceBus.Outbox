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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Aprico.Messaging.ServiceBus.Data.Extensions;
using Aprico.Messaging.ServiceBus.Extensions;
using Aprico.Messaging.ServiceBus.Outbox.Settings;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;

namespace Aprico.Messaging.ServiceBus.Outbox;

/// <summary>Background worker responsible for dequeuing <see cref="ServiceBusMessage"/> instances from the outbox for dispatch.</summary>
/// <remarks>
/// <para>
/// This component is part of the transactional outbox pattern and is typically used by a background service or job
/// scheduler. It dequeues messages that were previously enqueued using <see cref="IOutbox{TMessage}"/>, applying limits on
/// both message count and total size as defined in <see cref="OutboxSettings"/>.
/// </para>
/// <para>Messages returned by this worker are grouped by destination aggregate and are routed to the same Azure Service Bus queue.</para>
/// </remarks>
/// <seealso cref="ServiceBusMessage"/>
/// <seealso cref="OutboxSettings"/>
/// <seealso cref="OutboxClient"/>
public class OutboxStore : IOutboxStore<ServiceBusMessage>
{
	public OutboxStore(IOptions<OutboxSettings> settings)
	{
		ArgumentNullException.ThrowIfNull(settings);
		_settings = settings.Value;
	}

	#region IOutboxStore<ServiceBusMessage> Members

	/// <summary>Dequeues and dispatches a collection of <see cref="ServiceBusMessage"/> instances from the outbox.</summary>
	/// <param name="transaction">The active database transaction used for the dequeue operation.</param>
	/// <param name="messageCount">
	/// The maximum number of messages to retrieve. Defaults to <see cref="OutboxSettings.MaxDequeueCount"/>
	/// .
	/// </param>
	/// <param name="cancellationToken">A token that can be used to cancel the asynchronous operation.</param>
	/// <returns>
	/// A task that returns a tuple containing the name of the destination aggregate and the dequeued messages. If no messages
	/// are available, the result is <c>default</c>.
	/// </returns>
	/// <exception cref="ArgumentNullException">Thrown if <paramref name="transaction"/> is <see langword="null"/>.</exception>
	/// <exception cref="InvalidOperationException">Thrown if one or more messages are invalid.</exception>
	/// <remarks>
	/// <para>
	/// This operation retrieves messages associated with a single destination aggregate. All messages will be dispatched to the
	/// same Azure Service Bus queue.
	/// </para>
	/// <para>
	/// Only messages whose combined header and body size, when accumulated across the dequeued set, do not exceed the configured
	/// <see cref="OutboxSettings.MaxDequeueSize"/> will be dequeued. Note that any individual message that exceeds this limit on its
	/// own will remain in the outbox. Furthermore, each message is validated before dispatch to ensure it contains the required
	/// metadata and does not exceed the configured size limit.
	/// </para>
	/// </remarks>
	public async Task<(string destinationAggregate, IEnumerable<ServiceBusMessage> messages)> DequeueAsync(
		DbTransaction transaction,
		int messageCount = OutboxSettings.DEFAULT_MAX_DEQUEUE_SIZE,
		CancellationToken cancellationToken = default)
	{
		async IAsyncEnumerable<ServiceBusMessage> BuildMessagesAsync(DbDataReader reader, [EnumeratorCancellation] CancellationToken enumCancellationToken)
		{
			do
			{
				// @formatter:wrap_chained_method_calls chop_if_long
				var message = new ServiceBusMessage(reader.GetString(ordinal: 3)) {
					MessageId = reader.GetGuid(ordinal: 1).ToString()
				};
				reader.GetString(ordinal: 2).ToDictionary().CopyContextPropertiesTo(message);
				// @formatter:wrap_chained_method_calls restore
				yield return message.Validate(_settings.MaxMessageSize);
			} while (await reader.ReadAsync(enumCancellationToken));
		}

		ArgumentNullException.ThrowIfNull(transaction);
		await using var cmd = transaction.CreateDequeuingCommand(_settings.MaxDequeueCount, _settings.MaxDequeueSize);
		await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (await reader.ReadAsync(cancellationToken))
		{
			var entityName = reader.GetString(ordinal: 0);
			// @formatter:wrap_chained_method_calls chop_if_long
			// TODO return a ServiceBusMessageBatch though return type is IEnumerable<ServiceBusMessage>
			return (entityName, await BuildMessagesAsync(reader, cancellationToken).ToArrayAsync(cancellationToken));
			// @formatter:wrap_chained_method_calls restore
		}
		return default;
	}

	#endregion

	private readonly OutboxSettings _settings;
}
