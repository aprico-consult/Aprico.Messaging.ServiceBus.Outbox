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
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Aprico.Messaging.Outbox;
using Aprico.Messaging.ServiceBus.Extensions;
using Aprico.Messaging.ServiceBus.Outbox.Data.Extensions;
using Aprico.Messaging.ServiceBus.Outbox.Settings;
using Azure.Messaging.ServiceBus;
using Microsoft.Extensions.Options;

namespace Aprico.Messaging.ServiceBus.Outbox;

/// <summary>Allows dequeuing of <see cref="ServiceBusMessage"/> instances stored in the SQL Server–backed outbox store.</summary>
/// <remarks>
/// <para>
/// This component is part of the transactional outbox pattern and is intended to be used by a background delivery service to
/// retrieve messages previously enqueued via <see cref="SqlOutboxWriter"/>. It enforces limits on the number and total accumulated
/// size of dequeued messages, as configured via <see cref="OutboxSettings"/>.
/// </para>
/// <para>
/// Messages returned by this store are grouped by subject and are intended to be dispatched to the same Azure Service Bus
/// queue.
/// </para>
/// </remarks>
/// <seealso cref="IOutboxReader{TMessage}"/>
/// <seealso cref="OutboxSettings"/>
/// <seealso cref="ServiceBusMessage"/>
/// <seealso cref="SqlOutboxWriter"/>
public class SqlOutboxReader : IOutboxReader<ServiceBusMessage>
{
	[SuppressMessage("ReSharper", "MemberCanBeInternal", Justification = "Public API.")]
	public SqlOutboxReader(IOptions<OutboxSettings> settings)
	{
		ArgumentNullException.ThrowIfNull(settings);
		_settings = settings.Value;
	}

	#region IOutboxReader<ServiceBusMessage> Members

	/// <summary>Dequeues a batch of <see cref="ServiceBusMessage"/> instances from the SQL Server–backed outbox store.</summary>
	/// <param name="transaction">The active database transaction used for the dequeue operation.</param>
	/// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
	/// <returns>
	/// A task representing the asynchronous dequeue operation, yielding a tuple containing a subject and a collection of
	/// messages associated with that subject. If no messages are available, the result is
	/// <see cref="OutboxReaderExtensions.Empty{TMessage}"/>.
	/// </returns>
	/// <exception cref="ArgumentNullException">Thrown if <paramref name="transaction"/> is <c>null</c>.</exception>
	/// <exception cref="InvalidOperationException">Thrown if one or more messages are invalid.</exception>
	/// <remarks>
	/// <para>
	/// This operation retrieves a batch of messages that all share the same subject and are intended to be dispatched to the
	/// same Azure Service Bus queue.
	/// </para>
	/// <para>Batches are constrained by the following criteria:</para>
	/// <list type="bullet">
	/// <item>
	/// <description>
	/// The batch will include no more than <see cref="OutboxSettings.MaxDequeueCount"/> messages. If not configured, this
	/// defaults to <see cref="OutboxSettings.DEFAULT_MAX_DEQUEUE_COUNT"/>.
	/// </description>
	/// </item> <item>
	/// <description>
	/// The total batch payload size—calculated as the sum of individual message headers and bodies—must not exceed
	/// <see cref="OutboxSettings.MaxDequeueSize"/>. If not configured, this defaults to
	/// <see cref="OutboxSettings.DEFAULT_MAX_DEQUEUE_SIZE"/>. Any single message that exceeds this limit will be ignored by dequeue
	/// operations and will remain in the outbox indefinitely.
	/// </description>
	/// </item>
	/// </list>
	/// <para>
	/// Each message is validated to ensure it contains the required metadata and does not exceed size constraints before being
	/// returned.
	/// </para>
	/// </remarks>
	[SuppressMessage("ReSharper", "MoveLocalFunctionAfterJumpStatement")]
	public async Task<(string Subject, IEnumerable<ServiceBusMessage> Messages)> DequeueAsync(DbTransaction transaction, CancellationToken cancellationToken = default)
	{
		// @formatter:wrap_chained_method_calls chop_if_long
		async IAsyncEnumerable<ServiceBusMessage> ExtractMessagesFromReader(DbDataReader reader, [EnumeratorCancellation] CancellationToken enumCancellationToken)
		{
			do
			{
				var message = new ServiceBusMessage(reader.GetString(MessageConfiguration.COLUMN_ORDINAL_BODY)) {
					MessageId = reader.GetGuid(MessageConfiguration.COLUMN_ORDINAL_ID).ToString()
				};
				reader.GetString(MessageConfiguration.COLUMN_ORDINAL_HEADERS).ToDictionary().CopyContextPropertiesTo(message);
				yield return message.Validate(_settings.MaxMessageSize);
			} while (await reader.ReadAsync(enumCancellationToken));
		}

		ArgumentNullException.ThrowIfNull(transaction);

		await using var cmd = transaction.CreateDequeuingCommand(_settings.MaxDequeueCount, _settings.MaxDequeueSize);
		await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
		if (!await reader.ReadAsync(cancellationToken)) return this.Empty();
		var subject = reader.GetString(MessageConfiguration.COLUMN_ORDINAL_SUBJECT);
		return (subject, await ExtractMessagesFromReader(reader, cancellationToken).ToArrayAsync(cancellationToken));
		// @formatter:wrap_chained_method_calls restore
	}

	#endregion

	private readonly OutboxSettings _settings;
}
