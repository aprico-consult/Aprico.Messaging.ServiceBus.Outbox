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
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Aprico.Messaging.ServiceBus.Extensions;
using Azure.Messaging.ServiceBus;
using Microsoft.Data.SqlClient;

namespace Aprico.Messaging.ServiceBus.Data.Extensions;

[SuppressMessage("ReSharper", "UseRawString")]
internal static class DbTransactionExtensions
{
	[SuppressMessage("ReSharper", "StringLiteralTypo")]
	internal static DbCommand CreateDequeuingCommand(this DbTransaction transaction, int maxMessageCount, int maxBatchSize)
	{
		// https://dba.stackexchange.com/questions/42985/running-total-to-the-previous-row
		// Ordering by Timestamp and Id ensures a consistent sequence when multiple messages share the same timestamp
		// The dequeuing process prioritizes the oldest entity while implementing a safety mechanism:
		// - Messages exceeding the MAX_MESSAGE_SIZE limit are deliberately skipped
		// - This prevents potential infinite dequeuing stalls caused by over-sized messages
		// - Such messages, even if directly inserted into the database, will not be processed
		// - Effectively, over-sized messages are permanently excluded from dequeuing
		const string DEQUEUE_COMMAND = @"
WITH [DestinationAggregates] ([DestinationAggregate]) AS (
	SELECT TOP 1 [DestinationAggregate]
	FROM [outbox].[Queue] Q WITH (READPAST)
	WHERE (LEN(Q.[Body]) + LEN(Q.[Headers])) <= @maxBatchSize
	ORDER BY [Timestamp], [Id]
),
[Messages] ([DestinationAggregate], [Timestamp], [Id], [BatchSize]) AS (
	SELECT TOP (@maxMessageCount) Q.[DestinationAggregate], Q.[Timestamp], Q.[Id], SUM(LEN(Q.[Body]) + LEN(Q.[Headers])) OVER (PARTITION BY Q.[DestinationAggregate] ORDER BY Q.[Timestamp], Q.[Id]) AS [BatchSize]
	FROM [outbox].[Queue] Q WITH (READPAST) INNER JOIN [DestinationAggregates] E ON Q.DestinationAggregate = E.[DestinationAggregate]
	WHERE (LEN(Q.[Body]) + LEN(Q.[Headers])) <= @maxBatchSize
	ORDER BY [Timestamp], [Id]
)
DELETE [outbox].[Queue]
OUTPUT DELETED.[DestinationAggregate], DELETED.[Id], DELETED.[Headers], DELETED.[Body], DELETED.[Timestamp]
FROM [outbox].[Queue] Q INNER JOIN [Messages] M ON Q.[Id] = M.[Id]
WHERE M.[BatchSize] <= @maxBatchSize
";
		var command = transaction.CreateCommand(DEQUEUE_COMMAND);
		command.AddParameter("@maxMessageCount", maxMessageCount)
			.AddParameter("@maxBatchSize", maxBatchSize);
		return command;
	}

	internal static DbCommand CreateEnqueuingCommand(this DbTransaction transaction, string destinationAggregate, ServiceBusMessage message)
	{
		const string ENQUEUE_COMMAND = @"
INSERT INTO [outbox].[Queue] ([Id], [DestinationAggregate], [Headers], [Body], [Timestamp])
VALUES (@id, @destinationAggregate, @headers, @body, @timestamp)
";

		// @formatter:wrap_chained_method_calls chop_if_long
		var command = transaction.CreateCommand(ENQUEUE_COMMAND);
		command.AddParameter("@id", message.MessageId)
			.AddParameter("@destinationAggregate", destinationAggregate)
			.AddParameter("@headers", message.ApplicationProperties.ToJson().ToString())
			.AddParameter("@body", message.Body.ToString())
			.AddParameter("@timestamp", message.GetTimestamp());
		return command;
	}

	internal static SqlBulkCopy CreateSqlBulkCopy(this IDbTransaction transaction)
	{
		ArgumentNullException.ThrowIfNull(transaction.Connection);
		return new SqlBulkCopy((SqlConnection) transaction.Connection, SqlBulkCopyOptions.KeepIdentity, (SqlTransaction) transaction) {
			DestinationTableName = "outbox.Queue"
		};
	}

	private static DbCommand CreateCommand(this DbTransaction transaction, string commandText)
	{
		return (DbCommand) ((IDbTransaction) transaction).CreateCommand(commandText);
	}

	private static IDbCommand CreateCommand(this IDbTransaction transaction, string commandText)
	{
		ArgumentNullException.ThrowIfNull(transaction.Connection);
		var command = transaction.Connection.CreateCommand(commandText);
		command.Transaction = transaction;
		return command;
	}
}
