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

namespace Aprico.Messaging.ServiceBus.Outbox.Data.Extensions;

[SuppressMessage("ReSharper", "UseRawString")]
internal static class DbTransactionExtensions
{
	internal static SqlBulkCopy CreateBulkEnqueuingCommand(this DbTransaction transaction)
	{
		ArgumentNullException.ThrowIfNull(transaction.Connection);
		return new SqlBulkCopy((SqlConnection) transaction.Connection, SqlBulkCopyOptions.KeepIdentity, (SqlTransaction) transaction) {
			DestinationTableName = $"[{Messages.SCHEMA}].[{nameof(Messages)}]"
		};
	}

	[SuppressMessage("ReSharper", "StringLiteralTypo")]
	internal static DbCommand CreateDequeuingCommand(this DbTransaction transaction, int maxDequeueCount, int maxDequeueSize)
	{
		// Ordering by Timestamp and Id ensures deterministic sequencing of messages, even when timestamps are identical.
		// The dequeuing logic prioritizes the subject associated with the message holding the earliest Timestamp, while
		// explicitly skipping any message whose size exceeds maxDequeueSize. This constraint serves as a safeguard against
		// indefinite stalling, which could occur if the oldest subject contains a single over-sized message.
		// Likely a message manually inserted into the SQL Outbox Store... *sigh*
		// https://dba.stackexchange.com/questions/42985/running-total-to-the-previous-row
		const string DEQUEUE_COMMAND = $@"
WITH [OldestMessage] ([{nameof(Messages.Subject)}]) AS (
   SELECT TOP 1 [{nameof(Messages.Subject)}]
   FROM [{Messages.SCHEMA}].[{nameof(Messages)}] M WITH (READPAST)
   WHERE (LEN(M.[{nameof(Messages.Body)}]) + LEN(M.[{nameof(Messages.Headers)}])) <= @maxDequeueSize
   ORDER BY [{nameof(Messages.Timestamp)}], [{nameof(Messages.Id)}]
),
[MessageBatch] ([{nameof(Messages.Subject)}], [{nameof(Messages.Timestamp)}], [{nameof(Messages.Id)}], [DequeueSize]) AS (
   SELECT TOP (@maxDequeueCount) M.[{nameof(Messages.Subject)}], M.[{nameof(Messages.Timestamp)}], M.[{nameof(Messages.Id)}],
      SUM(LEN(M.[{nameof(Messages.Body)}]) + LEN(M.[{nameof(Messages.Headers)}])) OVER (PARTITION BY M.[{nameof(Messages.Subject)}] ORDER BY M.[{nameof(Messages.Timestamp)}], M.[{nameof(Messages.Id)}]) AS [DequeueSize]
   FROM [{Messages.SCHEMA}].[{nameof(Messages)}] M WITH (READPAST) INNER JOIN [OldestMessage] ON M.[{nameof(Messages.Subject)}] = [OldestMessage].[{nameof(Messages.Subject)}]
   WHERE (LEN(M.[{nameof(Messages.Body)}]) + LEN(M.[{nameof(Messages.Headers)}])) <= @maxDequeueSize
   ORDER BY [{nameof(Messages.Timestamp)}], [{nameof(Messages.Id)}]
)
DELETE [{Messages.SCHEMA}].[{nameof(Messages)}]
OUTPUT DELETED.[{nameof(Messages.Subject)}], DELETED.[{nameof(Messages.Id)}], DELETED.[{nameof(Messages.Headers)}], DELETED.[{nameof(Messages.Body)}], DELETED.[{nameof(Messages.Timestamp)}]
FROM [{Messages.SCHEMA}].[{nameof(Messages)}] M INNER JOIN [MessageBatch] AM ON M.[{nameof(Messages.Id)}] = AM.[{nameof(Messages.Id)}]
WHERE M.[DequeueSize] <= @maxDequeueSize
";
		var command = transaction.CreateCommand(DEQUEUE_COMMAND);
		command.AddParameter("@maxDequeueCount", maxDequeueCount)
			.AddParameter("@maxDequeueSize", maxDequeueSize);
		return command;
	}

	internal static DbCommand CreateEnqueuingCommand(this DbTransaction transaction, string subject, ServiceBusMessage message)
	{
		const string ENQUEUE_COMMAND = $@"
INSERT INTO [{Messages.SCHEMA}].[{nameof(Messages)}] ([{nameof(Messages.Id)}], [{nameof(Messages.Subject)}], [{nameof(Messages.Headers)}], [{nameof(Messages.Body)}], [{nameof(Messages.Timestamp)}])
VALUES (@id, @subject, @headers, @body, @timestamp)
";
		var command = transaction.CreateCommand(ENQUEUE_COMMAND);
		command.AddParameter("@id", message.MessageId)
			.AddParameter("@subject", subject) // @formatter:wrap_chained_method_calls chop_if_long
			.AddParameter("@headers", message.ApplicationProperties.ToJson().ToString()) // @formatter:wrap_chained_method_calls restore
			.AddParameter("@body", message.Body.ToString())
			.AddParameter("@timestamp", message.GetTimestamp());
		return command;
	}

	[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities")]
	private static DbCommand CreateCommand(this DbTransaction transaction, string commandText)
	{
		ArgumentNullException.ThrowIfNull(transaction.Connection);
		var command = transaction.Connection.CreateCommand();
		command.CommandText = commandText;
		command.CommandType = CommandType.Text;
		command.Transaction = transaction;
		return command;
	}

	private static IDbCommand AddParameter(this IDbCommand command, string parameterName, object value)
	{
		var parameter = command.CreateParameter();
		parameter.ParameterName = parameterName;
		parameter.Value = value;
		command.Parameters.Add(parameter);
		return command;
	}
}
