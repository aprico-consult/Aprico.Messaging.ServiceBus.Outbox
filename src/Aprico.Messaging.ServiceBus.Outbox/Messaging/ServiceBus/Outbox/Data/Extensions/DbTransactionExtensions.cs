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
			DestinationTableName = MessageConfiguration.NAME_TABLE
		};
	}

	internal static DbCommand CreateDequeuingCommand(this DbTransaction transaction, int maxDequeueCount, int maxDequeueSize)
	{
		return transaction.CreateCommand(MessageConfiguration.NAME_SPROC_DEQUEUING)
			.AddParameter($"@{nameof(maxDequeueCount)}", maxDequeueCount)
			.AddParameter($"@{nameof(maxDequeueSize)}", maxDequeueSize);
	}

	internal static DbCommand CreateEnqueuingCommand(this DbTransaction transaction, string subject, ServiceBusMessage message)
	{
		return transaction.CreateCommand(MessageConfiguration.NAME_SPROC_ENQUEUING)
			.AddParameter("@id", message.MessageId)
			.AddParameter("@subject", subject) // @formatter:wrap_chained_method_calls chop_if_long
			.AddParameter("@headers", message.ApplicationProperties.ToJson().ToString()) // @formatter:wrap_chained_method_calls restore
			.AddParameter("@body", message.Body.ToString())
			.AddParameter("@timestamp", message.GetTimestamp());
	}

	[SuppressMessage("Security", "CA2100:Review SQL queries for security vulnerabilities")]
	private static DbCommand CreateCommand(this DbTransaction transaction, string commandText)
	{
		ArgumentNullException.ThrowIfNull(transaction.Connection);
		var command = transaction.Connection.CreateCommand();
		command.CommandText = commandText;
		command.CommandType = CommandType.StoredProcedure;
		command.Transaction = transaction;
		return command;
	}

	private static DbCommand AddParameter(this DbCommand command, string parameterName, object value)
	{
		var parameter = command.CreateParameter();
		parameter.ParameterName = parameterName;
		parameter.Value = value;
		command.Parameters.Add(parameter);
		return command;
	}
}
