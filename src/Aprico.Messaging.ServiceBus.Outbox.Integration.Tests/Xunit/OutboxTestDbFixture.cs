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
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using Aprico.Messaging.ServiceBus.Outbox;
using Microsoft.Data.SqlClient;
using Microsoft.EntityFrameworkCore;
using Testcontainers.MsSql;
using Testcontainers.Xunit;
using Xunit.Abstractions;

namespace Aprico.Xunit;

[SuppressMessage("ReSharper", "ClassNeverInstantiated.Global")]
public sealed class OutboxTestDbFixture(IMessageSink messageSink) : DbContainerFixture<MsSqlBuilder, MsSqlContainer>(messageSink)
{
	#region Base Class Member Overrides

	protected override MsSqlBuilder Configure(MsSqlBuilder builder)
	{
		return builder.WithImage("mcr.microsoft.com/mssql/server:2022-latest")
			.WithName("aprico-lib-mssql-outbox-test-container");
	}

	#endregion

	#region Base Class Member Overrides

	public override DbProviderFactory DbProviderFactory => SqlClientFactory.Instance;

	protected override async Task InitializeAsync()
	{
		await base.InitializeAsync();
		await using var dbContext = CreateDbContext();
		await dbContext.Database.MigrateAsync();
	}

	#endregion

	private string OutboxTestDbConnectionString => _connectionString ??= new SqlConnectionStringBuilder(Container.GetConnectionString()) {
		InitialCatalog = "OutboxTestDb"
	}.ToString();

	internal void ClearMessages()
	{
		using var connection = new SqlConnection(_connectionString);
		connection.Open();
		var cmd = connection.CreateCommand();
		cmd.CommandText = "DELETE FROM [outbox].[Messages]";
		cmd.ExecuteNonQuery();
	}

	internal new DbConnection CreateConnection()
	{
		return new SqlConnection(OutboxTestDbConnectionString);
	}

	internal DataRow SingleMessageRow(string messageId)
	{
		return SelectRows(
				command => {
					command.CommandText = "SELECT * FROM [outbox].[Messages] WHERE [Id] = @messageId";
					command.Parameters.AddWithValue("@messageId", messageId);
				})
			.Single();
	}

	private IEnumerable<DataRow> SelectRows(Action<SqlCommand> commandBuilder)
	{
		using var connection = new SqlConnection(OutboxTestDbConnectionString);
		using var command = new SqlCommand();
		commandBuilder(command);
		connection.Open();
		command.Connection = connection;
		using var table = new DataTable();
		using var reader = command.ExecuteReader();
		table.Load(reader);
		return table.Rows.Cast<DataRow>();
	}

	private OutboxInstallerDbContext CreateDbContext()
	{
		var dbContext = new OutboxInstallerDbContextFactory().CreateDbContext([]);
		dbContext.Database.SetConnectionString(OutboxTestDbConnectionString);
		return dbContext;
	}

	private string? _connectionString;
}
