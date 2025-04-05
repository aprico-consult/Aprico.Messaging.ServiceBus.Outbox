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

using System.Diagnostics.CodeAnalysis;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata.Builders;

namespace Aprico.Messaging.ServiceBus.Outbox;

[SuppressMessage("Naming", "CA1707:Identifiers should not contain underscores")]
public class MessageConfiguration : IEntityTypeConfiguration<Message>
{
	#region IEntityTypeConfiguration<Message> Members

	[SuppressMessage("Design", "CA1062:Validate arguments of public methods")]
	[SuppressMessage("ReSharper", "StringLiteralTypo")]
	public void Configure(EntityTypeBuilder<Message> builder)
	{
		builder.ToTable($"{nameof(Message)}s", NAME_SCHEMA);
		builder.HasKey(static e => e.Id);
		builder.Property(static e => e.Id)
			.HasColumnOrder(COLUMN_ORDINAL_ID);
		builder.Property(static e => e.Subject)
			.HasColumnOrder(COLUMN_ORDINAL_SUBJECT)
			.HasMaxLength(maxLength: 256)
			.IsRequired();
		builder.Property(static e => e.Headers)
			.HasColumnOrder(COLUMN_ORDINAL_HEADERS)
			.IsRequired();
		builder.Property(static e => e.Body)
			.HasColumnOrder(COLUMN_ORDINAL_BODY)
			.IsRequired();
		builder.Property(static e => e.Timestamp)
			.HasColumnOrder(COLUMN_ORDINAL_TIMESTAMP)
			.HasColumnType("datetimeoffset(7)")
			.IsRequired();
	}

	#endregion

	#region NAMES

	private const string NAME_SCHEMA = "outbox";

	public const string NAME_TABLE = $"[{NAME_SCHEMA}].[{nameof(Message)}s]";

	[SuppressMessage("ReSharper", "IdentifierTypo")]
	public const string NAME_SPROC_DEQUEUING = $"[{NAME_SCHEMA}].[Dequeue{nameof(Message)}s]";

	[SuppressMessage("ReSharper", "IdentifierTypo")]
	public const string NAME_SPROC_ENQUEUING = $"[{NAME_SCHEMA}].[Enqueue{nameof(Message)}]";

	#endregion

	#region ORDINALS

	// zero-based indexing aligns with DbDataReader's ordinal column access, ensuring consistent data retrieval for column-based operations performed in SqlOutboxStore
	public const int COLUMN_ORDINAL_ID = 0;
	public const int COLUMN_ORDINAL_SUBJECT = 1;
	public const int COLUMN_ORDINAL_HEADERS = 2;
	public const int COLUMN_ORDINAL_BODY = 3;
	private const int COLUMN_ORDINAL_TIMESTAMP = 4;

	#endregion
}
