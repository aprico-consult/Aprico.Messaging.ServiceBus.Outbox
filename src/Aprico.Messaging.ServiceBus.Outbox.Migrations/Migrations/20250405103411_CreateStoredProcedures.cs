using System.Diagnostics.CodeAnalysis;
using Aprico.Messaging.ServiceBus.Outbox;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Aprico.Migrations
{
	/// <inheritdoc />
	[SuppressMessage("ReSharper", "StringLiteralTypo")]
	public partial class CreateStoredProcedures : Migration
	{
		/// <inheritdoc />
		[SuppressMessage("Globalization", "CA1308:Normalize strings to uppercase")]
		protected override void Up(MigrationBuilder migrationBuilder)
		{
			migrationBuilder.Sql($@"
CREATE PROCEDURE {MessageConfiguration.NAME_SPROC_ENQUEUING}
    @{nameof(Message.Id).ToLowerInvariant()} UNIQUEIDENTIFIER,
    @{nameof(Message.Subject).ToLowerInvariant()} NVARCHAR(MAX),
    @{nameof(Message.Headers).ToLowerInvariant()} NVARCHAR(MAX),
    @{nameof(Message.Body).ToLowerInvariant()} NVARCHAR(MAX),
    @{nameof(Message.Timestamp).ToLowerInvariant()} DATETIME2
AS
BEGIN
   INSERT INTO {MessageConfiguration.NAME_TABLE}
   ([{nameof(Message.Id)}], [{nameof(Message.Subject)}], [{nameof(Message.Headers)}], [{nameof(Message.Body)}], [{nameof(Message.Timestamp)}])
   VALUES (@{nameof(Message.Id).ToLowerInvariant()}, @{nameof(Message.Subject).ToLowerInvariant()}, @{nameof(Message.Headers).ToLowerInvariant()}, @{nameof(Message.Body).ToLowerInvariant()}, @{nameof(Message.Timestamp).ToLowerInvariant()})
END");

			// Ordering by Timestamp and Id ensures deterministic sequencing of messages, even when timestamps are identical.
			// The dequeuing logic prioritizes the subject associated with the message holding the earliest Timestamp, while
			// explicitly skipping any message whose size exceeds maxDequeueSize. This constraint serves as a safeguard against
			// indefinite stalling, which could occur if the oldest subject contains a single over-sized message.
			// Likely a message manually inserted into the SQL Outbox Store... *sigh*
			// https://dba.stackexchange.com/questions/42985/running-total-to-the-previous-row
			migrationBuilder.Sql($@"
CREATE PROCEDURE {MessageConfiguration.NAME_SPROC_DEQUEUING}
    @maxDequeueCount INT,
    @maxDequeueSize INT
AS
BEGIN
   SET NOCOUNT ON;
   WITH [OldestMessage] ([{nameof(Message.Subject)}]) AS (
      SELECT TOP 1 [{nameof(Message.Subject)}]
      FROM {MessageConfiguration.NAME_TABLE} M WITH (READPAST)
      WHERE (LEN(M.[{nameof(Message.Body)}]) + LEN(M.[{nameof(Message.Headers)}])) <= @maxDequeueSize
      ORDER BY [{nameof(Message.Timestamp)}], [{nameof(Message.Id)}]
   ),
   [MessageBatch] ([{nameof(Message.Subject)}], [{nameof(Message.Timestamp)}], [{nameof(Message.Id)}], [BatchSize]) AS (
      SELECT TOP (@maxDequeueCount) M.[{nameof(Message.Subject)}], M.[{nameof(Message.Timestamp)}], M.[{nameof(Message.Id)}],
         SUM(LEN(M.[{nameof(Message.Body)}]) + LEN(M.[{nameof(Message.Headers)}])) OVER (
            PARTITION BY M.[{nameof(Message.Subject)}]
            ORDER BY M.[{nameof(Message.Timestamp)}], M.[{nameof(Message.Id)}]
         ) AS [BatchSize]
      FROM {MessageConfiguration.NAME_TABLE} M WITH (READPAST)
      INNER JOIN [OldestMessage] O ON M.[{nameof(Message.Subject)}] = O.[{nameof(Message.Subject)}]
      WHERE (LEN(M.[{nameof(Message.Body)}]) + LEN(M.[{nameof(Message.Headers)}])) <= @maxDequeueSize
      ORDER BY [{nameof(Message.Timestamp)}], [{nameof(Message.Id)}]
   )
   DELETE FROM M
   OUTPUT
      DELETED.[{nameof(Message.Subject)}],
      DELETED.[{nameof(Message.Id)}],
      DELETED.[{nameof(Message.Headers)}],
      DELETED.[{nameof(Message.Body)}],
      DELETED.[{nameof(Message.Timestamp)}]
   FROM {MessageConfiguration.NAME_TABLE} M
   INNER JOIN [MessageBatch] MB ON M.[{nameof(Message.Id)}] = MB.[{nameof(Message.Id)}]
   WHERE MB.[BatchSize] <= @maxDequeueSize
END");
		}

		/// <inheritdoc />
		protected override void Down(MigrationBuilder migrationBuilder)
		{
			migrationBuilder.Sql($"DROP PROCEDURE IF EXISTS {MessageConfiguration.NAME_SPROC_ENQUEUING}");
			migrationBuilder.Sql($"DROP PROCEDURE IF EXISTS {MessageConfiguration.NAME_SPROC_DEQUEUING}");
		}
	}
}
