using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Aprico.Migrations
{
	/// <inheritdoc />
	public partial class CreateOutboxStore : Migration
	{
		/// <inheritdoc />
		protected override void Up(MigrationBuilder migrationBuilder)
		{
			migrationBuilder.EnsureSchema(
				name: "outbox");

			migrationBuilder.CreateTable(
				name: "Messages",
				schema: "outbox",
				columns: table => new
				{
					Id = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
					DestinationAggregate = table.Column<string>(type: "nvarchar(256)", maxLength: 256, nullable: false),
					Headers = table.Column<string>(type: "nvarchar(max)", nullable: false),
					Body = table.Column<string>(type: "nvarchar(max)", nullable: false),
					Timestamp = table.Column<DateTimeOffset>(type: "datetimeoffset(7)", nullable: false)
				},
				constraints: table =>
				{
					table.PrimaryKey("PK_Messages", x => x.Id);
				});
		}

		/// <inheritdoc />
		protected override void Down(MigrationBuilder migrationBuilder)
		{
			migrationBuilder.DropTable(
				name: "Messages",
				schema: "outbox");
		}
	}
}
