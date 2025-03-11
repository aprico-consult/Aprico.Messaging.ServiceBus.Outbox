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
using System.Collections.ObjectModel;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

namespace Aprico.Messaging.ServiceBus.Extensions;

public abstract class ApplicationPropertiesExtensionsFixture
{
	#region Nested Type: ToDictionary

	[SuppressMessage("Naming", "CA1711:Identifiers should not have incorrect suffix")]
	public class ToDictionary : ApplicationPropertiesExtensionsFixture
	{
		[Fact]
		[SuppressMessage("ReSharper", "NullableWarningSuppressionIsUsed")]
		public void FilterOutNullValues()
		{
			"{\"CorrelationId\":\"id\",\"MessageType\":null,\"SomeProperty\":null,\"Key\":\"\"}".ToDictionary()
				.Should()
				.BeEquivalentTo(
					new Dictionary<string, object> {
						{ "CorrelationId", "id" },
						{ "Key", "" }
					});
		}

		[Fact]
		public void ReturnsCorrespondingDictionaryWhereValuesAreNotJsonElement()
		{
			"{\"key1\": \"value1\", \"key2\": 42, \"key3\": true}".ToDictionary()
				.Should()
				.NotBeNull()
				.And.HaveCount(expected: 3)
				// any of the following lines would fail if value was a JsonElement
				.And.Contain("key1", "value1")
				.And.Contain("key2", value: 42)
				.And.Contain("key3", value: true);
		}

		[Fact]
		public void ReturnsEmptyDictionaryForEmptyJson()
		{
			"{}".ToDictionary()
				.Should()
				.NotBeNull()
				.And.BeEmpty();
		}

		[Fact]
		public void ReturnsEquivalentDictionary()
		{
			var correlationId = Guid.NewGuid();

			("{" + $"\"CorrelationId\":\"{correlationId}\"," + "\"MessageType\":\"urn:aprico:app#root\"," + "\"SomeProperty\":\"SomeValue\"" + "}").ToDictionary()
				.Should()
				.NotBeNull()
				.And.HaveCount(expected: 3)
				.And.BeEquivalentTo(
					new Dictionary<string, object> {
						{ "CorrelationId", correlationId.ToString() },
						{ "MessageType", "urn:aprico:app#root" },
						{ "SomeProperty", "SomeValue" }
					});
		}

		[Fact]
		public void ReturnsReadOnlyDictionary()
		{
			"{\"key1\": \"value1\"}".ToDictionary()
				.Should()
				.NotBeNull()
				.And.BeOfType<ReadOnlyDictionary<string, object>>()
				.And.BeAssignableTo<IReadOnlyDictionary<string, object>>();
		}

		[Fact]
		[SuppressMessage("ReSharper", "NullableWarningSuppressionIsUsed")]
		public void ThrowsArgumentNullExceptionForNullJson()
		{
			string jsonString = null!;

			Invoking(() => jsonString.ToDictionary())
				.Should()
				.Throw<ArgumentNullException>();
		}

		[Fact]
		public void ThrowsJsonExceptionForInvalidJson()
		{
			Invoking(static () => "{invalid json}".ToDictionary())
				.Should()
				.ThrowExactly<JsonException>();
		}
	}

	#endregion

	#region Nested Type: ToJson

	public class ToJson : ApplicationPropertiesExtensionsFixture
	{
		[Fact]
		[SuppressMessage("ReSharper", "NullableWarningSuppressionIsUsed")]
		public void FilterOutNullValues()
		{
			string nil = null!;
			new Dictionary<string, object> {
					["CorrelationId"] = "id",
					["MessageType"] = null!,
					["SomeProperty"] = nil,
					["Key"] = ""
				}.ToJson()
				.ToString()
				.Should()
				.Be("{\"CorrelationId\":\"id\",\"Key\":\"\"}");
		}

		[Fact]
		public void ReturnsEmptyJsonObjectForEmptyDictionary()
		{
			new Dictionary<string, object>().ToJson()
				.ToString()
				.Should()
				.Be("{}");
		}

		[Fact]
		public void ReturnsJsonCorrespondingToDictionary()
		{
			var correlationId = Guid.NewGuid();
			var sut = new Dictionary<string, object> {
				["CorrelationId"] = correlationId,
				["MessageType"] = "urn:aprico:app#root",
				["SomeProperty"] = "SomeValue",
				["key2"] = 42,
				["key3"] = true
			};

			sut.ToJson()
				.ToString()
				.Should()
				.Be(
					// @formatter:keep_existing_arrangement true
					"{"
					+ $"\"CorrelationId\":\"{correlationId}\","
					+ "\"MessageType\":\"urn:aprico:app#root\","
					+ "\"SomeProperty\":\"SomeValue\","
					+ "\"key2\":42,"
					+ "\"key3\":true"
					+ "}"
					// @formatter:keep_existing_arrangement restore
				);
		}

		[Fact]
		[SuppressMessage("ReSharper", "NullableWarningSuppressionIsUsed")]
		public void ThrowsArgumentNullExceptionForNullDictionary()
		{
			Dictionary<string, object> dictionary = null!;
			Invoking(() => dictionary.ToJson())
				.Should()
				.Throw<ArgumentNullException>();
		}
	}

	#endregion
}
