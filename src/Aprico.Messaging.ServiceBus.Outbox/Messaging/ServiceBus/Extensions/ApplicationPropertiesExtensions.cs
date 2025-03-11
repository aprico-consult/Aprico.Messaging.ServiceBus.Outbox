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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text.Json;
using Be.Stateless.Extensions;

namespace Aprico.Messaging.ServiceBus.Extensions;

internal static class ApplicationPropertiesExtensions
{
	internal static IReadOnlyDictionary<string, object> ToDictionary(this string applicationProperties)
	{
		return JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(applicationProperties)
			.UnlessIsNull()
			.Where(static kvp => kvp.Value.ValueKind != JsonValueKind.Null && (kvp.Value.ValueKind != JsonValueKind.String || kvp.Value.GetString() != null))
			.ToDictionary(static kvp => kvp.Key, static kvp => kvp.Value.ToObject())
			.AsReadOnly();
	}

	[SuppressMessage("ReSharper", "ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract")]
	internal static BinaryData ToJson(this IDictionary<string, object> applicationProperties)
	{
		ArgumentNullException.ThrowIfNull(applicationProperties);
		var filteredProperties = applicationProperties.Where(static kvp => kvp.Value != null);
		return BinaryData.FromBytes(JsonSerializer.SerializeToUtf8Bytes(filteredProperties.ToDictionary()));
	}

	[SuppressMessage("ReSharper", "NullableWarningSuppressionIsUsed", Justification = "Null strings have been filtered out by the caller.")]
	[SuppressMessage("ReSharper", "SwitchExpressionHandlesSomeKnownEnumValuesWithExceptionInDefault", Justification = "This is by design.")]
	private static object ToObject(this JsonElement element)
	{
		return element.ValueKind switch {
			JsonValueKind.String => element.GetString()!,
			JsonValueKind.Number => element.TryGetInt32(out var intValue) ? intValue : element.TryGetDouble(out var doubleValue) ? doubleValue : element.GetDecimal(),
			JsonValueKind.True => true,
			JsonValueKind.False => false,
			_ => throw new NotSupportedException($"Cannot convert {element.ValueKind} {nameof(JsonElement)} '{element.GetRawText()}'.")
		};
	}
}
