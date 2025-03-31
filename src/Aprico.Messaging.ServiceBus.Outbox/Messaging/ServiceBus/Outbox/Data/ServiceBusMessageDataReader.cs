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
using Aprico.Data;
using Aprico.Messaging.ServiceBus.Extensions;
using Azure.Messaging.ServiceBus;

namespace Aprico.Messaging.ServiceBus.Outbox.Data;

internal class ServiceBusMessageDataReader : EnumeratorDataReader<ServiceBusMessage>
{
	internal ServiceBusMessageDataReader(IEnumerator<ServiceBusMessage> source, string destinationAggregate) : base(source, FIELDS_COUNT)
	{
		_destinationAggregate = destinationAggregate;
	}

	#region Base Class Member Overrides

	public override object GetValue(int index)
	{
		// @formatter:wrap_chained_method_calls chop_if_long
		return index switch {
			0 => Guid.Parse(_source.Current.MessageId), // TODO !! fix inconsistencies: ServiceBusMessageAssembler expects a string but SQL outbox enforces a Guid
			1 => _destinationAggregate,
			2 => _source.Current.ApplicationProperties.ToJson().ToString(),
			3 => _source.Current.Body.ToString(),
			4 => _source.Current.GetTimestamp(),
			_ => throw new ArgumentOutOfRangeException(nameof(index), index, "Outbox store column index must be between 0 and 4.")
		};
		// @formatter:wrap_chained_method_calls restore
	}

	#endregion

	private const int FIELDS_COUNT = 5;
	private readonly string _destinationAggregate;
}
