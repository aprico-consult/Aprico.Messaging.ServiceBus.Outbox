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

namespace Aprico.Messaging.ServiceBus.Outbox.Settings;

/// <summary>Configuration settings for the transactional outbox.</summary>
[SuppressMessage("ReSharper", "MemberCanBeInternal", Justification = "Public API.")]
[SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global", Justification = "Public API.")]
public class OutboxSettings
{
	/// <summary>The maximum number of messages that can be dequeued from the outbox and dispatched in a single operation.</summary>
	/// <remarks>
	/// <para>Each dequeue operation only retrieves messages pertaining to the same subject.</para>
	/// <para>Defaults to <see cref="DEFAULT_MAX_DEQUEUE_COUNT"/>, i.e. <c>10</c>.</para>
	/// </remarks>
	public int MaxDequeueCount { get; set; } = DEFAULT_MAX_DEQUEUE_COUNT;

	/// <summary>
	/// The maximum total size, in bytes, of all messages that can be dequeued and dispatched in a single operation. This
	/// limit applies to the combined size of all messages in the operation.
	/// </summary>
	/// <remarks>
	/// <para>Each dequeue operation only retrieves messages pertaining to the same subject.</para>
	/// <para>Defaults to <see cref="DEFAULT_MAX_DEQUEUE_SIZE"/>, typically <c>256 KB</c> for a standard-tier Azure Service Bus queue.</para>
	/// </remarks>
	public int MaxDequeueSize { get; set; } = DEFAULT_MAX_DEQUEUE_SIZE;

	/// <summary>
	/// The maximum allowed size, in bytes, of an individual message. This helps prevent over-sized messages from being
	/// enqueued or dispatched.
	/// </summary>
	/// <remarks>
	/// Defaults to <see cref="DEFAULT_MAX_MESSAGE_SIZE"/>, typically <c>256 KB</c> for a standard-tier Azure Service Bus
	/// queue.
	/// </remarks>
	public int MaxMessageSize { get; set; } = DEFAULT_MAX_MESSAGE_SIZE;

	internal const int DEFAULT_MAX_DEQUEUE_COUNT = 10;
	internal const int DEFAULT_MAX_DEQUEUE_SIZE = DEFAULT_MAX_MESSAGE_SIZE;
	internal const int DEFAULT_MAX_MESSAGE_SIZE = 256 * 1024;
}
