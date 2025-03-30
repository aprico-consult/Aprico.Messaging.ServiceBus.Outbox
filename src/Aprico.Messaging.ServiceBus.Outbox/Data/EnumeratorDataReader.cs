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

namespace Aprico.Data;

internal abstract class EnumeratorDataReader<TSource> : IDataReader
{
	internal EnumeratorDataReader(IEnumerator<TSource> source, int fieldCount)
	{
		_source = source;
		FieldCount = fieldCount;
	}

	#region IDataReader Members

	public void Dispose()
	{
		_source.Dispose();
	}

	public bool GetBoolean(int i)
	{
		throw new NotSupportedException();
	}

	public byte GetByte(int i)
	{
		throw new NotSupportedException();
	}

	public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length)
	{
		throw new NotSupportedException();
	}

	public char GetChar(int i)
	{
		throw new NotSupportedException();
	}

	public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length)
	{
		throw new NotSupportedException();
	}

	public IDataReader GetData(int i)
	{
		throw new NotSupportedException();
	}

	public string GetDataTypeName(int i)
	{
		throw new NotSupportedException();
	}

	public DateTime GetDateTime(int i)
	{
		throw new NotSupportedException();
	}

	public decimal GetDecimal(int i)
	{
		throw new NotSupportedException();
	}

	public double GetDouble(int i)
	{
		throw new NotSupportedException();
	}

	public Type GetFieldType(int i)
	{
		throw new NotSupportedException();
	}

	public float GetFloat(int i)
	{
		throw new NotSupportedException();
	}

	public Guid GetGuid(int i)
	{
		throw new NotSupportedException();
	}

	public short GetInt16(int i)
	{
		throw new NotSupportedException();
	}

	public int GetInt32(int i)
	{
		throw new NotSupportedException();
	}

	public long GetInt64(int i)
	{
		throw new NotSupportedException();
	}

	public virtual string GetName(int i)
	{
		throw new NotSupportedException();
	}

	public virtual int GetOrdinal(string name)
	{
		throw new NotSupportedException();
	}

	public string GetString(int i)
	{
		throw new NotSupportedException();
	}

	public virtual object GetValue(int index)
	{
		throw new NotSupportedException();
	}

	public int GetValues(object[] values)
	{
		throw new NotSupportedException();
	}

	public bool IsDBNull(int i)
	{
		throw new NotSupportedException();
	}

	public int FieldCount { get; }

	public object this[int i] => throw new NotSupportedException();

	public object this[string name] => throw new NotSupportedException();

	public void Close()
	{
		throw new NotSupportedException();
	}

	public DataTable GetSchemaTable()
	{
		throw new NotSupportedException();
	}

	public bool NextResult()
	{
		throw new NotSupportedException();
	}

	public bool Read()
	{
		return _source.MoveNext();
	}

	public int Depth => throw new NotSupportedException();

	public bool IsClosed => throw new NotSupportedException();

	public int RecordsAffected => throw new NotSupportedException();

	#endregion

	protected readonly IEnumerator<TSource> _source;
}
