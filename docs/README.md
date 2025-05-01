# CoreImpact.ParquetMapper

**CoreImpact.ParquetMapper** is a .NET library for reading and writing [Parquet](https://parquet.apache.org/) files using strongly-typed C# classes. It enables seamless serialization and deserialization of data with schema support, attribute-based mapping, and compatibility validation tools.

---

## Features

- ✅ Write and read Parquet files using generic classes  
- ✅ Async and sync methods  
- ✅ Attribute-based column mapping  
- ✅ Case-insensitive and punctuation-tolerant property matching  
- ✅ Schema generation from C# classes  
- ✅ Schema compatibility and comparison utilities

---

## Installation

When available via NuGet:

```bash
dotnet add package CoreImpact.ParquetMapper
```

Or reference the source project directly in your solution.

---
## Interfaces

The core functionality is encapsulated within the `ParquetMapper` class, which implements three main interfaces:
  
  - **IParquetWriter:**  
    Provides methods to write data to Parquet files from both synchronous and asynchronous sources.
    ```csharp
    public interface IParquetWriter
    {
        Task WriteToParquetFileAsync<TDataType>(IAsyncEnumerable<TDataType> data, string path, int rowGroupSize = 1_000_000, CancellationToken cancellationToken = default) where TDataType : new();
        Task WriteToParquetFileAsync<TDataType>(IEnumerable<TDataType> data, string path, int rowGroupSize = 1_000_000, CancellationToken cancellationToken = default) where TDataType : new();
    }
    ```
  
  - **IParquetReader:**  
    Offers methods to read an entire Parquet file or to stream its data as asynchronous enumerable collections.
    ```csharp
    public interface IParquetReader
    {
        Task<ParquetData<TDataType>> ReadParquetAsync<TDataType>(string path, CancellationToken cancellationToken = default) where TDataType : new();
        IAsyncEnumerable<TDataType[]> ReadParquetAsAsyncEnumerable<TDataType>(string path, CancellationToken cancellationToken = default) where TDataType : new();
    }
    ```
  
  - **ISchemaCreator:**  
    Provides functionality to create a Parquet schema from a class type.
    ```csharp
    public interface ISchemaCreator
    {
        ParquetSchema CreateParquetSchema<TDataType>() where TDataType : new();
        ParquetSchema CreateParquetSchema(Type type);
    }
    ```
---
## Attribute Support

Customize how your class properties are mapped with these attributes:

### `HasParquetColNameAttribute(string name)`

Assigns a specific column name for a property.

```csharp
[HasParquetColName("full_name")]
public string Name { get; set; }
```

### `IgnoreCasingAttribute`

Can be applied to a class or individual properties. Enables case-insensitive matching and ignores `-`, `_`, and whitespace.

```csharp
[IgnoreCasing]
public class Person { ... }
```

### `IgnorePropertyAttribute`

Skips a property during both serialization and deserialization.

```csharp
[IgnoreProperty]
public string TemporaryData { get; set; }
```

---

## Schema Extension Methods

Extension methods for validating and comparing Parquet schemas:

```csharp
bool IsSchemaCompatible<TDataType>(this ParquetSchema parquetSchema);
bool TryCompareSchema<TDataType>(this ParquetSchema parquetSchema, out Dictionary<string, PropertyInfo>? result);
Dictionary<string, PropertyInfo> CompareSchema<TDataType>(this ParquetSchema parquetSchema);
```

These allow comparing a Parquet schema with a C# class and retrieving mapped columns as `Dictionary<string, PropertyInfo>`.

---
## Getting Started

### Basic Usage

Below are simplified examples illustrating how to use the main functionality of CoreImpact.ParquetMapper.

#### Example

```csharp
public class Person
{
    [HasParquetColName("full_name")]
    public string Name { get; set; }

    public int Age { get; set; }

    [IgnoreProperty]
    public string InternalNote { get; set; }
}

IEnumerable<Person> data = GetData(); // Your data source here

// Writing
await parquetMapper.WriteToParquetFileAsync(data, "people.parquet");

// Reading
var people = await parquetMapper.ReadParquetAsync<Person>("people.parquet");

// Schema compatibility
var schema = parquetMapper.CreateParquetSchema<Person>();
if (schema.IsSchemaCompatible<Person>())
{
    // Schema is compatible
}
```


---


## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for further details.

---
