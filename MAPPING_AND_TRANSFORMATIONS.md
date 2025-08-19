# Field Mapping, Transformations & Validation Guide

This guide explains the **Field-Centric Mapping** system in Analitiq Stream, which consolidates field mapping, transformations, and validation into a single, efficient configuration structure.

## Overview

The field-centric approach provides:
- **Unified Configuration**: Map, transform, and validate fields in one place
- **Processing Efficiency**: Single pass through data with all operations
- **Better Maintainability**: Complete field lifecycle visibility
- **Integrated Validation**: Validation rules applied after transformations

## Configuration Structure

### Basic Structure

```json
{
  "field_mappings": {
    "source_field_name": {
      "target": "destination_field_name",
      "transformations": ["transform1", "transform2"],
      "validation": {
        "rules": [{"type": "validation_type", "param": "value"}],
        "error_action": "dlq|skip|fail"
      }
    }
  },
  "computed_fields": {
    "new_field_name": {
      "expression": "computed_value_or_expression",
      "validation": {
        "rules": [...],
        "error_action": "dlq|skip|fail"
      }
    }
  },
  "default_error_action": "fail"
}
```

### Field Mappings

Field mappings define how source fields are mapped to destination fields with optional transformations and validation.

#### Simple Direct Mapping
```json
{
  "field_mappings": {
    "source_field": {
      "target": "destination_field"
    }
  }
}
```

#### Default Target Name
If no `target` is specified, the source field name is used:
```json
{
  "field_mappings": {
    "field_name": {}  // Maps to "field_name" in destination
  }
}
```

#### Nested Field Access
Use dot notation to access nested fields:
```json
{
  "field_mappings": {
    "user.profile.name": {
      "target": "full_name"
    },
    "details.merchant.category": {
      "target": "merchant_category"
    }
  }
}
```

### Computed Fields

Computed fields create new fields using expressions that can reference source data, environment variables, or function calls.

#### Static Values
```json
{
  "computed_fields": {
    "source_system": {
      "expression": "wise"
    }
  }
}
```

#### Environment Variables
```json
{
  "computed_fields": {
    "account_id": {
      "expression": "${BANK_ACCOUNT_ID}"
    }
  }
}
```

#### Field References
```json
{
  "computed_fields": {
    "transaction_key": {
      "expression": "txn_${id}_${date}"
    }
  }
}
```

#### Function Calls
```json
{
  "computed_fields": {
    "created_at": {
      "expression": "now()"
    },
    "unique_id": {
      "expression": "uuid()"
    }
  }
}
```

## Available Transformations

### String Transformations

| Transformation | Description | Example |
|---------------|-------------|---------|
| `strip` | Remove whitespace | `"  hello  "` → `"hello"` |
| `upper` | Convert to uppercase | `"Hello"` → `"HELLO"` |
| `lower` | Convert to lowercase | `"Hello"` → `"hello"` |
| `truncate` | Limit string length | `"very long text"` → `"very long te"` (default: 50 chars) |
| `regex_replace` | Replace using regex | Requires parameters |

### Numeric Transformations

| Transformation | Description | Example |
|---------------|-------------|---------|
| `abs` | Absolute value | `-123.45` → `123.45` |
| `number_format` | Format with decimals | `3.14159` → `"3.14"` (default: 2 decimals) |

### Date/Time Transformations

| Transformation | Description | Example |
|---------------|-------------|---------|
| `iso_to_date` | ISO datetime to date | `"2023-12-25T10:30:00Z"` → `"2023-12-25"` |
| `iso_to_datetime` | ISO to formatted datetime | `"2023-12-25T10:30:00Z"` → `"2023-12-25 10:30:00"` |
| `now` | Current timestamp | Any input → `"2023-12-25T10:30:00.123456"` |

### Utility Transformations

| Transformation | Description | Example |
|---------------|-------------|---------|
| `uuid` | Generate UUID4 | Any input → `"550e8400-e29b-41d4-a716-446655440000"` |
| `md5` | MD5 hash | `"hello"` → `"5d41402abc4b2a76b9719d911017c592"` |
| `boolean` | Convert to boolean | `"true"`, `"1"`, `"yes"` → `true` |
| `coalesce` | First non-null value | Multiple inputs → First valid value |

### Transformation Chaining

Transformations are applied in order:
```json
{
  "field_mappings": {
    "messy_amount": {
      "target": "clean_amount",
      "transformations": ["strip", "abs", "number_format"]
    }
  }
}
```

Processing flow: `"  -123.456  "` → `"-123.456"` → `123.456` → `"123.46"`

## Validation Rules

### Rule Types

#### 1. `not_null` - Required Field Validation
Ensures field is not null or empty.

```json
{
  "validation": {
    "rules": [{"type": "not_null"}],
    "error_action": "dlq"
  }
}
```

**Enum Attributes**: None
**Valid Values**: Any non-null, non-empty value

#### 2. `enum` - Enumerated Values Validation
Restricts field to specific allowed values.

```json
{
  "validation": {
    "rules": [{
      "type": "enum",
      "values": ["active", "inactive", "pending"]
    }],
    "error_action": "skip"
  }
}
```

**Enum Attributes**: 
- `values` (required): Array of allowed values

**Valid Values**: Exactly one of the values in the `values` array

#### 3. `range` - Numeric Range Validation
Validates numeric values within specified bounds.

```json
{
  "validation": {
    "rules": [{
      "type": "range",
      "min": 0,
      "max": 1000
    }],
    "error_action": "fail"
  }
}
```

**Enum Attributes**: 
- `min` (optional): Minimum allowed value (inclusive)
- `max` (optional): Maximum allowed value (inclusive)

**Valid Values**: Any numeric value within the specified range

#### 4. `regex` - Pattern Matching Validation
Validates field against regular expression pattern.

```json
{
  "validation": {
    "rules": [{
      "type": "regex",
      "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    }],
    "error_action": "dlq"
  }
}
```

**Enum Attributes**:
- `pattern` (required): Regular expression pattern as string

**Valid Values**: Any string matching the regex pattern

#### 5. `custom` - Custom Validation (Future)
Reserved for future custom validation functions.

### Error Actions

Error actions determine how validation failures are handled:

#### `fail` - Stop Processing
Raises exception and stops pipeline processing.
```json
{
  "validation": {
    "rules": [{"type": "not_null"}],
    "error_action": "fail"
  }
}
```

**Use When**: Critical fields that must be valid for processing to continue

#### `dlq` - Dead Letter Queue
Logs error and excludes field, but continues processing. Failed records can be reviewed later.
```json
{
  "validation": {
    "rules": [{"type": "enum", "values": ["USD", "EUR"]}],
    "error_action": "dlq"
  }
}
```

**Use When**: Fields that should be valid but processing can continue without them

#### `skip` - Skip Field
Silently excludes invalid field and continues processing.
```json
{
  "validation": {
    "rules": [{"type": "range", "min": 0}],
    "error_action": "skip"
  }
}
```

**Use When**: Optional fields where invalid values should be ignored

### Multiple Validation Rules

Fields can have multiple validation rules that are applied in order:
```json
{
  "validation": {
    "rules": [
      {"type": "not_null"},
      {"type": "range", "min": 0, "max": 100}
    ],
    "error_action": "fail"
  }
}
```

## Processing Flow

The field mapping processor follows this sequence:

1. **Extract** source field value (including nested fields)
2. **Transform** value through transformation chain (if specified)
3. **Validate** transformed value against rules (if specified)
4. **Handle** validation errors according to error action
5. **Map** valid value to target field name

```
Source Field → Transformations → Validation → Target Field
     ↓              ↓              ↓           ↓
  Raw Value → Transformed Value → Validated → Final Value
```

## Complete Example

Here's a comprehensive example showing all features:

```json
{
  "field_mappings": {
    "id": {
      "target": "externalId",
      "validation": {
        "rules": [{"type": "not_null"}],
        "error_action": "dlq"
      }
    },
    "amount": {
      "target": "amount",
      "transformations": ["abs"],
      "validation": {
        "rules": [
          {"type": "not_null"},
          {"type": "range", "min": -1000000, "max": 1000000}
        ],
        "error_action": "dlq"
      }
    },
    "currency": {
      "target": "currency",
      "transformations": ["upper"],
      "validation": {
        "rules": [
          {"type": "not_null"},
          {"type": "enum", "values": ["EUR", "USD", "GBP", "CHF"]}
        ],
        "error_action": "dlq"
      }
    },
    "createdAt": {
      "target": "valueDate",
      "transformations": ["iso_to_date"],
      "validation": {
        "rules": [{"type": "not_null"}],
        "error_action": "dlq"
      }
    },
    "description": {
      "target": "purpose",
      "transformations": ["strip"],
      "validation": {
        "rules": [{"type": "not_null"}],
        "error_action": "skip"
      }
    },
    "details.merchant.name": {
      "target": "payeeName",
      "transformations": ["strip", "truncate"]
    }
  },
  "computed_fields": {
    "checkAccount": {
      "expression": "${BANK_ACCOUNT_ID}",
      "validation": {
        "rules": [{"type": "not_null"}],
        "error_action": "dlq"
      }
    },
    "transactionKey": {
      "expression": "wise_${externalId}_${valueDate}"
    },
    "sourceSystem": {
      "expression": "wise"
    },
    "syncedAt": {
      "expression": "now()"
    },
    "uniqueId": {
      "expression": "uuid()"
    }
  },
  "default_error_action": "fail"
}
```

## Best Practices

### 1. Field Organization
- Group related transformations together
- Apply validation after transformations
- Use descriptive target field names

### 2. Error Handling Strategy
- Use `fail` for critical fields that must be present
- Use `dlq` for important fields that should be reviewed if invalid
- Use `skip` for optional fields where invalid values can be ignored

### 3. Transformation Ordering
- Clean data first (strip, normalize)
- Transform data types (iso_to_date, abs)
- Format for output (number_format, truncate)

### 4. Performance Considerations
- Minimize transformation chains where possible
- Use direct mappings when no transformation is needed
- Consider computed fields for expensive operations

### 5. Environment Variables
- Use environment variables for sensitive configuration
- Provide meaningful names with clear purposes
- Document required environment variables

## Migration from Legacy Configuration

### Before (Verbose Transformations)
```json
{
  "transformations": [
    {
      "type": "field_mapping",
      "mappings": {"id": "externalId"}
    },
    {
      "type": "value_transformation",
      "field": "amount",
      "function": "abs"
    }
  ]
}
```

### After (Field-Centric)
```json
{
  "field_mappings": {
    "id": {
      "target": "externalId"
    },
    "amount": {
      "transformations": ["abs"]
    }
  }
}
```

**Benefits**:
- 60% reduction in configuration size
- Single location for all field-related logic
- Integrated validation without separate files
- Faster processing with single data pass

## Error Handling and Debugging

### Common Issues

1. **Transformation Not Found**
   ```
   Error: Invalid transformation 'invalid_name'
   ```
   **Solution**: Check available transformations with `list_available_transformations()`

2. **Validation Rule Failure**
   ```
   Error: Field 'amount' value '150' above maximum 100
   ```
   **Solution**: Review validation rules and source data

3. **Missing Environment Variable**
   ```
   Warning: Environment variable 'API_TOKEN' not found
   ```
   **Solution**: Set required environment variables

### Debugging Tips

1. **Enable Detailed Logging**
   ```python
   logging.getLogger('analitiq_stream.mapping').setLevel(logging.DEBUG)
   ```

2. **Test Field Mappings Individually**
   ```python
   processor = FieldMappingProcessor(config)
   result = processor.process_record(test_record)
   ```

3. **Validate Configuration**
   ```python
   # Configuration is validated during processor initialization
   try:
       processor = FieldMappingProcessor(config)
   except MappingError as e:
       print(f"Configuration error: {e}")
   ```

This field-centric mapping system provides a powerful, efficient, and maintainable approach to data transformation and validation in your streaming pipelines.