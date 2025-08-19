# State File Specification

**Version**: 1.0  
**Last Updated**: 2025-08-15  
**Framework**: Analitiq Stream  

This document provides the complete technical specification for Analitiq Stream's sharded state management system.

## 📁 File Structure Overview

```
state/
  {pipeline-id}/
    v1/                              # State format version
    ├── streams/
    │   └── {stream-name}/
    │       ├── partition-default.json      # Single partition (empty {})
    │       ├── partition-a1b2c3d4.json     # Hashed partition identifier
    │       └── partition-e5f6g7h8.json     # Another partition
    ├── index.json                   # Master manifest and metadata
    └── lock                        # File-based coordination (optional)
```

## 🔍 Partition State File Schema

### File Naming Convention
- **Pattern**: `partition-{identifier}.json`
- **Default Partition**: `partition-default.json` (when partition = `{}`)
- **Hash Generation**: MD5 hash of `JSON.stringify(partition, {sort: true})` truncated to 8 characters

### Complete Schema

```json
{
  "partition": {
    "description": "Partition key identifying the data subset",
    "type": "object",
    "examples": [
      {},
      {"account_id": "12345"},
      {"region": "EU", "account_type": "business"}
    ]
  },
  "cursor": {
    "description": "Current position within the data stream",
    "type": "object",
    "required": true,
    "properties": {
      "primary": {
        "description": "Primary cursor field for ordering and filtering",
        "type": "object",
        "required": true,
        "properties": {
          "field": {
            "description": "Name of the replication key field",
            "type": "string",
            "examples": ["created", "updated", "id", "timestamp"]
          },
          "value": {
            "description": "Current cursor value (timestamp, ID, etc.)",
            "type": ["string", "number", "null"],
            "examples": ["2025-08-14T11:58:03Z", 431245, null]
          },
          "inclusive": {
            "description": "Whether next query includes (>=) or excludes (>) this value",
            "type": "boolean",
            "default": true
          }
        }
      },
      "tiebreaker": {
        "description": "Secondary field to handle ties in primary cursor",
        "type": "object",
        "optional": true,
        "properties": {
          "field": {
            "description": "Tiebreaker field name (often 'id')",
            "type": "string"
          },
          "value": {
            "description": "Last processed tiebreaker value",
            "type": ["string", "number"]
          },
          "inclusive": {
            "description": "Tiebreaker inclusion mode",
            "type": "boolean",
            "default": true
          }
        }
      }
    }
  },
  "hwm": {
    "description": "High-water mark - highest cursor value seen",
    "type": ["string", "number"],
    "purpose": "Used for safety window calculations and resume validation"
  },
  "page_state": {
    "description": "Pagination continuation state",
    "type": "object",
    "optional": true,
    "properties": {
      "next_token": {
        "description": "Opaque pagination token from API",
        "type": "string"
      },
      "offset": {
        "description": "Numeric offset for offset-based pagination",
        "type": "integer"
      },
      "page": {
        "description": "Current page number for page-based pagination",
        "type": "integer"
      },
      "cursor": {
        "description": "Cursor value for cursor-based pagination",
        "type": "string"
      }
    }
  },
  "http_conditionals": {
    "description": "HTTP conditional headers for efficient polling",
    "type": "object",
    "optional": true,
    "properties": {
      "etag": {
        "description": "ETag header value for conditional requests",
        "type": "string",
        "format": "quoted-string"
      },
      "last_modified": {
        "description": "Last-Modified header value",
        "type": "string",
        "format": "RFC-2822"
      },
      "if_none_match": {
        "description": "If-None-Match header for next request",
        "type": "string"
      },
      "if_modified_since": {
        "description": "If-Modified-Since header for next request",
        "type": "string"
      }
    }
  },
  "stats": {
    "description": "Processing statistics for monitoring",
    "type": "object",
    "required": true,
    "properties": {
      "records_synced": {
        "description": "Total records processed in this partition",
        "type": "integer",
        "minimum": 0
      },
      "batches_written": {
        "description": "Number of batches successfully written",
        "type": "integer",
        "minimum": 0
      },
      "last_checkpoint_at": {
        "description": "Timestamp of last successful checkpoint",
        "type": "string",
        "format": "ISO-8601"
      },
      "errors_since_checkpoint": {
        "description": "Number of errors since last successful checkpoint",
        "type": "integer",
        "minimum": 0
      }
    }
  },
  "last_updated": {
    "description": "Timestamp when this partition state was last modified",
    "type": "string",
    "format": "ISO-8601",
    "required": true
  }
}
```

## 📋 Index Manifest Schema

### File Location
- **Path**: `{pipeline-id}/v1/index.json`
- **Purpose**: Master registry and coordination point
- **Concurrency**: Protected by RLock during updates

### Complete Schema

```json
{
  "version": {
    "description": "State format version number",
    "type": "integer",
    "current": 1,
    "purpose": "Schema evolution and backward compatibility"
  },
  "streams": {
    "description": "Registry of all streams and their partitions",
    "type": "object",
    "patternProperties": {
      "^[a-zA-Z0-9._-]+$": {
        "description": "Stream name (often endpoint.{uuid})",
        "type": "object",
        "properties": {
          "schema_hash": {
            "description": "SHA256 hash of current schema for drift detection",
            "type": "string",
            "pattern": "^sha256:[a-f0-9]{16}$",
            "purpose": "Detect schema changes that might break processing"
          },
          "partitions": {
            "description": "List of all partitions in this stream",
            "type": "array",
            "items": {
              "type": "object",
              "properties": {
                "partition": {
                  "description": "Partition key (same as in partition state file)",
                  "type": "object"
                },
                "file": {
                  "description": "Relative path to partition state file",
                  "type": "string",
                  "pattern": ".*partition-[a-f0-9]{8}\\.json$"
                }
              }
            }
          }
        }
      }
    }
  },
  "run": {
    "description": "Current execution metadata",
    "type": "object",
    "properties": {
      "run_id": {
        "description": "Unique identifier for current execution",
        "type": "string",
        "format": "ISO-8601-timestamp-uuid",
        "example": "2025-08-14T11:55:00Z-d04f"
      },
      "lease_owner": {
        "description": "Identifier of worker/process holding the execution lease",
        "type": "string",
        "format": "worker-{thread-id}",
        "purpose": "Detect stale processes and coordinate leadership"
      },
      "started_at": {
        "description": "When the current run began",
        "type": "string",
        "format": "ISO-8601"
      },
      "checkpoint_seq": {
        "description": "Monotonic sequence number incremented on each checkpoint",
        "type": "integer",
        "minimum": 0,
        "purpose": "Detect checkpoint ordering and progress"
      },
      "config_fingerprint": {
        "description": "SHA256 hash of immutable pipeline configuration",
        "type": "string",
        "pattern": "^sha256:[a-f0-9]{16}$",
        "purpose": "Ensure state compatibility with current config"
      }
    }
  }
}
```

## 🔄 Concurrent Access Patterns

### Worker Isolation Strategy

1. **Partition Assignment**: Each worker processes distinct partitions
2. **File Isolation**: Each partition writes to separate state file
3. **Atomic Updates**: Temporary files + atomic replace operations
4. **Minimal Coordination**: Only index.json requires synchronization

### Concurrency Safety Guarantees

| Operation | Concurrency Level | Protection Method |
|-----------|-------------------|------------------|
| Partition checkpoint | **Full parallelism** | Separate files |
| Index manifest update | **Sequential** | RLock protection |
| Schema hash validation | **Read-parallel** | Immutable during run |
| Config fingerprint check | **Read-parallel** | Validated at startup |

### Performance Characteristics

- **Checkpoint Latency**: O(1) per partition, independent of other partitions
- **Storage Overhead**: ~500-2000 bytes per partition state file
- **Coordination Cost**: Single index.json update per checkpoint batch
- **Recovery Time**: O(partitions) to scan all state files

## 🛠 Implementation Details

### Hash Generation Algorithm
```python
def _get_partition_file(self, stream_name: str, partition: Dict[str, Any]) -> Path:
    if not partition:
        partition_id = "default"
    else:
        partition_str = json.dumps(partition, sort_keys=True)
        partition_id = hashlib.md5(partition_str.encode()).hexdigest()[:8]
    return self.streams_dir / stream_name / f"partition-{partition_id}.json"
```

### Atomic Write Pattern
```python
def _save_partition_state(self, partition_file: Path, state: Dict[str, Any]):
    partition_file.parent.mkdir(parents=True, exist_ok=True)
    temp_file = partition_file.with_suffix(".tmp")
    
    try:
        with open(temp_file, "w") as f:
            json.dump(state, f, indent=2)
        temp_file.replace(partition_file)  # Atomic on most filesystems
    except Exception as e:
        if temp_file.exists():
            temp_file.unlink()
        raise e
```

### Configuration Fingerprint Calculation
```python
def _compute_config_fingerprint(self, config: Dict[str, Any]) -> str:
    immutable_config = {
        "pipeline_id": config.get("pipeline_id"),
        "src": config.get("src", {}),
        "dst": config.get("dst", {}),
        "mapping": config.get("mapping", {}),
        "engine_config": config.get("engine_config", {})
    }
    config_str = json.dumps(immutable_config, sort_keys=True)
    return f"sha256:{hashlib.sha256(config_str.encode()).hexdigest()[:16]}"
```

## 🔧 Operational Procedures

### Resume After Crash
1. Load index.json to discover all partitions
2. Validate config fingerprint matches current configuration
3. Load each partition state file for cursor positions
4. Continue processing from last recorded cursors

### Scale-Out (Add Workers)
1. Assign different partition keys to new workers
2. Workers automatically create new partition state files
3. No coordination required between workers processing different partitions

### Scale-Down (Remove Workers) 
1. Ensure all partition checkpoints are saved
2. Remaining workers can pick up abandoned partitions
3. State files remain valid for future resume

### Schema Evolution
1. Schema hash changes trigger drift detection
2. Can choose to continue (with warning) or halt pipeline
3. New schema hash recorded in index.json on first successful batch

## 📊 Monitoring & Debugging

### Key Metrics to Track
- `checkpoint_seq`: Progress indicator
- `records_synced`: Throughput per partition  
- `errors_since_checkpoint`: Error rate
- `last_checkpoint_at`: Staleness detection

### Troubleshooting Commands
```bash
# List all partitions for a stream
find state/wise-to-sevdesk/v1/streams/ -name "partition-*.json" | head -10

# Check partition state
cat state/wise-to-sevdesk/v1/streams/endpoint.123/partition-default.json | jq '.cursor'

# Validate index consistency
cat state/wise-to-sevdesk/v1/index.json | jq '.streams | keys[]'

# Monitor checkpoint progress  
watch -n 5 'cat state/wise-to-sevdesk/v1/index.json | jq ".run.checkpoint_seq"'
```

## ⚠️ Limitations & Considerations

### Current Limitations
1. **Single partition per worker**: Each worker should process distinct partitions
2. **File system dependency**: Requires shared filesystem for multi-node deployment
3. **JSON overhead**: Text format has higher storage overhead than binary
4. **Schema evolution**: Breaking changes require manual intervention

### Best Practices
1. **Partition by time/geography/account**: Ensures natural work distribution
2. **Monitor partition sizes**: Avoid highly skewed partition distributions  
3. **Regular cleanup**: Archive old state files periodically
4. **Backup strategy**: Include state files in disaster recovery procedures

---

**Note**: This specification covers Analitiq Stream State Format Version 1. Future versions will maintain backward compatibility or provide migration tools.