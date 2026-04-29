# Changelog

All notable changes to Analitiq Stream will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] - 2025-08-18

### Changed

#### Connector-driven engine (parameterise-connections-connectors)
- **Typed `ResolutionContext`** - new `src/engine/resolver.py` walks
  `ref` / `template` / `literal` / `function` JSON expressions over
  scopes (`connector`, `connection.{parameters,selections,discovered,
  auth_state}`, `secrets`, `auth`, `runtime`, `state`, `derived`,
  `request`, `response`).
- **Derived function registry** - new `src/engine/derived_functions.py`
  with `lookup`, `basic_auth`, `base64_encode`, `url_encode`. Connector
  authors can only call registered functions.
- **Generic transport factory** - new `src/shared/transport_factory.py`
  resolves `transports.<ref>` (with `transport_defaults` merge and
  fixpoint evaluation of `connector.derived`) and dispatches on `kind`
  (`sqlalchemy`, `http`) to materialise concrete transports.
- **`ConnectionRuntime`** - drives materialisation through the transport
  factory whenever the connector declares a `transports` block,
  validates `secret_refs` against the loaded secret store, and exposes
  the base SQL dialect via `runtime.driver` derived from
  `transports.<default>.driver`.
- **`PipelineConfigPrep`** - stops loading `ssl-mode-map.json` and the
  legacy top-level `connector.driver` field; SSL is now a `lookup` over
  `connection.parameters.ssl_mode` inside the connector's
  `connect_args.ssl`.
- **Aligned with `docs/connector-connection-parameterization.md`** -
  spec is committed alongside this PR; module docstrings cite it as the
  canonical reference.

### Removed

- Legacy hard-coded engine factory: `DIALECT_MAP`, `SSL_DIALECTS`,
  `DatabaseConnectionParams`, `extract_connection_params`,
  `create_database_engine`, `canonical_ssl_to_connect_arg`,
  `_create_api_session`. `src/shared/database_utils.py` now contains
  only pure SQL helpers (identifier validation, fully-qualified names,
  `DEFAULT` clause synthesis, read-side type coercion).
- Best-effort `EnrichedConfig` warning-only validation in
  `pipeline.py:_build_config_dict`. `ConnectionRuntime.materialize()`
  and the transport factory raise on bad config; the redundant warning
  pass masked real bugs.
- HTTP transport `rate_limit.time_window` alias. The single
  `time_window_seconds` key is now mandatory when `max_requests` is set
  (and vice versa).

### ✨ Added

#### Modern Engine Architecture
- **Pipeline Orchestration Layer** - New `PipelineOrchestrator` class for concurrent stream execution
- **Factory Pattern Implementation** - Factory methods for connector and pipeline stage creation
- **Enhanced Exception Hierarchy** - Specific exception types with contextual information:
  - `StreamExecutionError` with stage and batch context
  - `StreamConfigurationError` with stream ID and validation details
  - `PipelineOrchestrationError` for orchestration failures
  - `StageConfigurationError` for pipeline stage errors
- **Python 3.11+ Exception Handling** - `ExceptionGroup` patterns for concurrent failure handling

#### Pydantic v2 Integration
- **Engine Configuration Models** - `EngineConfig` with comprehensive validation
- **Stream Processing Models** - `StreamProcessingConfig` for stream-level validation
- **Metrics Models** - `PipelineMetricsSnapshot` with computed properties
- **Task Execution Models** - `TaskExecutionInfo` for task lifecycle tracking
- **Configuration Validation** - `ConfigDict(extra='forbid')` to prevent typos

#### Comprehensive Observability
- **Pipeline-level Metrics** - Real-time performance tracking with success rates
- **Structured Logging** - Correlation IDs and contextual information throughout
- **Task Lifecycle Monitoring** - Track stream tasks from pending to completion
- **Performance Metrics** - Records/second, batch sizes, processing rates
- **Error Aggregation** - Detailed error context and categorization

#### Enhanced API Connector
- **Tie-breaker Deduplication** - Advanced deduplication logic with multiple fields
- **Incremental Replication** - Enhanced cursor tracking with safety windows
- **State Persistence** - Improved state saving for tie-breaker information
- **Configuration Inheritance** - Proper field inheritance from stream to connector config

### 🔧 Improved

#### Type Safety & Validation
- **Comprehensive Type Hints** - Full type annotations throughout the engine
- **Pydantic v2 Features** - Modern validation with field constraints and custom validators
- **Configuration Validation** - Front-loaded validation to catch errors early
- **Field Constraints** - Proper bounds checking for numeric values

#### Error Handling & Resilience
- **Contextual Error Messages** - Rich error information with stream, stage, and batch context
- **Exception Sanitization** - Prevent information leakage in error messages
- **Graceful Degradation** - Partial failure handling with detailed reporting
- **Error Recovery** - Better error context for debugging and recovery

#### Performance & Efficiency
- **Concurrent Stream Processing** - Independent stream execution with parallel processing
- **Factory-based Creation** - Reduced object creation overhead
- **Memory Management** - Improved async queue usage and batch processing
- **State Management** - Enhanced sharded state with better performance

### 🧪 Testing

#### Comprehensive Test Suite
- **`test_engine_improvements.py`** - 18 tests covering engine architecture
- **`test_orchestrator.py`** - 17 tests covering pipeline orchestration
- **Configuration Testing** - Pydantic model validation and error handling
- **Exception Testing** - Python 3.11+ exception patterns and context
- **Integration Testing** - End-to-end pipeline execution with proper mocking
- **Async Testing** - Proper async test patterns with cleanup

### 📚 Documentation

#### Enhanced Documentation
- **`ENGINE_ARCHITECTURE.md`** - Comprehensive technical documentation
- **Updated README.md** - Modern architecture overview and usage examples
- **Code Examples** - Real-world usage patterns and best practices
- **Migration Guide** - Instructions for upgrading to new architecture
- **API Documentation** - Enhanced docstrings and type annotations

### 🔄 Changed

#### Architecture Separation
- **Layer Separation** - Clear separation between Pipeline, Orchestration, Engine, and Connector layers
- **Configuration Management** - Enhanced configuration loading and validation
- **State Management** - Improved sharded state handling with better error recovery
- **Connector Abstraction** - More abstract API connector suitable for both sources and destinations

#### Backward Compatibility
- **Interface Preservation** - Maintained backward compatibility for existing `stream_data` interface
- **Configuration Format** - Existing configuration formats continue to work
- **Migration Path** - Clear upgrade path to new features without breaking changes

### 🐛 Fixed

#### Data Integrity
- **Duplicate Record Prevention** - Fixed critical deduplication bug in API connector
- **Tie-breaker State Saving** - Fixed missing tie-breaker fields in state persistence
- **Configuration Inheritance** - Fixed missing `tie_breaker_fields` in engine configuration
- **State Consistency** - Improved state file format and validation

#### Error Handling
- **Exception Context** - Fixed missing context in error messages
- **Concurrent Failures** - Better handling of multiple stream failures
- **Resource Cleanup** - Improved cleanup of async tasks and connections
- **State Recovery** - Enhanced state recovery after failures

### 🔒 Security

#### Enhanced Security Features
- **Input Validation** - Comprehensive validation with Pydantic v2
- **Configuration Fingerprinting** - State consistency validation
- **URL Validation** - Prevent injection attacks with proper URL validation
- **Exception Sanitization** - Prevent information leakage in error messages
- **Path Validation** - Protected file operations with encoding specification

---

## Development Notes

### Technical Improvements Summary

**Architecture:**
- Implemented modern layered architecture with clear separation of concerns
- Added pipeline orchestration layer for better concurrent execution management
- Enhanced exception hierarchy with Python 3.11+ patterns
- Improved factory patterns for better testability and modularity

**Validation & Type Safety:**
- Upgraded to Pydantic v2 with modern features (`ConfigDict`, field validators)
- Added comprehensive type hints throughout the codebase
- Implemented proper bounds checking and constraint validation
- Enhanced configuration validation with detailed error reporting

**Observability:**
- Added pipeline-level metrics with real-time performance tracking
- Implemented structured logging with correlation IDs
- Enhanced error context and aggregation
- Improved monitoring capabilities with task lifecycle tracking

**Testing:**
- Created comprehensive test suite with 35+ tests
- Implemented proper async testing patterns
- Added integration tests for end-to-end scenarios
- Enhanced test coverage for critical functionality

**Documentation:**
- Created detailed technical documentation
- Updated README with modern architecture examples
- Added migration guide for existing users
- Enhanced inline documentation and type annotations

### Future Considerations

- **Performance Monitoring** - Consider adding OpenTelemetry integration
- **Advanced Metrics** - Explore Prometheus metrics export
- **Plugin System** - Extensible connector architecture for custom integrations
- **Schema Evolution** - Enhanced schema management with automatic migrations

---

*This changelog follows the [Keep a Changelog](https://keepachangelog.com/) format for clear communication of changes to users and developers.*