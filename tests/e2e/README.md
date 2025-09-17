# E2E Tests: Comprehensive Edge Case Testing

This directory contains comprehensive end-to-end tests for the Analitiq Stream framework, focusing on edge cases and fault tolerance scenarios that stress-test the data streaming capabilities.

## Test Categories

### 🛡️ Fault Tolerance (`test_fault_tolerance_e2e.py`)
Tests comprehensive fault tolerance scenarios including:
- Circuit breaker activation under repeated failures
- Retry exhaustion leading to dead letter queue
- Partial batch failure recovery
- Cascading failure isolation across streams
- Checkpoint corruption recovery
- Resource exhaustion handling
- Concurrent pipeline conflict resolution

### 🔍 Data Quality (`test_data_quality_e2e.py`)
Tests data quality and schema evolution scenarios:
- Schema drift detection and handling
- Data type coercion and validation edge cases
- Corrupt and malformed data handling
- Complex nested data validation
- Schema compatibility modes (backward/forward)
- Data lineage tracking through transformations

### ⚡ Performance (`test_performance_e2e.py`)
Tests performance and scalability edge cases:
- Large dataset processing with memory constraints
- Backpressure handling when destination is slower than source
- Concurrent stream processing with resource sharing
- Memory pressure adaptation
- Throughput optimization under various conditions
- Degraded performance handling and recovery

### 🌐 Network Resilience (`test_network_resilience_e2e.py`)
Tests network-related edge cases:
- Connection failure recovery (DNS, SSL, timeouts)
- Network partition handling with offline mode
- SSL/TLS certificate issues and rotation
- DNS resolution failures with fallback mechanisms
- Bandwidth limitations and rate limiting
- Intermittent connectivity patterns with predictive disconnection

### 💾 State Management (`test_state_management_e2e.py`)
Tests state management and recovery scenarios:
- Corrupted checkpoint recovery with various corruption types
- Concurrent state access with file locking
- Incremental sync edge cases (duplicates, out-of-order, gaps)
- State migration between different versions
- Checkpoint frequency optimization based on performance

### 🌪️ Comprehensive Edge Cases (`test_comprehensive_edge_cases_e2e.py`)
Tests comprehensive scenarios combining multiple failure modes:
- Cascading failure scenarios across multiple components
- Extreme data volume processing (millions of records)
- Mixed failure mode recovery (network + data + memory issues)
- Resource exhaustion with graceful degradation
- End-to-end chaos engineering with fault injection
- Long-running pipeline stability and resource management

### 🔗 Basic E2E Tests
Basic connector combination tests:
- `api_to_api/`: API source to API destination
- `api_to_db/`: API source to database destination
- `db_to_api/`: Database source to API destination
- `db_to_db/`: Database source to database destination

## Running Tests

### Prerequisites
```bash
# Install test dependencies
pip install pytest pytest-asyncio pytest-timeout

# Or using poetry
poetry install --with test
```

### Quick Start
```bash
# Run all E2E tests
python run_e2e_tests.py --all

# Run specific category
python run_e2e_tests.py --category fault_tolerance

# Run with verbose output
python run_e2e_tests.py --category performance --verbose

# List available categories
python run_e2e_tests.py --list
```

### Advanced Usage
```bash
# Run all except slow performance tests
python run_e2e_tests.py --all --exclude performance

# Run specific test file
python run_e2e_tests.py --test test_fault_tolerance_e2e.py

# Run tests matching pattern
python run_e2e_tests.py --test "circuit_breaker"

# Generate detailed report
python run_e2e_tests.py --all --report /path/to/report.json
```

### Using pytest directly
```bash
# Run all E2E tests
pytest

# Run specific category using markers
pytest -m fault_tolerance
pytest -m "performance and not slow"

# Run with specific output
pytest --tb=long --verbose -s

# Run specific test
pytest test_fault_tolerance_e2e.py::TestFaultToleranceE2E::test_circuit_breaker_activation
```

## Test Structure

### Fixtures (`conftest.py`)
- `temp_dirs`: Temporary directories for pipeline artifacts
- `mock_pipeline_id`: Unique pipeline ID for test isolation
- `data_generator`: Generate various types of test data
- `mock_connector_factory`: Create mock connectors with failure patterns
- `fault_tolerance_config`: Standard fault tolerance configuration

### Mock Components
- **DataGenerator**: Generates valid, malformed, and large datasets
- **MockConnectorFactory**: Creates failing connectors with various patterns
- **FaultInjectionConnector**: Simulates specific failure scenarios
- **NetworkResilientConnector**: Simulates network resilience scenarios

### Test Patterns
Each test follows a consistent pattern:

1. **Setup**: Configure pipeline with specific edge case settings
2. **Mock**: Create mock connectors simulating failure conditions
3. **Execute**: Run pipeline with mocked StreamingEngine
4. **Verify**: Assert metrics and behavior match expected edge case handling

## Edge Cases Covered

### Data Streaming Edge Cases
- **Volume**: Processing millions of records with memory constraints
- **Velocity**: High-throughput scenarios with backpressure
- **Variety**: Mixed data types, malformed records, schema evolution
- **Veracity**: Data quality issues, validation failures, corrupt records

### System Resilience Edge Cases
- **Network**: Partitions, DNS failures, SSL issues, bandwidth limits
- **Memory**: Pressure, leaks, exhaustion, garbage collection
- **Storage**: Disk space, I/O limits, checkpoint corruption
- **Concurrency**: Race conditions, deadlocks, resource contention

### Fault Tolerance Edge Cases
- **Failures**: Cascading, partial, intermittent, permanent
- **Recovery**: Circuit breakers, retries, fallbacks, graceful degradation
- **State**: Corruption, conflicts, migration, consistency
- **Performance**: Degradation, optimization, scaling, bottlenecks

## Metrics and Assertions

Tests verify comprehensive metrics including:
- **Processing**: Records processed/failed, throughput, latency
- **Reliability**: Failure rates, recovery success, availability
- **Performance**: Resource usage, optimization effectiveness
- **Quality**: Data accuracy, schema compliance, validation success

## Configuration

### Pytest Configuration (`pytest.ini`)
- Test discovery patterns
- Marker definitions for categorization
- Asyncio support for async tests
- Timeout configuration (5 minutes default)
- Warning filters

### Test Runner Configuration (`run_e2e_tests.py`)
- Category-based test organization
- Flexible execution modes
- Comprehensive reporting
- Integration with CI/CD pipelines

## Best Practices

### Writing Edge Case Tests
1. **Isolate Failure Modes**: Test one primary failure mode per test
2. **Realistic Scenarios**: Base tests on real-world failure patterns
3. **Comprehensive Coverage**: Include setup, execution, and cleanup failures
4. **Deterministic Behavior**: Use controlled randomness with seeds
5. **Clear Assertions**: Verify both positive and negative outcomes

### Mock Design
1. **Realistic Failures**: Mirror actual system failure modes
2. **Configurable Patterns**: Allow tuning failure rates and types
3. **State Tracking**: Record operations for verification
4. **Resource Management**: Proper cleanup and resource release

### Performance Considerations
1. **Timeout Configuration**: Set appropriate timeouts for long-running tests
2. **Resource Limits**: Prevent tests from consuming excessive resources
3. **Parallel Execution**: Design tests for safe concurrent execution
4. **Cleanup**: Ensure proper cleanup of temporary resources

## Troubleshooting

### Common Issues
1. **Test Timeouts**: Increase timeout or optimize test data size
2. **Resource Conflicts**: Ensure unique pipeline IDs and temp directories
3. **Mock Failures**: Verify mock configuration matches test expectations
4. **Async Issues**: Ensure proper async/await usage in test code

### Debugging Tips
1. **Verbose Mode**: Use `--verbose` flag for detailed output
2. **Single Test**: Run individual tests for focused debugging
3. **Log Analysis**: Check pipeline logs in temp directories
4. **Mock Inspection**: Examine mock operation history

## Contributing

When adding new edge case tests:

1. **Identify Gap**: Determine what edge case is not covered
2. **Design Scenario**: Create realistic failure scenario
3. **Implement Test**: Follow existing patterns and conventions
4. **Add Documentation**: Update this README with new test description
5. **Categorize**: Add appropriate pytest markers
6. **Verify**: Ensure test passes and fails appropriately

## Integration with CI/CD

The test runner supports CI/CD integration:
- Exit codes indicate test success/failure
- JUnit XML reports for test result parsing
- JSON reports for detailed analysis
- Category-based execution for pipeline stages
- Exclusion of slow tests for quick feedback

Example CI configuration:
```yaml
# Quick feedback (exclude slow tests)
- name: Quick E2E Tests
  run: python tests/e2e/run_e2e_tests.py --all --exclude performance

# Full test suite (nightly)
- name: Full E2E Tests
  run: python tests/e2e/run_e2e_tests.py --all --report results.json
```