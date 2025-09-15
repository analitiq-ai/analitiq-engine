Create comprehensive tests for: $ARGUMENTS

Make sure that test architectures that are:
- **Hermetic**: Unit tests isolated from external dependencies
- **Discoverable**: Following pytest conventions for easy test discovery
- **Maintainable**: Well-organized with clear separation of concerns
- **Comprehensive**: Covering edge cases, error paths, and happy paths
- **Fast**: Optimized test execution with appropriate use of markers
- Aim for high code coverage
- Test all major functionality
- Include edge cases and error scenarios

## Testing Architecture Standards

### File Organization
- Name test files `test_*.py` and test functions `test_*`
- Mirror package structure under `tests/unit/your_pkg/` for easy navigation
- Avoid `__init__.py` in tests/ unless specifically needed as a package
- Use src layout (`src/your_pkg`) and import as `from your_pkg.module_a import foo`
- Store test data files under `tests/data/` and access via `pathlib.Path(__file__).parent / "data" / "file.json"`

### Test Categories & Markers
- **Unit tests**: `@pytest.mark.unit` - Fast, hermetic, no I/O
- **Integration tests**: `@pytest.mark.integration` - Test component interactions
- **E2E tests**: `@pytest.mark.e2e` - Full system workflows
- **Slow tests**: `@pytest.mark.slow` - Long-running tests

### Fixtures & Test Data
- Place shared fixtures in `tests/conftest.py` (project-wide) or per-folder `conftest.py`
- Complex fixtures can live in `tests/fixtures/` and be imported into conftest.py
- Prefer factories/builders over raw literals for test data
- Use parametrization over loops for testing multiple scenarios

### Test Behavior Guidelines
- Keep unit tests hermetic: mock I/O, time, network, and environment dependencies
- One behavior per test with descriptive names: `test_<behavior>_<condition>`
- Avoid test interdependence; each test creates its own isolated world
- Test both happy paths and error conditions explicitly
- Use ExceptionGroup and except* for Python 3.11+ error handling patterns

## Implementation Approach

When creating tests, you will:

1. **Analyze the code structure** to understand dependencies and interaction patterns
2. **Design test architecture** with appropriate separation of unit/integration/e2e concerns
3. **Create comprehensive fixtures** using factories for maintainable test data
4. **Implement hermetic unit tests** with proper mocking of external dependencies
5. **Build integration tests** that verify component interactions
6. **Design e2e tests** for critical user workflows
7. **Apply appropriate markers** for test categorization and execution control
8. **Ensure test discoverability** through proper naming and organization

Adhere to the following directory tree:
project/
├─ src/                         # if you use the "src layout"
│  └─ your_pkg/
│     ├─ __init__.py
│     └─ ... code ...
├─ tests/
│  ├─ unit/                     # fast, pure-Python tests (no I/O/network/db)
│  │  └─ your_pkg/
│  │     ├─ test_module_a.py
│  │     └─ test_module_b.py
│  ├─ integration/              # touches db, filesystem, external services
│  │  ├─ test_db_*.py
│  │  └─ test_api_*.py
│  ├─ e2e/                      # high-level flows, slow, minimal count
│  │  └─ test_user_journey.py
│  ├─ factories/                # test data builders (factory_boy, etc.)
│  │  └─ user_factory.py
│  ├─ fixtures/                 # reusable pytest fixtures
│  │  ├─ db.py
│  │  └─ http.py
│  ├─ helpers/                  # assertion helpers, custom matchers
│  │  └─ asserts.py
│  ├─ data/                     # static files used by tests
│  │  └─ sample_payload.json
│  ├─ conftest.py               # session-/module-scoped fixtures & hooks
│  └─ __init__.py               # usually OMIT this
├─ pyproject.toml               # pytest/coverage config lives here
└─ ...
