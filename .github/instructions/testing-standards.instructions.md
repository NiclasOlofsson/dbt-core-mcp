---
applyTo: '**/*.{py,html}'
description: Testing standards and patterns for dbt-core-mcp tools
---

# dbt-core-mcp Testing Standards

## Testing Architecture (2-Layer Strategy)

### Layer 1: Tool-Specific Comprehensive Tests
- **Purpose**: Command construction, parameter validation, tool-specific business logic
- **Speed Target**: < 10 seconds per tool
- **Pattern**: Mock external dependencies, use real fixtures for parsing validation
- **Coverage**: All command variations, success/failure scenarios, edge cases
- **File**: `test_tool_{tool_name}.py`

### Layer 2: Cross-Tool Output Parsing Tests  
- **Purpose**: Validate parsing of dbt output structures across different scenarios
- **Speed Target**: < 5 seconds total
- **Pattern**: Real + synthetic fixtures for comprehensive edge case coverage
- **Coverage**: Success, failure, mixed, empty, skipped results across all tools
- **File**: `test_output_parsing.py` (shared validation logic)

## Performance Requirements

**MANDATORY**: All tool test suites must achieve **< 10 seconds** execution time

**Target**: Fast, comprehensive testing approach
- ✅ **Goal**: 3-7 seconds with mock-based + fixture approach

## File Organization Standards

### Test File Structure
```
tests/
├── test_tool_{tool_name}.py          # Tool-specific: command + parsing + business logic
├── test_output_parsing.py            # Shared: cross-tool output parsing validation
└── fixtures/
    └── target/
        └── run_results.json          # Real dbt output for parsing tests
```

### Consolidated + Shared Pattern (REQUIRED)
- ✅ **Tool-Specific Tests**: `test_tool_{name}.py` with complete tool coverage
- ✅ **Shared Output Parsing**: `test_output_parsing.py` for cross-tool validation
- **Sections in Tool Tests**: Command Construction → Real Fixture Parsing → Business Logic
- **Shared Tests**: Edge cases (failed, mixed, empty, skipped) + structure validation

## Type Safety Standards

### Required Type Annotations
```python
from typing import Any, Callable, Dict, Optional
from unittest.mock import Mock

def test_function(mock_state: Mock) -> None:
    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        # Implementation
```

### Fixture Type Safety
```python
@pytest.fixture
def real_run_results(fixtures_dir: Path) -> Dict[str, Any]:
    with open(fixtures_dir / "target" / "run_results.json") as f:
        return json.load(f)
```

**CRITICAL**: Never exclude tests from pyright type checking - fix annotations properly

## Test Categories and Implementation Pattern

### 1. Tool-Specific Tests (`test_tool_{name}.py`)

#### Command Construction Tests (Mock-Based)
```python
@pytest.mark.asyncio
async def test_command_construction_basic(mock_state: Mock) -> None:
    """Test basic command construction without execution."""
    commands_run = []
    
    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        return result
    
    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke
    
    # Call tool implementation directly
    await tool_impl(ctx=None, select="customers", state=mock_state)
    
    # Assert command structure
    assert len(commands_run) == 1
    args = commands_run[0]
    assert args[0] == "expected_command"  # e.g., "build", "run", "test"
    assert "--select" in args
    assert "customers" in args
```

#### Output Parsing with Real Fixtures
```python
def test_successful_parsing(mock_state_with_real_parsing: Mock) -> None:
    """Test parsing successful dbt output using real fixture."""
    mock_result = Mock()
    mock_result.success = True
    
    # Parse using real fixture data
    parsed = mock_state_with_real_parsing.validate_and_parse_results(mock_result, "command_name")
    
    # Validate structure
    assert "results" in parsed
    assert len(parsed["results"]) > 0
    assert "elapsed_time" in parsed
```

### 2. Shared Output Parsing Tests (`test_output_parsing.py`)
```python
@pytest.mark.asyncio
async def test_command_construction_basic(mock_state: Mock) -> None:
    """Test basic command construction without execution."""
    commands_run = []
    
    async def capture_invoke(args: Dict[str, Any], progress_callback: Optional[Callable[..., Any]] = None) -> Mock:
        commands_run.append(args)
        result = Mock()
        result.success = True
        return result
    
    mock_runner = await mock_state.get_runner()
    mock_runner.invoke.side_effect = capture_invoke
    
    # Call tool implementation
    # Assert command structure
```

### 2. Shared Output Parsing Tests (`test_output_parsing.py`)

#### Comprehensive Scenario Coverage
```python
def test_successful_results_parsing(real_run_results: Dict[str, Any]) -> None:
    """Test parsing successful dbt output from real fixture."""
    results_list = real_run_results.get("results", [])
    
    # Assert structure
    assert len(results_list) > 0
    assert "elapsed_time" in real_run_results
    
    # Test counting logic
    passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
    failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
    
    assert passed_count == len(results_list)  # All successful in real fixture
    assert failed_count == 0

def test_failed_results_parsing(failed_run_results: Dict[str, Any]) -> None:
    """Test parsing failed dbt build output."""
    results_list = failed_run_results.get("results", [])
    
    passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
    failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
    
    assert failed_count > 0
    assert passed_count == 0
    
    # Test message construction for failures
    parts = []
    if failed_count > 0:
        parts.append(f"❌ {failed_count} failed")
    
    summary = f"Command: {len(results_list)}/{len(results_list)} resources completed ({', '.join(parts)})"
    assert "❌" in summary
```

**Required Scenarios in Shared Tests**:
```python
def test_successful_results_parsing(real_run_results: Dict[str, Any]) -> None:
    """Test parsing successful dbt output from real fixture."""
    results_list = real_run_results.get("results", [])
    
    # Assert structure
    assert len(results_list) > 0
    assert "elapsed_time" in real_run_results
    
    # Test counting logic
    passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
    failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
```

**Required Scenarios in Shared Tests**:
- ✅ Successful builds (real fixture)
- ❌ Failed builds (synthetic fixture)
- 🔀 Mixed results (synthetic fixture) 
- ⏭️ Empty results (synthetic fixture)
- ⏭️ Skipped results (synthetic fixture)
- 📊 Status counting accuracy across all scenarios
- 🏗️ Message construction validation with emojis
- 🔧 Resource type extraction (`model`, `test`, `seed`, `unit_test`, etc.)
- 📋 Result structure validation (required fields, data types)

**Command Construction Test Cases Required (Per Tool)**:
- Basic parameters (no selection)
- Select/exclude combinations  
- State-based selections (`state:modified`, `state:modified+`)
- All flags enabled together
- Parameter validation (invalid combinations)
- Error conditions and edge cases

### 3. Synthetic Fixtures for Edge Cases
```python
@pytest.fixture
def failed_run_results() -> Dict[str, Any]:
    """Create synthetic failed run results for testing."""
    return {
        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json"},
        "results": [
            {
                "status": "error",
                "unique_id": "model.jaffle_shop.customers",
                "message": "Compilation Error in model 'customers'",
                "failures": ["compilation error"],
                # ... other required fields
            }
        ],
        "elapsed_time": 0.8
    }
```

## Mocking Standards

### State Mock Pattern (REQUIRED)
```python
@pytest.fixture
def mock_state() -> Mock:
    """Create a mock server state for testing."""
    state = Mock(spec=DbtCoreServerContext)
    state.ensure_initialized = AsyncMock()
    state.prepare_state_based_selection = AsyncMock(return_value=None)
    state.clear_stale_run_results = Mock()
    state.validate_and_parse_results = Mock(return_value={"results": []})
    state.save_execution_state = AsyncMock()
    
    # Mock runner
    mock_runner = Mock()
    mock_result = Mock()
    mock_result.success = True
    mock_runner.invoke = AsyncMock(return_value=mock_result)
    state.get_runner = AsyncMock(return_value=mock_runner)
    
    return state
```

### Real Parsing Integration Pattern
```python
@pytest.fixture
def mock_state_with_real_parsing(real_run_results: Dict[str, Any]) -> Mock:
    """Mock state that uses real fixture data for parsing validation."""
    mock_state = Mock(spec=DbtCoreServerContext)
    mock_state.parse_run_results.return_value = real_run_results
    
    def validate_and_parse_results(result: Any, command_name: str) -> Dict[str, Any]:
        run_results = mock_state.parse_run_results()
        if not run_results.get("results"):
            if result and not result.success:
                raise RuntimeError(f"dbt {command_name} failed to execute")
        return run_results
    
    mock_state.validate_and_parse_results = validate_and_parse_results
    return mock_state
```

## Assertion Patterns

### Status Counting Validation
```python
# Test business logic for all scenarios
passed_count = sum(1 for r in results_list if r.get("status") in ("success", "pass"))
failed_count = sum(1 for r in results_list if r.get("status") in ("error", "fail"))
skip_count = sum(1 for r in results_list if r.get("status") == "skipped")
total = len(results_list)

assert passed_count + failed_count + skip_count == total
```

### Message Construction Testing
```python
# Test emoji indicators and summary messages
parts = []
if passed_count > 0:
    parts.append(f"✅ {passed_count} passed" if failed_count > 0 or skip_count > 0 else "✅ All passed")
if failed_count > 0:
    parts.append(f"❌ {failed_count} failed")
if skip_count > 0:
    parts.append(f"⏭️ {skip_count} skipped")

summary = f"Build: {total}/{total} resources completed ({', '.join(parts)})"
```

### Structure Validation
```python
# Validate response structure matches expected format
assert "status" in response
assert "results" in response  
assert "elapsed_time" in response
assert "command" in response

# Validate business rules
if isinstance(results, list) and all(isinstance(r, dict) and r.get("status") in ("success", "pass") for r in results):
    assert response["status"] == "success"
```

## Pre-Commit Validation Protocol

**MANDATORY**: Before committing any test changes, run validation sequence:

```bash
# Step 1: Validation sequence (must ALL succeed)
uv run ruff check src tests
uv run ruff format --check src tests  
uv run pyright src tests
uv run pytest

# Step 2: Only proceed if ALL steps pass
# Step 3: Restore any .vscode/mcp.json changes (testing only)
git restore .vscode/mcp.json  # if modified during testing
```

## Migration Pattern for dbt Tools

### Apply to All dbt Tools
Apply this consolidated testing pattern to all dbt-core-mcp tools:
- All `run_*`, `test_*`, `build_*` commands
- Data management tools (`seed_*`, `snapshot_*`)
- Query and analysis tools (`query_*`, `get_*`, `list_*`, `analyze_*`)
- Project management tools (`install_*`, project info)
- Any future dbt-core-mcp tools

### Target Performance
- **Per Tool**: < 10 seconds per tool test file
- **Cross-Tool Parsing**: < 5 seconds total
- **All Tools Combined**: < 60s for complete test suite

## Anti-Patterns (AVOID)

❌ **Subprocess execution in unit tests** - Use mocks for speed  
❌ **Separate `_fast.py` files** - Consolidate into single test file  
❌ **Excluding tests from type checking** - Fix annotations properly  
❌ **Integration-only testing** - Layer approach for performance  
❌ **Missing edge case coverage** - Always test success/fail/mixed/empty  
❌ **Artificial test separation** - Keep related tests together  
❌ **CSV fixtures** - Use dict/JSON format for better maintainability  

## Success Metrics

### Performance Requirements
- ✅ **Individual tool tests**: < 10 seconds per file
- ✅ **Full test suite**: < 5 minutes 
- ✅ **Type checking**: 0 errors in tests
- ✅ **Coverage**: Command + parsing + integration layers

### Quality Indicators  
- ✅ **Real-world scenarios**: Failed builds, mixed results, empty selections
- ✅ **Type safety**: Full annotations, no exclusions
- ✅ **Maintainability**: Clear patterns, consolidated structure
- ✅ **CI compliance**: Passes ruff, pyright, pytest consistently

---

**REMEMBER**: Speed enables comprehensive testing. Fast tests = more scenarios covered = higher quality. The 2-layer approach achieves both performance and coverage goals.