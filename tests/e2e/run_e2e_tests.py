"""E2E Test Runner for Comprehensive Edge Case Testing

This script runs all e2e tests with proper reporting and categorization.
"""

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import pytest


class E2ETestRunner:
    """Comprehensive E2E test runner with categorization and reporting."""

    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.test_categories = {
            "fault_tolerance": {
                "description": "Fault tolerance and resilience tests",
                "files": ["test_fault_tolerance_e2e.py"],
                "markers": ["fault_tolerance", "resilience"]
            },
            "data_quality": {
                "description": "Data quality and schema evolution tests",
                "files": ["test_data_quality_e2e.py"],
                "markers": ["data_quality", "schema"]
            },
            "performance": {
                "description": "Performance and scalability tests",
                "files": ["test_performance_e2e.py"],
                "markers": ["performance", "scalability", "slow"]
            },
            "network_resilience": {
                "description": "Network resilience and connectivity tests",
                "files": ["test_network_resilience_e2e.py"],
                "markers": ["network", "connectivity"]
            },
            "state_management": {
                "description": "State management and recovery tests",
                "files": ["test_state_management_e2e.py"],
                "markers": ["state", "recovery"]
            },
            "comprehensive": {
                "description": "Comprehensive edge case scenarios",
                "files": ["test_comprehensive_edge_cases_e2e.py"],
                "markers": ["comprehensive", "chaos", "stress"]
            },
            "basic_e2e": {
                "description": "Basic end-to-end connector tests",
                "files": [
                    "api_to_api/test_api_to_api_e2e.py",
                    "api_to_db/test_api_to_db_e2e.py",
                    "db_to_api/test_db_to_api_e2e.py",
                    "db_to_db/test_db_to_db_e2e.py"
                ],
                "markers": ["basic_e2e"]
            }
        }

    def run_category(self, category: str, verbose: bool = False) -> Dict:
        """Run tests for a specific category."""
        if category not in self.test_categories:
            raise ValueError(f"Unknown category: {category}")

        category_info = self.test_categories[category]
        print(f"\n🧪 Running {category} tests: {category_info['description']}")
        print("=" * 80)

        test_files = []
        for file_pattern in category_info["files"]:
            if "/" in file_pattern:
                # Subdirectory file
                test_path = self.base_dir / file_pattern
            else:
                # Direct file
                test_path = self.base_dir / file_pattern

            if test_path.exists():
                test_files.append(str(test_path))
            else:
                print(f"⚠️  Warning: Test file not found: {test_path}")

        if not test_files:
            return {"category": category, "status": "no_tests", "duration": 0}

        # Prepare pytest arguments
        pytest_args = test_files.copy()

        if verbose:
            pytest_args.extend(["-v", "-s"])

        # Skip marker filtering - run all tests in the specified files
        # if category_info.get("markers"):
        #     marker_expr = " or ".join(category_info["markers"])
        #     pytest_args.extend(["-m", marker_expr])

        # Add output options
        pytest_args.extend([
            "--tb=short",
            "--disable-warnings",
            f"--junit-xml={self.base_dir}/reports/{category}_results.xml"
        ])

        # Ensure reports directory exists
        reports_dir = self.base_dir / "reports"
        reports_dir.mkdir(exist_ok=True)

        start_time = time.time()

        try:
            # Run pytest programmatically
            exit_code = pytest.main(pytest_args)
            duration = time.time() - start_time

            status = "passed" if exit_code == 0 else "failed"
            print(f"\n✅ {category} tests {status} in {duration:.2f}s")

            return {
                "category": category,
                "status": status,
                "exit_code": exit_code,
                "duration": duration,
                "test_files": test_files
            }

        except Exception as e:
            duration = time.time() - start_time
            print(f"\n❌ {category} tests failed with exception: {e}")

            return {
                "category": category,
                "status": "error",
                "error": str(e),
                "duration": duration,
                "test_files": test_files
            }

    def run_all_categories(self, exclude_categories: Optional[List[str]] = None, verbose: bool = False) -> Dict:
        """Run all test categories."""
        exclude_categories = exclude_categories or []
        results = {"categories": [], "summary": {}}

        print("🚀 Starting Comprehensive E2E Test Suite")
        print("=" * 80)

        total_start_time = time.time()

        for category in self.test_categories.keys():
            if category in exclude_categories:
                print(f"⏭️  Skipping {category} (excluded)")
                continue

            result = self.run_category(category, verbose)
            results["categories"].append(result)

        total_duration = time.time() - total_start_time

        # Generate summary
        passed = len([r for r in results["categories"] if r["status"] == "passed"])
        failed = len([r for r in results["categories"] if r["status"] in ["failed", "error"]])
        total_categories = len(results["categories"])

        results["summary"] = {
            "total_categories": total_categories,
            "passed": passed,
            "failed": failed,
            "success_rate": passed / total_categories if total_categories > 0 else 0,
            "total_duration": total_duration
        }

        # Print summary
        print("\n" + "=" * 80)
        print("📊 Test Suite Summary")
        print("=" * 80)
        print(f"Total Categories: {total_categories}")
        print(f"Passed: {passed}")
        print(f"Failed: {failed}")
        print(f"Success Rate: {results['summary']['success_rate']:.1%}")
        print(f"Total Duration: {total_duration:.2f}s")

        # Print detailed results
        print("\n📋 Detailed Results:")
        for result in results["categories"]:
            status_emoji = "✅" if result["status"] == "passed" else "❌"
            print(f"  {status_emoji} {result['category']}: {result['status']} ({result['duration']:.2f}s)")

        return results

    def run_specific_tests(self, test_patterns: List[str], verbose: bool = False) -> Dict:
        """Run specific tests matching the given patterns."""
        print(f"🎯 Running specific tests: {', '.join(test_patterns)}")
        print("=" * 80)

        pytest_args = []

        # Add test patterns
        for pattern in test_patterns:
            if pattern.startswith("test_"):
                # Specific test file
                test_path = self.base_dir / pattern
                if test_path.exists():
                    pytest_args.append(str(test_path))
                else:
                    print(f"⚠️  Warning: Test file not found: {test_path}")
            else:
                # Pattern or keyword
                pytest_args.extend(["-k", pattern])

        if not pytest_args:
            return {"status": "no_tests", "duration": 0}

        if verbose:
            pytest_args.extend(["-v", "-s"])

        pytest_args.extend([
            "--tb=short",
            "--disable-warnings",
            f"--junit-xml={self.base_dir}/reports/specific_results.xml"
        ])

        # Ensure reports directory exists
        reports_dir = self.base_dir / "reports"
        reports_dir.mkdir(exist_ok=True)

        start_time = time.time()

        try:
            exit_code = pytest.main(pytest_args)
            duration = time.time() - start_time

            status = "passed" if exit_code == 0 else "failed"
            print(f"\n✅ Specific tests {status} in {duration:.2f}s")

            return {
                "status": status,
                "exit_code": exit_code,
                "duration": duration,
                "patterns": test_patterns
            }

        except Exception as e:
            duration = time.time() - start_time
            print(f"\n❌ Specific tests failed with exception: {e}")

            return {
                "status": "error",
                "error": str(e),
                "duration": duration,
                "patterns": test_patterns
            }

    def generate_report(self, results: Dict, output_file: Optional[Path] = None):
        """Generate a comprehensive test report."""
        if output_file is None:
            output_file = self.base_dir / "reports" / "e2e_test_report.json"

        output_file.parent.mkdir(exist_ok=True)

        # Add metadata
        report = {
            "timestamp": time.time(),
            "test_suite": "Analitiq Stream E2E Tests",
            "framework_version": "1.0",
            "results": results
        }

        with open(output_file, "w") as f:
            json.dump(report, f, indent=2)

        print(f"\n📄 Test report generated: {output_file}")

    def list_categories(self):
        """List all available test categories."""
        print("📂 Available Test Categories:")
        print("=" * 50)

        for category, info in self.test_categories.items():
            print(f"\n🏷️  {category}")
            print(f"   Description: {info['description']}")
            print(f"   Files: {len(info['files'])}")
            if info.get("markers"):
                print(f"   Markers: {', '.join(info['markers'])}")


def main():
    """Main entry point for the E2E test runner."""
    parser = argparse.ArgumentParser(
        description="Run comprehensive E2E tests for Analitiq Stream",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python run_e2e_tests.py --all                    # Run all test categories
  python run_e2e_tests.py --category fault_tolerance  # Run specific category
  python run_e2e_tests.py --test test_fault_tolerance_e2e.py  # Run specific test
  python run_e2e_tests.py --list                   # List available categories
  python run_e2e_tests.py --all --exclude performance  # Run all except performance
        """
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all test categories"
    )

    parser.add_argument(
        "--category",
        type=str,
        help="Run specific test category"
    )

    parser.add_argument(
        "--test",
        type=str,
        action="append",
        help="Run specific test (can be used multiple times)"
    )

    parser.add_argument(
        "--exclude",
        type=str,
        action="append",
        help="Exclude specific categories when running --all"
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List available test categories"
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output"
    )

    parser.add_argument(
        "--report",
        type=str,
        help="Generate report to specified file"
    )

    args = parser.parse_args()

    # Determine base directory
    base_dir = Path(__file__).parent

    # Create test runner
    runner = E2ETestRunner(base_dir)

    if args.list:
        runner.list_categories()
        return

    results = None

    if args.all:
        results = runner.run_all_categories(
            exclude_categories=args.exclude,
            verbose=args.verbose
        )
    elif args.category:
        result = runner.run_category(args.category, args.verbose)
        results = {"categories": [result]}
    elif args.test:
        result = runner.run_specific_tests(args.test, args.verbose)
        results = {"specific": result}
    else:
        print("❌ Please specify --all, --category, --test, or --list")
        parser.print_help()
        sys.exit(1)

    # Generate report if requested
    if args.report and results:
        report_path = Path(args.report)
        runner.generate_report(results, report_path)

    # Exit with appropriate code
    if results:
        if "categories" in results:
            failed_categories = [r for r in results["categories"] if r["status"] in ["failed", "error"]]
            sys.exit(len(failed_categories))
        elif "specific" in results and results["specific"]["status"] != "passed":
            sys.exit(1)


if __name__ == "__main__":
    main()