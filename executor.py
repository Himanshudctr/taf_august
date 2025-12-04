import pytest
import sys


def main():
    # Customize the arguments for pytest run
    pytest_args = [
        "tests/table1",
        "tests/table2",
        "-v",
        "-s",
        "-m", "regression",
        "-k", "count"  # Run tests marked with regression

    ]

    # Exit with pytest's exit code
    sys.exit(pytest.main(pytest_args))


# updated in local

if __name__ == "__main__":
    main()
