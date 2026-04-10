# Contributing

## Setup

```bash
poetry install
poetry shell
```

## Testing

```bash
poetry run pytest                           # all tests
poetry run pytest -m "unit"                 # unit tests only
poetry run pytest --cov=src --cov-report=html
```

## Code Quality

```bash
poetry run black src/ && poetry run isort src/
poetry run mypy src/
poetry run flake8 src/
```

## Coding Guidelines

- Python 3.11+ with Pydantic V2 validation
- No backward compatibility code
- No emojis in code
- Modular, pythonic code with explicit error paths
- Deterministic behavior, idempotent writes, safe retries
- Update README.md when modifying documented functionality
- Do not add legacy support or backward compatibility unless explicitly instructed

## Working on Issues

1. Examine GitHub issues, pick the easiest one to implement.
2. Create a new branch for the issue.
3. Implement the issue.
4. Commit your changes and push to the branch.
5. Create a pull request (if not yet created).
6. Run the PR Review Process.
7. Wait for feedback from the review executor.

## PR Review Process

1. Use `/pr-review-toolkit` to review the PR after you have implemented all changes.
2. Wait for feedback from the review executor.
3. Determine if the raised issues are legitimate or not.
   a. If the issue is legitimate and relevant to the PR, fix it.
   b. If the issue is outside the scope of the PR, check if there is a related issue in the GitHub issue tracker. If not, create a new issue in GitHub and move on.
   c. If the issue is not a legitimate problem, summarize your thoughts on the point and move on.
4. Once you fixed all issues that need fixing, commit fixes, push to the branch.
5. Use `/pr-review-toolkit` to review again.
6. Continue doing this cycle until the PR is approved by the review executor.
7. Once the PR is approved, run the tests to make sure they all pass.