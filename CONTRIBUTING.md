# Contributing

## Python Coding Style

Use these rules for new or modified Python code in this repository:

- Prefer readability-first code over compact or clever patterns.
- Keep functions small and focused on one job.
- Put constants and configuration defaults in a clear section near the top of the module.
- Validate required environment variables and config values explicitly with actionable error messages.
- Keep pure helper functions deterministic when possible (same input, same output).
- Avoid hidden side effects at import time; put runtime behavior behind `main()` and `if __name__ == "__main__":`.
- Add short, plain-language docstrings to non-trivial functions.
- Preserve existing behavior and business logic unless a change is explicitly requested.

## Tests

- Add lightweight unit tests for pure logic/helpers in changed scripts.
- Keep unit tests independent from external infrastructure (Kafka, Docker, Airflow, Snowflake, Superset).
