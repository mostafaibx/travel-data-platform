# GitHub Actions Workflows for Travel Data Platform

This directory contains GitHub Actions workflows for automating Continuous Integration (CI) processes for the Travel Data Platform.

## Workflow: Data Pipeline CI (`data-pipeline-ci.yml`)

This workflow runs on pushes to `main` and `develop` branches as well as pull requests to these branches, specifically for changes to the `pipelines/` directory or `pyproject.toml`. It can also be triggered manually via the GitHub Actions tab.

### Jobs

1. **Lint**
   - Performs code quality checks using:
     - `black` for code formatting
     - `isort` for import sorting
     - `flake8` for PEP8 compliance and other code quality issues

2. **Test**
   - Runs unit and integration tests with pytest
   - Generates and uploads test reports in JUnit XML format

3. **Data Validation**
   - Verifies data schema consistency across pipelines
   - Uses Great Expectations for data schema validation
   - Ensures data quality standards are maintained

4. **Dry-run**
   - Sets up a mock environment for pipeline testing
   - Executes pipelines in dry-run mode to verify logic without external dependencies
   - Validates expected pipeline outputs

5. **Code Security**
   - Performs security scans using Bandit to detect common security issues
   - Checks dependencies for known vulnerabilities using Safety
   - Uploads security scan results as artifacts

## Best Practices Implemented

- **Isolated Testing**: Each pipeline is tested independently in controlled environments
- **No External Dependencies**: Dry runs avoid external API calls or production data access
- **Code Quality Gates**: Multiple validation steps ensure high-quality code
- **Security First**: Automated security scanning prevents common vulnerabilities
- **Data Quality Validation**: Schema and data quality checks prevent data-related issues
- **Artifact Preservation**: Test results and security reports are saved for review

## Getting Started

To use this workflow, ensure your project has:

1. Unit tests in appropriate locations within the pipelines directory
2. Dev dependencies specified in the `pyproject.toml` file
3. Properly configured linting tools (black, isort, flake8)

## Manual Triggers

The workflow supports manual triggering using the `workflow_dispatch` event, which can be initiated from the GitHub Actions tab in the repository. 