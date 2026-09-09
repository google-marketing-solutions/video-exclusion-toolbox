# Video Exclusion Toolbox (VET) — Python 3.14 Environment & Development Standard

## 1. Runtime Standard Overview

All modernized Gen 2 Cloud Functions and local development environments are
standardized on **Python 3.14 (`python314`)**.

*   **Cloud Functions Gen 2 Runtime:** `python314` (declared in
    `terraform/resource_google_cloudfunctions2_function.tf`)
*   **Local Development Environment:** `pyenv` virtualenv named **`vet-3.14`**
    located at: `/usr/local/google/home/jakubmedved/.pyenv/versions/vet-3.14/`

--------------------------------------------------------------------------------

## 2. Dependency Hierarchy & Locking Standard

Dependencies are split into shared layers and service-specific requirements:

### A. Shared Common Dependencies (`src/common_requirements.txt`)

Included across all services via dynamic packaging in Terraform:

```text
functions-framework==3.10.2
google-auth==2.56.3
google-api-python-client==2.198.0
google-cloud-logging==3.16.2
google-cloud-pubsub==2.39.1
google-cloud-bigquery==3.43.0
jsonschema==4.26.0
```

### B. Google Ads Shared Dependencies (`src/google_ads_requirements.txt`)

Included for all Google Ads ingestion and excluder services:

```text
google-ads==31.3.0
```

### C. Testing & Local Tooling

Installed only in the local virtualenv (`vet-3.14`):

```text
pytest
pytest-mock
```

--------------------------------------------------------------------------------

## 3. Local Virtual Environment Setup (`pyenv`)

To configure or recreate the `vet-3.14` virtual environment on the workstation:

```bash
# 1. Update pyenv definitions
git -c url.https://github.com/.insteadOf= -C ~/.pyenv pull
git -c url.https://github.com/.insteadOf= -C ~/.pyenv/plugins/pyenv-virtualenv pull

# 2. Install Python 3.14
~/.pyenv/bin/pyenv install 3.14.0

# 3. Create the vet-3.14 virtualenv
~/.pyenv/bin/pyenv virtualenv 3.14.0 vet-3.14

# 4. Install locked project requirements
~/.pyenv/versions/vet-3.14/bin/pip install --upgrade pip
~/.pyenv/versions/vet-3.14/bin/pip install \
  -r src/common_requirements.txt \
  -r src/google_ads_requirements.txt \
  pytest pytest-mock
```

--------------------------------------------------------------------------------

## 4. Test Execution Protocol

Always execute unit tests using the `vet-3.14` Python binary:

```bash
# From the repository root or src/ directory:
/usr/local/google/home/jakubmedved/.pyenv/versions/vet-3.14/bin/pytest -v src/
```
