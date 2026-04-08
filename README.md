# CRM Cleanup Tool

A web-based tool for cleaning Salesforce account names and deduplicating accounts via the Tiga API. It provides a browser UI for scanning, reviewing, and applying changes to your CRM data.

## Features

- **Name Cleaning** - Scans Salesforce accounts and suggests standardized name corrections. Review changes individually, apply selected fixes, and undo if needed.
- **Deduplication** - Identifies duplicate account groups using fuzzy matching (name similarity, website, phone, billing address). Choose which record to keep in each group and mark the rest as duplicates.
- **Operations Log** - All changes are logged to `crm_ops_log.csv` for auditability.

## Prerequisites

### Install Go

This project requires **Go 1.21** or later.

**macOS (Homebrew):**

```bash
brew install go
```

**macOS (installer):**

Download the `.pkg` installer from [https://go.dev/dl/](https://go.dev/dl/) and run it. It installs to `/usr/local/go` and adds it to your PATH automatically.

**Linux:**

```bash
# Download (replace version as needed)
wget https://go.dev/dl/go1.21.13.linux-amd64.tar.gz

# Remove any previous installation and extract
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.21.13.linux-amd64.tar.gz

# Add to PATH (add this to ~/.bashrc or ~/.zshrc to persist)
export PATH=$PATH:/usr/local/go/bin
```

**Windows:**

Download the `.msi` installer from [https://go.dev/dl/](https://go.dev/dl/) and run it. It installs to `C:\Go` and updates your PATH.

**Verify installation:**

```bash
go version
```

### Tiga API Key

You need a valid Tiga API key with access to your Salesforce instance. You'll enter this in the browser UI when you start the tool.

## Running the Tool

```bash
go run main.go
```

The server starts on **http://localhost:8082**. Open that URL in your browser.

## Usage

1. Enter your **Tiga API Key** and optionally the **Tiga Base URL** in the Connection Settings, then click **Connect**.
2. Switch between the **Name Cleaning** and **Deduplication** tabs.

### Name Cleaning

- Click **Scan Accounts** to fetch and analyze all account names.
- Review the suggested changes in the table. Deselect any you don't want.
- Click **Apply Selected** to push changes to Salesforce.
- Click **Undo** to revert the last batch of applied changes.

### Deduplication

- Click **Scan for Duplicates** to identify duplicate account groups.
- For each group, select which account to **keep** (the rest will be marked as duplicates).
- Click **Mark Duplicates** to apply.

## Configuration

| Constant | Default | Description |
|---|---|---|
| `sfAPIVersion` | `v59.0` | Salesforce API version |
| `scanWorkers` | `5` | Concurrent workers for scanning |
| `sfBatchSize` | `25` | Records per Salesforce API batch |
| `dupScoreThreshold` | `40` | Minimum similarity score to flag as duplicate |

These are defined in `main.go` and can be adjusted before running.

## License

MIT - See [license.txt](license.txt) for details.
