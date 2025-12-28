# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

DB4DD (Data Base for Digital Democracy) is an automated system that processes Japanese government meeting documents (PDFs) and generates AI-powered summaries in Markdown format. The system uses OpenAI's API to extract structured information, create summaries, and organize the data into an Obsidian vault structure.

The project focuses on two main government ministries:
- **デジタル庁** (Digital Agency) - 60+ meeting groups, 403+ files
- **こども家庭庁** (Children and Families Agency) - 40+ meeting groups, 310+ files

## Core Architecture

### Multi-Stage Processing Pipeline

The system uses a sophisticated multi-stage summarization approach:

1. **PDF Extraction** (`processing/pdf_processor.py`)
   - Extracts text from PDFs using PyMuPDF

2. **Text Chunking & Parallel Processing** (`processing/text_summarizer.py`)
   - Intelligently chunks large documents (adaptive chunk sizing based on document size)
   - Documents >100k tokens use special handling (truncation/smaller chunks)
   - Parallel extraction of named entities, numbers, and action items from all chunks

3. **Multi-Level Summarization**
   - Detailed mini-summaries for each chunk
   - Full-text deep analysis
   - Combined section synthesis
   - Enhanced final summary with structured outputs

4. **Structured Output Generation** (`output/markdown_generator.py`)
   - Generates Markdown files with YAML frontmatter
   - Deduplicates content across sections
   - Organizes by: 概要, 開催目的, 主要な論点, 議論の流れ, 決定事項, etc.

### Rate Limiting & Concurrency

The system has adaptive rate limiting (`core/rate_limiter.py`):
- Default: 5000 RPM, 200000 TPM (for gpt-4o-mini)
- Adaptive concurrency control (starts at configured max, adjusts based on success/errors)
- Two modes: Conservative (safe) and Aggressive (`--aggressive` flag)
- Environment variable `OPENAI_MAX_PARALLEL` controls max concurrent requests

### Caching Strategy

Two-tier caching system (`core/api_client.py`):
- API response caching in `.cache/` directory (JSON files with MD5 hashes)
- Text extraction caching in `data/text_cache/`
- Processed file tracking via `ProcessedDatabase` (prevents reprocessing)

### File Naming Convention

Expected PDF naming pattern:
```
{meeting_name}_第{N}回_{YYYYMMDD}_{optional_suffix}.pdf
```

Example: `デジタル社会推進会議_第05回_20230615_資料1.pdf`

Output Markdown naming:
```
{meeting_name}_第{N}回_{YYYY}-{MM}-{DD}.md
```

## Commands

### Main Processing Scripts

python src/main.py [options]
```

**Advanced features (formerly separate scripts):**
```bash
# Process only specific ministry
python src/main.py --ministry "デジタル庁"

**Text cache processing (process from cached text instead of PDFs):**
```bash
python src/main_from_text_cache.py [options]

# Process offline (no API)
python src/main_from_text_cache.py --turbo

# Skip huge files (>500KB)
python src/main_from_text_cache.py --max-size-kb 500
```

**Automated Crawling:**
```bash
# Run the PDF crawler
cd infrastructure
python main_crawler.py
```

### Common Options

**Basic usage:**
```bash
python src/main.py                          # Process all unprocessed PDFs
python src/main.py --meeting "デジタル"      # Filter by meeting name
python src/main.py --round 5                # Filter by round number
python src/main.py --overwrite              # Reprocess existing files
python src/main.py --dry-run                # Show what would be processed
```

**Performance tuning:**
```bash
python src/main.py --aggressive                      # Maximum parallelism mode
python src/main.py --rate-limit-rpm 3000             # Set requests per minute
python src/main.py --rate-limit-tpm 150000           # Set tokens per minute
python src/main.py --workers 8                       # Set worker count
```

**Caching control:**
```bash
python src/main.py --nocache                         # Disable API caching
python src/main.py --cleanup-cache 7                 # Remove cache >7 days old
```

**Maintenance:**
```bash
python src/main.py --clean                           # Clear vault and processed DB
```

### Post-Processing

**Wikilink generation (Obsidian integration):**
```bash
cd infrastructure
python src/wikilinkify.py
```

This script:
- Scans all `*.md` files in the vault
- Converts keywords from `Keywords.txt` to `[[WikiLinks]]`
- Skips YAML frontmatter, code blocks, and already-linked text
- Normalizes full-width/half-width numbers and ignores case for ASCII

## Directory Structure

```
infrastructure/
├── src/
│   ├── main.py                          # Standard processing entry point
├── src/
├── infrastructure/
│   ├── main_crawler.py                  # Crawler entry point
│   ├── requirements.txt                 # Project dependencies
│   ├── src/
│   │   ├── crawler/                     # Crawler module
│   │   ├── main.py                      # Main session-based processing entry point
│   │   ├── main_from_text_cache.py      # Process from cached text (Supports --turbo/smart)
│   ├── wikilinkify.py                   # Obsidian wikilink generator
│   ├── core/
│   │   ├── api_client.py                # OpenAI API client with caching
│   │   ├── rate_limiter.py              # Adaptive rate limiting
│   │   └── models.py                    # Pydantic models for structured output
│   ├── processing/
│   │   ├── pdf_processor.py             # PDF text extraction
│   │   ├── text_summarizer.py           # Multi-stage AI summarization
│   │   └── prompt_manager.py            # Prompt templates management
│   ├── output/
│   │   └── markdown_generator.py        # Markdown file generation
│   └── utils/
│       ├── file_utils.py                # File parsing and database
│       └── file_utils_enhanced.py       # Enhanced parsers for multi-ministry
├── data/
│   ├── raw/                             # Source PDF files
│   ├── text_cache/                      # Cached text extractions
│   └── raw_shortened/                   # Processed PDFs
├── vaults/                              # Output Obsidian vaults
│   ├── master_vault/                    # Master database (always latest)
│   ├── 20251228/                        # Daily snapshot
│   └── .cache/                          # API response cache
```

## Key Data Models

**MeetingSummary** (final output structure):
- `summary` - 3-4 sentence overview
- `main_arguments` - 5-8 key discussion points
- `discussion_flow` - Chronological flow description
- `action_items` - Concrete action items (max 5)
- `open_issues` - Unresolved issues (max 5)
- `named_entities` - Important people/organizations (max 10)
- `tags` - Categorization tags (3-5)

**ExtractionResult** (per-chunk extraction):
- `named_entities` - Names, organizations, systems (max 10)
- `numbers` - Important numbers, budgets, dates (max 10)
- `todos` - Action items and decisions (max 10)

## Environment Configuration

Required `.env` file (or `.env.fast` for fast processing):
```bash
OPENAI_API_KEY=sk-...                    # OpenAI API key
OPENAI_MODEL=gpt-4o-mini                 # Model to use
VAULT_ROOT=./vaults                      # Output directory
CLOUDFLARE_ACCOUNT_ID=...                # Optional Cloudflare gateway
CLOUDFLARE_GATEWAY_ID=...                # Optional Cloudflare gateway
MAX_CONCURRENT_REQUESTS=10               # Max parallel requests
OPENAI_MAX_PARALLEL=20                   # Max concurrent workers
```

## Processing Flow

1. **Find PDFs**: Scan `data/raw/` for PDF files matching naming pattern
2. **Check Processing Status**: Skip if already in `ProcessedDatabase` (unless `--overwrite`)
3. **Extract Text**: Use PyMuPDF to extract text from PDF
4. **Chunk Text**: Adaptively chunk based on document size
5. **Parallel Extraction**: Extract entities, numbers, todos from all chunks
6. **Multi-Stage Summarization**:
   - Detailed mini-summaries (parallel, one per chunk)
   - Deep analysis of full text (first 5000 chars)
   - Section combination and deduplication
   - Final enhanced summary generation
7. **Generate Markdown**: Create structured markdown with YAML frontmatter
8. **Write Output**: Save to `vaults/{ministry}/{meeting_name}/` directory
9. **Update Database**: Mark file as processed

## Important Implementation Details

### Adaptive Chunk Sizing
- Documents >500k tokens: Truncate to 100k chars, use 500-char chunks
- Documents >100k tokens: Use chunk_size/4 (default: 500 chars)
- Normal documents: Optimize between 1000 and chunk_size based on length

### Smart Batching
- If >50 chunks created, batch them into ~25 groups
- Reduces API calls while maintaining detail

### Dynamic Worker Adjustment
- Workers = min(rate_limiter.max_concurrent, chunk_count, 40)
- Never exceed rate limits, adapt to available parallelism

### Deduplication Strategy
- Used for main_arguments, action_items, open_issues, named_entities
- 0.8 similarity threshold (word overlap ratio)
- Case-insensitive comparison
- Prevents redundant information in final output

### Vault Organization
### Vault Organization
- Master Vault: `vaults/master_vault/` (Living database)
- Daily Snapshots: `vaults/{YYYYMMDD}/`
- Ministry-specific subdirectories
- Meeting-specific subdirectories
- One markdown file per meeting session

## Development Notes

- All processing scripts expect to be run from `infrastructure/` directory
- Python path manipulation adds `src/` to sys.path
- Logging configured at INFO level by default
- Progress bars use tqdm for long-running operations
- All markdown files use UTF-8 encoding
- YAML frontmatter uses safe_dump with allow_unicode=True
