"""
GitMind — ADR Parser and Importer
----------------------------------
Rajveer's Task R-1

What this file does:
- Scans a repository's docs folder for markdown ADR files
- Parses each file to extract: id, title, status, date, context, decision, consequences
- Saves each ADR into Snowflake's ADR_RECORDS table
- Creates a Decision node in Neo4j for each accepted ADR
- Prints a summary at the end

How to run:
    python backend/ingestion/adr_parser.py --path /path/to/your/repo/docs

Or to test with sample data (no real connections needed):
    python backend/ingestion/adr_parser.py --test
"""

import os
import re
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────
# SECTION 1 — ADR FILE FINDER
# ─────────────────────────────────────────────

# These are the folder names we look inside for ADR markdown files
ADR_FOLDER_NAMES = [
    "decisions",
    "adr",
    "architecture",
    "architectural-decisions",
    "docs/decisions",
    "docs/adr",
    "docs/architecture",
]


def find_adr_files(root_path: str) -> list[Path]:
    """
    Recursively searches a repository path for markdown files
    that are inside ADR-style folders.

    Returns a list of Path objects pointing to each .md file found.
    """
    root = Path(root_path)

    if not root.exists():
        print(f"ERROR: Path does not exist: {root_path}")
        return []

    found_files = []

    # Walk through every folder in the repository
    for folder in root.rglob("*"):
        if not folder.is_dir():
            continue

        # Check if this folder's name matches any known ADR folder name
        folder_lower = folder.name.lower()
        if any(adr_name in str(folder).lower() for adr_name in ADR_FOLDER_NAMES):
            # Found an ADR folder — collect all .md files inside it
            md_files = list(folder.glob("*.md"))
            found_files.extend(md_files)

    # Remove duplicates (a file might match multiple folder patterns)
    found_files = list(set(found_files))

    print(f"Found {len(found_files)} markdown files in ADR folders")
    return found_files


# ─────────────────────────────────────────────
# SECTION 2 — ADR FILE PARSER
# ─────────────────────────────────────────────

def extract_adr_id_from_filename(filename: str) -> str:
    """
    Tries to extract an ADR ID from the filename.

    Examples:
        "ADR-001-use-postgresql.md"  →  "ADR-001"
        "0012-jwt-authentication.md" →  "ADR-0012"
        "adr_005_caching.md"         →  "ADR-005"
        "random-name.md"             →  None
    """
    # Pattern 1: ADR-001, adr-12, ADR_005
    match = re.search(r'(?i)(adr[-_]?\d+)', filename)
    if match:
        # Normalize to ADR-XXX format
        digits = re.search(r'\d+', match.group())
        return f"ADR-{digits.group().zfill(3)}"

    # Pattern 2: just a number at the start like "0012-something.md"
    match = re.search(r'^(\d+)', filename)
    if match:
        return f"ADR-{match.group().zfill(3)}"

    return None


def extract_adr_id_from_heading(content: str) -> str:
    """
    Tries to extract an ADR ID from the first heading of the file.

    Examples:
        "# ADR-12: Use PostgreSQL"  →  "ADR-012"
        "# 5. Decision: JWT tokens" →  "ADR-005"
    """
    first_line = content.strip().split('\n')[0]

    match = re.search(r'(?i)(adr[-_]?\s*\d+)', first_line)
    if match:
        digits = re.search(r'\d+', match.group())
        return f"ADR-{digits.group().zfill(3)}"

    match = re.search(r'^#+\s*(\d+)[.\s]', first_line)
    if match:
        return f"ADR-{match.group(1).zfill(3)}"

    return None


def extract_section(content: str, section_name: str) -> str:
    """
    Extracts text from a specific markdown section.

    For example, given section_name="Decision", this finds:
        ## Decision
        We chose PostgreSQL because...
        (everything until the next ## heading)

    Returns the extracted text, or empty string if section not found.
    """
    # Build a pattern that matches ## SectionName (case insensitive)
    pattern = rf'(?i)^#+\s*{re.escape(section_name)}\s*\n(.*?)(?=\n#+\s|\Z)'
    match = re.search(pattern, content, re.MULTILINE | re.DOTALL)

    if match:
        return match.group(1).strip()
    return ""


def extract_metadata_field(content: str, field_name: str) -> str:
    """
    Extracts a metadata field like:
        **Status**: Accepted
        **Date**: 2024-03-14
        Status: Accepted

    Returns the value, or empty string if not found.
    """
    # Pattern 1: **Field**: Value  or  *Field*: Value
    pattern = rf'(?i)\*{{1,2}}{re.escape(field_name)}\*{{1,2}}\s*:?\s*(.+)'
    match = re.search(pattern, content)
    if match:
        return match.group(1).strip()

    # Pattern 2: Field: Value  (plain text)
    pattern = rf'(?i)^{re.escape(field_name)}\s*:\s*(.+)'
    match = re.search(pattern, content, re.MULTILINE)
    if match:
        return match.group(1).strip()

    return ""


def parse_adr_file(file_path: Path) -> dict:
    """
    Parses a single ADR markdown file and returns a dict
    containing all extracted fields.

    This is the main parsing function. It tries standard format first,
    then falls back to unstructured extraction if headers don't match.
    """
    try:
        content = file_path.read_text(encoding='utf-8')
    except Exception as e:
        print(f"  WARNING: Could not read {file_path.name}: {e}")
        return None

    # --- Extract ADR ID ---
    # Try filename first, then heading, then generate one
    adr_id = (
        extract_adr_id_from_filename(file_path.stem)
        or extract_adr_id_from_heading(content)
        or f"ADR-{abs(hash(file_path.name)) % 1000:03d}"
    )

    # --- Extract Title ---
    # Look for the first # heading
    title_match = re.search(r'^#+\s+(.+)', content, re.MULTILINE)
    if title_match:
        title = title_match.group(1).strip()
        # Remove the ADR ID from title if it's there
        title = re.sub(r'(?i)adr[-_]?\d+\s*:?\s*', '', title).strip()
    else:
        title = file_path.stem.replace('-', ' ').replace('_', ' ').title()

    # --- Extract Status ---
    status = extract_metadata_field(content, 'Status') or "Unknown"
    # Normalize common status values
    status_map = {
        'accepted': 'Accepted',
        'proposed': 'Proposed',
        'deprecated': 'Deprecated',
        'superseded': 'Superseded',
        'rejected': 'Rejected',
        'draft': 'Draft',
    }
    status = status_map.get(status.lower(), status)

    # --- Extract Date ---
    date_str = extract_metadata_field(content, 'Date')
    if not date_str:
        # Try to find any date pattern in the content
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', content)
        date_str = date_match.group() if date_match else None

    # --- Extract Main Sections ---
    context_text = extract_section(content, 'Context')
    decision_text = extract_section(content, 'Decision')
    consequences_text = (
        extract_section(content, 'Consequences')
        or extract_section(content, 'Consequence')
        or extract_section(content, 'Impact')
    )

    # --- Determine if structured or unstructured ---
    # If we found at least one real section, it's structured
    is_structured = bool(context_text or decision_text or consequences_text)

    if not is_structured:
        # Fallback: store the full content as unstructured
        unstructured_content = content
        context_text = ""
        decision_text = ""
        consequences_text = ""
    else:
        unstructured_content = None

    result = {
        "adr_id": adr_id,
        "title": title,
        "status": status,
        "date": date_str,
        "file_path": str(file_path),
        "context_text": context_text,
        "decision_text": decision_text,
        "consequences_text": consequences_text,
        "unstructured_content": unstructured_content,
        "is_structured": is_structured,
        "parsed_at": datetime.utcnow().isoformat(),
    }

    return result


# ─────────────────────────────────────────────
# SECTION 3 — SNOWFLAKE UPSERTER
# ─────────────────────────────────────────────

def get_snowflake_connection():
    """
    Creates and returns a Snowflake connection using environment variables.
    Returns None if credentials are not set (for testing without Snowflake).
    """
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USER',
        'SNOWFLAKE_PASSWORD',
        'SNOWFLAKE_DATABASE',
        'SNOWFLAKE_SCHEMA',
        'SNOWFLAKE_WAREHOUSE',
    ]

    # Check if any credentials are missing
    missing = [v for v in required_vars if not os.getenv(v)]
    if missing:
        print(f"  Snowflake credentials not fully set. Skipping Snowflake upload.")
        print(f"  Missing: {', '.join(missing)}")
        return None

    try:
        import snowflake.connector
        conn = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        )
        print("  Connected to Snowflake successfully")
        return conn
    except Exception as e:
        print(f"  Snowflake connection failed: {e}")
        return None


def ensure_adr_table_exists(cursor):
    """
    Creates the ADR_RECORDS table in Snowflake if it doesn't exist.
    Safe to run multiple times — uses CREATE TABLE IF NOT EXISTS.
    """
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ADR_RECORDS (
            adr_id          VARCHAR(50)     NOT NULL COMMENT 'Unique ADR identifier e.g. ADR-001',
            title           VARCHAR(500)    COMMENT 'Title of the ADR',
            status          VARCHAR(50)     COMMENT 'Accepted, Proposed, Deprecated, etc.',
            date            DATE            COMMENT 'Date the ADR was written',
            file_path       VARCHAR(1000)   COMMENT 'Path to the source file',
            context_text    TEXT            COMMENT 'Context section content',
            decision_text   TEXT            COMMENT 'Decision section content',
            consequences_text TEXT          COMMENT 'Consequences section content',
            unstructured_content TEXT       COMMENT 'Full content if non-standard format',
            is_structured   BOOLEAN         COMMENT 'Whether standard sections were found',
            parsed_at       TIMESTAMP_TZ    COMMENT 'When this record was parsed',
            metadata_updated_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
                COMMENT 'Last time this record was updated',
            PRIMARY KEY (adr_id)
        )
    """)


def upsert_adr_to_snowflake(cursor, adr: dict) -> str:
    """
    Inserts or updates one ADR record in Snowflake.

    Returns "inserted" if it was new, "updated" if it already existed.
    """
    # Check if this ADR ID already exists
    cursor.execute(
        "SELECT adr_id FROM ADR_RECORDS WHERE adr_id = %s",
        (adr['adr_id'],)
    )
    exists = cursor.fetchone()

    if exists:
        # Update the existing record
        cursor.execute("""
            UPDATE ADR_RECORDS SET
                title = %s,
                status = %s,
                date = %s,
                file_path = %s,
                context_text = %s,
                decision_text = %s,
                consequences_text = %s,
                unstructured_content = %s,
                is_structured = %s,
                parsed_at = %s,
                metadata_updated_at = CURRENT_TIMESTAMP()
            WHERE adr_id = %s
        """, (
            adr['title'],
            adr['status'],
            adr['date'],
            adr['file_path'],
            adr['context_text'],
            adr['decision_text'],
            adr['consequences_text'],
            adr['unstructured_content'],
            adr['is_structured'],
            adr['parsed_at'],
            adr['adr_id'],
        ))
        return "updated"
    else:
        # Insert new record
        cursor.execute("""
            INSERT INTO ADR_RECORDS (
                adr_id, title, status, date, file_path,
                context_text, decision_text, consequences_text,
                unstructured_content, is_structured, parsed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            adr['adr_id'],
            adr['title'],
            adr['status'],
            adr['date'],
            adr['file_path'],
            adr['context_text'],
            adr['decision_text'],
            adr['consequences_text'],
            adr['unstructured_content'],
            adr['is_structured'],
            adr['parsed_at'],
        ))
        return "inserted"


# ─────────────────────────────────────────────
# SECTION 4 — NEO4J NODE CREATOR
# ─────────────────────────────────────────────

def get_neo4j_driver():
    """
    Creates and returns a Neo4j driver using environment variables.
    Returns None if credentials are not set.
    """
    uri = os.getenv('NEO4J_URI')
    username = os.getenv('NEO4J_USERNAME', 'neo4j')
    password = os.getenv('NEO4J_PASSWORD')

    if not uri or not password:
        print("  Neo4j credentials not set. Skipping Neo4j node creation.")
        return None

    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(uri, auth=(username, password))
        # Verify connection
        driver.verify_connectivity()
        print("  Connected to Neo4j successfully")
        return driver
    except Exception as e:
        print(f"  Neo4j connection failed: {e}")
        return None


def create_decision_node_in_neo4j(driver, adr: dict) -> bool:
    """
    Creates a Decision node in Neo4j for this ADR.

    Only creates nodes for ADRs with status "Accepted" —
    proposed or deprecated ADRs don't shape the codebase yet.

    Returns True if node was created, False if skipped.
    """
    if adr['status'] != 'Accepted':
        return False

    # The Cypher query to create or update the node
    # MERGE means: create it if it doesn't exist, match it if it does
    cypher = """
        MERGE (d:Decision {id: $adr_id})
        SET d.title = $title,
            d.summary = $summary,
            d.source_type = 'ADR',
            d.source_id = $adr_id,
            d.timestamp = $date,
            d.status = $status,
            d.file_path = $file_path,
            d.consequences = $consequences,
            d.updated_at = timestamp()
        RETURN d.id as created_id
    """

    # Build a summary from the decision text
    # (first 200 characters if it's long)
    decision_text = adr.get('decision_text') or adr.get('unstructured_content') or ''
    summary = decision_text[:200] + '...' if len(decision_text) > 200 else decision_text

    try:
        with driver.session() as session:
            result = session.run(cypher, {
                'adr_id': adr['adr_id'],
                'title': adr['title'],
                'summary': summary,
                'date': adr['date'] or 'unknown',
                'status': adr['status'],
                'file_path': adr['file_path'],
                'consequences': adr.get('consequences_text', ''),
            })
            record = result.single()
            return record is not None
    except Exception as e:
        print(f"  Neo4j node creation failed for {adr['adr_id']}: {e}")
        return False


# ─────────────────────────────────────────────
# SECTION 5 — MAIN PIPELINE
# ─────────────────────────────────────────────

def run_adr_pipeline(docs_path: str):
    """
    Main function that runs the complete ADR ingestion pipeline:
    1. Find all ADR files
    2. Parse each one
    3. Save to Snowflake
    4. Create Neo4j nodes for accepted ADRs
    5. Print summary
    """
    print("\n" + "="*60)
    print("GitMind ADR Parser — Starting")
    print("="*60)

    # --- Step 1: Find files ---
    print(f"\n📁 Scanning: {docs_path}")
    files = find_adr_files(docs_path)

    if not files:
        print("No ADR files found. Check your path and folder structure.")
        return

    # --- Step 2: Parse all files ---
    print(f"\n📄 Parsing {len(files)} files...")
    parsed_adrs = []
    parse_errors = 0

    for file_path in files:
        print(f"  Parsing: {file_path.name}")
        result = parse_adr_file(file_path)
        if result:
            parsed_adrs.append(result)
            structured = "✅ structured" if result['is_structured'] else "⚠️  unstructured"
            print(f"    → {result['adr_id']}: {result['title'][:50]} [{result['status']}] {structured}")
        else:
            parse_errors += 1
            print(f"    → FAILED to parse")

    # --- Step 3: Connect to Snowflake ---
    print(f"\n❄️  Connecting to Snowflake...")
    sf_conn = get_snowflake_connection()
    sf_inserted = 0
    sf_updated = 0
    sf_skipped = 0

    if sf_conn:
        cursor = sf_conn.cursor()
        ensure_adr_table_exists(cursor)

        for adr in parsed_adrs:
            try:
                action = upsert_adr_to_snowflake(cursor, adr)
                if action == "inserted":
                    sf_inserted += 1
                elif action == "updated":
                    sf_updated += 1
            except Exception as e:
                print(f"  Snowflake error for {adr['adr_id']}: {e}")
                sf_skipped += 1

        sf_conn.commit()
        sf_conn.close()
        print(f"  Snowflake: {sf_inserted} inserted, {sf_updated} updated, {sf_skipped} skipped")
    else:
        sf_skipped = len(parsed_adrs)
        print(f"  Snowflake skipped — no credentials")

    # --- Step 4: Connect to Neo4j ---
    print(f"\n🕸️  Connecting to Neo4j...")
    neo4j_driver = get_neo4j_driver()
    neo4j_created = 0
    neo4j_skipped = 0

    if neo4j_driver:
        for adr in parsed_adrs:
            created = create_decision_node_in_neo4j(neo4j_driver, adr)
            if created:
                neo4j_created += 1
                print(f"  Created Decision node: {adr['adr_id']}")
            else:
                neo4j_skipped += 1
                reason = "not Accepted" if adr['status'] != 'Accepted' else "already exists"
                print(f"  Skipped {adr['adr_id']} ({reason})")

        neo4j_driver.close()
        print(f"  Neo4j: {neo4j_created} nodes created, {neo4j_skipped} skipped")
    else:
        print(f"  Neo4j skipped — no credentials")

    # --- Step 5: Print summary ---
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"  Files found:        {len(files)}")
    print(f"  Successfully parsed:{len(parsed_adrs)}")
    print(f"  Parse errors:       {parse_errors}")
    print(f"  Snowflake inserted: {sf_inserted}")
    print(f"  Snowflake updated:  {sf_updated}")
    print(f"  Neo4j nodes created:{neo4j_created}")
    print("="*60)
    print(f"\nParsed {len(parsed_adrs)} ADRs, "
          f"{sf_inserted} inserted, "
          f"{sf_skipped} skipped (already exist)")

    return parsed_adrs


# ─────────────────────────────────────────────
# SECTION 6 — TEST MODE (No real connections)
# ─────────────────────────────────────────────

def run_test_mode():
    """
    Test mode — creates sample ADR files, parses them,
    and prints the results without needing Snowflake or Neo4j.

    Run with: python adr_parser.py --test
    """
    import tempfile

    print("\n" + "="*60)
    print("GitMind ADR Parser — TEST MODE")
    print("(No real Snowflake or Neo4j connections needed)")
    print("="*60)

    # Create a temporary directory with sample ADR files
    with tempfile.TemporaryDirectory() as tmpdir:
        decisions_dir = Path(tmpdir) / "docs" / "decisions"
        decisions_dir.mkdir(parents=True)

        # Sample ADR 1 — standard format
        (decisions_dir / "ADR-001-use-jwt.md").write_text("""
# ADR-001: Use JWT for Authentication

**Status**: Accepted
**Date**: 2024-01-15

## Context
We need a stateless authentication mechanism that works
across our microservices without shared session storage.

## Decision
We will use JSON Web Tokens (JWT) for all authentication.
Tokens will expire after 24 hours and must be refreshed
using the /auth/refresh endpoint.

## Consequences
- All services must validate JWT signatures
- Token expiry logic must be consistent across all services
- Server time synchronization is assumed (see ADR-002)
""")

        # Sample ADR 2 — standard format, critical one
        (decisions_dir / "ADR-002-utc-timestamps.md").write_text("""
# ADR-002: Use UTC Timestamps and Assume Server Time Sync

**Status**: Accepted
**Date**: 2024-02-01

## Context
Multiple services run on different servers. We need
consistent timestamp handling to avoid token validation
failures due to clock skew.

## Decision
All timestamps will use UTC. We assume NTP synchronization
is configured on all servers. Clock skew handling is
deferred to a future sprint (see ENG-103).

## Consequences
- All servers MUST have NTP sync configured
- Token validation assumes clocks are synchronized
- If NTP is not configured, legacy tokens may fail validation
""")

        # Sample ADR 3 — non-standard format (tests fallback)
        (decisions_dir / "adr_003_oauth.md").write_text("""
# OAuth Migration Plan

Status: Proposed
Date: 2024-03-01

We decided to migrate to OAuth 2.0 for third-party integrations.
Legacy users will use a compatibility layer during the transition.
This was discussed in the March sprint planning session.
""")

        # Run the parser on these sample files
        files = find_adr_files(tmpdir)
        print(f"\nFound {len(files)} sample ADR files\n")

        parsed = []
        for f in files:
            result = parse_adr_file(f)
            if result:
                parsed.append(result)
                print(f"✅ Parsed: {result['adr_id']}")
                print(f"   Title:    {result['title']}")
                print(f"   Status:   {result['status']}")
                print(f"   Date:     {result['date']}")
                print(f"   Format:   {'Structured' if result['is_structured'] else 'Unstructured (fallback)'}")
                if result['decision_text']:
                    preview = result['decision_text'][:80].replace('\n', ' ')
                    print(f"   Decision: {preview}...")
                print()

        print("="*60)
        print(f"TEST COMPLETE: Parsed {len(parsed)}/{len(files)} ADR files successfully")
        print("Parser is working correctly ✅")
        print("="*60)
        print("\nTo use with real data:")
        print("  python backend/ingestion/adr_parser.py --path /path/to/your/repo")
        print("\nTo use with real Snowflake + Neo4j:")
        print("  Fill in your .env file with credentials, then run the above command")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="GitMind ADR Parser — parses ADR markdown files into Snowflake and Neo4j"
    )
    parser.add_argument(
        "--path",
        type=str,
        help="Path to the repository root or docs folder to scan for ADR files"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in test mode with sample data (no real connections needed)"
    )

    args = parser.parse_args()

    if args.test:
        run_test_mode()
    elif args.path:
        run_adr_pipeline(args.path)
    else:
        print("Usage:")
        print("  Test mode (no credentials needed):")
        print("    python backend/ingestion/adr_parser.py --test")
        print()
        print("  Real mode (needs .env credentials):")
        print("    python backend/ingestion/adr_parser.py --path /path/to/repo")