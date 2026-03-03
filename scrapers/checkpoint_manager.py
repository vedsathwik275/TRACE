# checkpoint_manager.py
"""
Checkpoint management for TRACE Reddit scraper progress persistence.

This module handles only persistence of scraper progress — no scraping logic,
no Reddit API calls, no sentiment analysis.
"""

import os
import json
import csv
from pathlib import Path
from typing import Optional

import pandas as pd

from scrapers.reddit_config import SCRAPER_SETTINGS


def generate_query_key(subreddit: str, query: str, date_range: tuple[str, str]) -> str:
    """
    Generate a deterministic, human-readable key for a unique query combination.

    Args:
        subreddit: The subreddit name string.
        query: The search query string.
        date_range: Tuple of (start_date, end_date) in YYYY-MM-DD format.

    Returns:
        A single deterministic string uniquely identifying the combination.
        Format: {subreddit}_{query_sanitized}_{start_date}_{end_date}
    """
    # Sanitize query: replace spaces with underscores, remove special chars
    query_sanitized = query.lower().replace(" ", "_").replace("-", "_")
    query_sanitized = "".join(c for c in query_sanitized if c.isalnum() or c == "_")

    start_date, end_date = date_range
    return f"{subreddit}_{query_sanitized}_{start_date}_{end_date}"


class TRACECheckpointManager:
    """
    Manages checkpoint persistence for the TRACE Reddit scraper.

    Tracks completed query keys to avoid re-scraping and maintains a rolling
    buffer of collected records in CSV format.
    """

    def __init__(self, checkpoint_dir: Optional[str] = None) -> None:
        """
        Initialize the checkpoint manager.

        Args:
            checkpoint_dir: Optional directory path for checkpoint files.
                           Defaults to SCRAPER_SETTINGS["checkpoint_dir"] from config.
        """
        # Determine checkpoint directory
        if checkpoint_dir is None:
            checkpoint_dir = SCRAPER_SETTINGS["checkpoint_dir"]

        self.checkpoint_dir = Path(checkpoint_dir)

        # Create directory if it doesn't exist
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

        # Define file paths
        self.completed_queries_path = self.checkpoint_dir / "completed_queries.json"
        self.records_buffer_path = self.checkpoint_dir / "records_buffer.csv"

    def load_completed_queries(self) -> set[str]:
        """
        Load the set of completed query keys from the JSON checkpoint file.

        Returns:
            Set of query key strings that have been marked as complete.
            Returns an empty set if the file does not exist.
        """
        if not self.completed_queries_path.exists():
            return set()

        try:
            with open(self.completed_queries_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return set(data)
        except (json.JSONDecodeError, IOError):
            # Corrupted or unreadable file - start fresh
            return set()

    def mark_query_complete(self, query_key: str) -> None:
        """
        Mark a query key as completed by adding it to the checkpoint file.

        Args:
            query_key: The unique query identifier string.

        Note:
            This method is safe to call repeatedly (idempotent).
            Uses atomic write to prevent file corruption.
        """
        # Load current completed queries
        completed = self.load_completed_queries()

        # Add the new key (set handles duplicates automatically)
        completed.add(query_key)

        # Write atomically: write to temp file, then rename
        temp_path = self.completed_queries_path.with_suffix(".json.tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(sorted(completed), f, indent=2)

        # Atomic rename (on most filesystems)
        temp_path.replace(self.completed_queries_path)

    def save_records_batch(self, records: list[dict]) -> None:
        """
        Append a batch of records to the CSV buffer file.

        Args:
            records: List of dictionaries representing scraped records.

        Note:
            If the file doesn't exist, writes with a header row.
            If the file exists, appends without repeating the header.
            Does nothing if the records list is empty.
        """
        if not records:
            return

        # Determine if we need to write header
        write_header = not self.records_buffer_path.exists()

        # Get fieldnames from the first record
        fieldnames = list(records[0].keys())

        # Append to CSV
        with open(self.records_buffer_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerows(records)

    def load_all_records(self) -> pd.DataFrame:
        """
        Load all records from the CSV buffer file into a DataFrame.

        Returns:
            pandas DataFrame containing all buffered records.
            Returns an empty DataFrame if the file does not exist.
        """
        if not self.records_buffer_path.exists():
            return pd.DataFrame()

        return pd.read_csv(self.records_buffer_path)

    def get_record_count(self) -> int:
        """
        Count the number of data rows in the CSV buffer file.

        Returns:
            Integer count of data rows (excluding header).
            Returns 0 if the file does not exist.

        Note:
            Does not load the entire file into memory - reads only enough
            to count rows efficiently.
        """
        if not self.records_buffer_path.exists():
            return 0

        # Count lines efficiently without loading full file
        with open(self.records_buffer_path, "r", encoding="utf-8") as f:
            # Subtract 1 for header row
            line_count = sum(1 for _ in f) - 1
            return max(0, line_count)

    def clear_checkpoint(self) -> None:
        """
        Delete both checkpoint files and print a confirmation message.

        Note:
            Prints the number of records that were cleared before deletion.
        """
        # Get record count before deletion
        record_count = self.get_record_count()

        # Delete completed queries file
        if self.completed_queries_path.exists():
            self.completed_queries_path.unlink()

        # Delete records buffer file
        if self.records_buffer_path.exists():
            self.records_buffer_path.unlink()

        print(f"✅ Cleared checkpoint: {record_count} records removed")
