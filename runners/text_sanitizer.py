"""
Text sanitization utilities for cleaning NBA injury posts before LLM processing.
Removes problematic characters and formatting that can cause JSON parsing issues.
"""

import pandas as pd


def sanitize_text(text) -> str:
    """
    Clean and sanitize text to prevent JSON parsing errors and ensure valid input.

    Steps performed:
    1. Convert to string and handle None/NaN
    2. Encode to UTF-8 and decode back (strip invalid unicode)
    3. Replace newlines and carriage returns with spaces
    4. Replace tabs with spaces
    5. Replace double quotes with single quotes
    6. Replace backslashes with forward slashes
    7. Replace curly braces with parentheses
    8. Remove null bytes
    9. Remove control characters (ord < 32 except space)
    10. Collapse multiple consecutive spaces
    11. Strip leading/trailing whitespace
    12. Truncate to 500 characters

    Args:
        text: Input text (any type, will be converted to string)

    Returns:
        Sanitized string (max 500 characters)
    """
    # Step 1: Convert to string and handle None/NaN
    if text is None:
        return ''

    text = str(text)

    if text.lower() == 'nan':
        return ''

    # Step 2: Encode to UTF-8 and decode back, ignoring errors
    text = text.encode('utf-8', errors='ignore').decode('utf-8')

    # Step 3: Replace newlines and carriage returns with space
    text = text.replace('\n', ' ').replace('\r', ' ')

    # Step 4: Replace tabs with space
    text = text.replace('\t', ' ')

    # Step 5: Replace double quotes with single quotes
    text = text.replace('"', "'")

    # Step 6: Replace backslashes with forward slash
    text = text.replace('\\', '/')

    # Step 7: Replace curly braces with parentheses
    text = text.replace('{', '(').replace('}', ')')

    # Step 8: Replace null bytes with empty string
    text = text.replace('\x00', '')

    # Step 9: Remove other control characters (ord < 32 except space)
    text = ''.join(char for char in text if ord(char) >= 32 or char == ' ')

    # Step 10: Collapse multiple consecutive spaces into one
    text = ' '.join(text.split())

    # Step 11: Strip leading and trailing whitespace
    text = text.strip()

    # Step 12: Truncate to 500 characters
    text = text[:500]

    return text


def sanitize_dataframe(df: pd.DataFrame, column: str = 'text_content') -> pd.DataFrame:
    """
    Apply sanitize_text to every value in the specified column.

    Prints statistics about:
    - Number of rows that had their text changed
    - Average length before vs after sanitization

    Args:
        df: Input DataFrame
        column: Column name to sanitize (default: 'text_content')

    Returns:
        DataFrame with sanitized column
    """
    if column not in df.columns:
        print(f"Warning: Column '{column}' not found in DataFrame")
        return df

    print(f"Sanitizing column '{column}'...")

    # Store original values for comparison
    original = df[column].copy()

    # Calculate before stats
    original_str = original.apply(lambda x: str(x) if x is not None else '')
    avg_length_before = original_str.str.len().mean()

    # Apply sanitization
    df[column] = df[column].apply(sanitize_text)

    # Calculate after stats
    avg_length_after = df[column].str.len().mean()

    # Count changed rows
    changed_rows = (original_str != df[column]).sum()
    total_rows = len(df)
    percent_changed = (changed_rows / total_rows * 100) if total_rows > 0 else 0

    # Print statistics
    print(f"\nSanitization Statistics:")
    print(f"  Total rows: {total_rows:,}")
    print(f"  Rows changed: {changed_rows:,} ({percent_changed:.1f}%)")
    print(f"  Average length before: {avg_length_before:.1f} characters")
    print(f"  Average length after: {avg_length_after:.1f} characters")
    print(f"  Length reduction: {avg_length_before - avg_length_after:.1f} characters ({((avg_length_before - avg_length_after) / avg_length_before * 100) if avg_length_before > 0 else 0:.1f}%)")
    print()

    return df
