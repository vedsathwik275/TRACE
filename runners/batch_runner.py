"""
Batch runner for NBA Achilles injury post classification.
Supports resumable processing with checkpointing and stratified sampling.
"""

import pandas as pd
import argparse
import time
from datetime import datetime
from pathlib import Path
from text_sanitizer import sanitize_dataframe
from gemini_classifier import classify_record

# Configuration
BATCH_SIZE = 50
API_DELAY = 0.5  # seconds between API calls
SAMPLE_PERCENTAGE = 0.10  # 10% for default run


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Batch classify NBA Achilles injury posts using Gemini LLM'
    )
    parser.add_argument(
        '--full',
        action='store_true',
        help='Process all records instead of 10%% stratified sample'
    )
    return parser.parse_args()


def stratified_sample(df, sample_percentage):
    """
    Create stratified sample across source_platform values.

    Args:
        df: Input DataFrame
        sample_percentage: Percentage of total records to sample (0.0-1.0)

    Returns:
        Sampled DataFrame with indices preserved
    """
    total_target = int(len(df) * sample_percentage)
    platforms = df['source_platform'].value_counts()
    num_platforms = len(platforms)

    print(f"Creating {sample_percentage*100:.0f}% stratified sample ({total_target:,} records)...")
    print(f"\nPlatform distribution in full dataset:")
    for platform, count in platforms.items():
        print(f"  {platform}: {count:,} rows")

    # Calculate target per platform
    target_per_platform = total_target // num_platforms
    remainder = total_target % num_platforms

    print(f"\nTarget per platform: ~{target_per_platform} rows")

    # Sample from each platform
    sampled_dfs = []
    actual_samples = {}

    for idx, (platform, count) in enumerate(platforms.items()):
        # Add extra sample to first platforms if there's a remainder
        target = target_per_platform + (1 if idx < remainder else 0)

        # Take all records if platform has fewer than target
        if count < target:
            sample = df[df['source_platform'] == platform]
            actual_samples[platform] = len(sample)
        else:
            sample = df[df['source_platform'] == platform].sample(n=target, random_state=42)
            actual_samples[platform] = target

        sampled_dfs.append(sample)

    # Combine all samples
    sample_df = pd.concat(sampled_dfs, ignore_index=False)

    # If we're short due to small platforms, sample more from larger platforms
    if len(sample_df) < total_target:
        shortage = total_target - len(sample_df)
        print(f"\n⚠ Short by {shortage} samples, supplementing from larger platforms...")

        large_platforms = [p for p, c in platforms.items() if c > actual_samples[p]]

        for platform in large_platforms:
            if shortage == 0:
                break

            available = platforms[platform] - actual_samples[platform]
            to_sample = min(shortage, available)

            # Sample additional records (excluding already sampled)
            already_sampled_idx = sample_df[sample_df['source_platform'] == platform].index
            remaining = df[(df['source_platform'] == platform) & (~df.index.isin(already_sampled_idx))]
            additional = remaining.sample(n=to_sample, random_state=42)

            sampled_dfs.append(additional)
            actual_samples[platform] += to_sample
            shortage -= to_sample

        sample_df = pd.concat(sampled_dfs, ignore_index=False)

    print(f"\nActual samples per platform:")
    for platform, count in sorted(actual_samples.items()):
        percentage = (count / len(sample_df) * 100)
        print(f"  {platform}: {count} rows ({percentage:.1f}%)")

    print(f"\nTotal sample size: {len(sample_df)} rows")

    return sample_df


def load_checkpoint(output_file):
    """
    Load existing results to determine which records have been processed.

    Args:
        output_file: Path to output CSV file

    Returns:
        Set of row indices already processed
    """
    if not Path(output_file).exists():
        print(f"No checkpoint found at {output_file}")
        return set()

    try:
        existing_df = pd.read_csv(output_file)
        processed_indices = set(existing_df['row_index'].values)
        print(f"✓ Loaded checkpoint: {len(processed_indices):,} records already processed")
        return processed_indices
    except Exception as e:
        print(f"⚠ Error loading checkpoint: {e}")
        return set()


def save_batch(batch_results, output_file, is_first_batch):
    """
    Save batch results to CSV file.

    Args:
        batch_results: List of result dictionaries
        output_file: Path to output CSV file
        is_first_batch: Whether this is the first batch (write header)
    """
    batch_df = pd.DataFrame(batch_results)

    # Append to file (write header only for first batch)
    mode = 'w' if is_first_batch else 'a'
    header = is_first_batch

    batch_df.to_csv(output_file, mode=mode, header=header, index=False)


def main():
    """Main execution function."""
    args = parse_args()

    print("=" * 80)
    print("NBA ACHILLES INJURY CLASSIFICATION - BATCH RUNNER")
    print("=" * 80)
    print(f"Mode: {'FULL DATASET' if args.full else '10% STRATIFIED SAMPLE'}")
    print()

    # Determine output file
    output_file = 'data/llm_classifications_full.csv' if args.full else 'data/llm_classifications_sample.csv'
    print(f"Output file: {output_file}")
    print()

    # Step 1: Load and sanitize data
    print("Step 1: Loading and sanitizing data...")
    df = pd.read_csv('data/sentiment_results.csv')
    print(f"Loaded {len(df):,} rows")
    print()

    df = sanitize_dataframe(df)

    # Step 2: Sample or use full dataset
    print("=" * 80)
    print("Step 2: Preparing dataset...")
    print("=" * 80)
    print()

    if args.full:
        print("Using full dataset")
        working_df = df
    else:
        working_df = stratified_sample(df, SAMPLE_PERCENTAGE)

    print()

    # Step 3: Load checkpoint and filter out already processed records
    print("=" * 80)
    print("Step 3: Checking for existing progress...")
    print("=" * 80)
    print()

    processed_indices = load_checkpoint(output_file)
    remaining_df = working_df[~working_df.index.isin(processed_indices)]

    print(f"\nRecords already completed: {len(processed_indices):,}")
    print(f"Records remaining: {len(remaining_df):,}")
    print()

    if len(remaining_df) == 0:
        print("✓ All records already processed!")
        return

    # Step 4: Process in batches
    print("=" * 80)
    print("Step 4: Processing records...")
    print("=" * 80)
    print()

    start_time = time.time()
    total_to_process = len(remaining_df)
    processed_count = 0
    suitable_count = 0
    unsuitable_count = 0
    error_count = 0

    batch_results = []
    is_first_batch = len(processed_indices) == 0

    for idx, (row_idx, row) in enumerate(remaining_df.iterrows(), 1):
        try:
            result = classify_record(row)

            # Track counts
            if result['classification'] == 'SUITABLE':
                suitable_count += 1
            elif result['classification'] == 'UNSUITABLE':
                unsuitable_count += 1
            elif result['classification'] == 'ERROR':
                error_count += 1

            # Store result
            batch_results.append({
                'row_index': row_idx,
                'source_platform': row['source_platform'],
                'is_achilles_related': row['is_achilles_related'],
                'text_preview': str(row['text_content'])[:80],
                'classification': result['classification'],
                'confidence': result['confidence'],
                'reasoning': result['reasoning'],
                'recovery_phase': result['recovery_phase'],
                'key_entities': ', '.join(result.get('key_entities', [])),
                'error': result.get('error', ''),
                'processed_at': datetime.now().isoformat()
            })

        except Exception as e:
            error_count += 1
            error_msg = f"{type(e).__name__}: {str(e)}"

            batch_results.append({
                'row_index': row_idx,
                'source_platform': row['source_platform'],
                'is_achilles_related': row['is_achilles_related'],
                'text_preview': str(row.get('text_content', ''))[:80],
                'classification': 'ERROR',
                'confidence': 0.0,
                'reasoning': '',
                'recovery_phase': 'unknown',
                'key_entities': '',
                'error': error_msg,
                'processed_at': datetime.now().isoformat()
            })

        processed_count += 1

        # Save batch when it reaches BATCH_SIZE or at the end
        if len(batch_results) >= BATCH_SIZE or idx == total_to_process:
            save_batch(batch_results, output_file, is_first_batch)
            is_first_batch = False

            # Calculate stats
            elapsed = time.time() - start_time
            records_remaining = total_to_process - processed_count
            avg_time_per_record = elapsed / processed_count if processed_count > 0 else 0

            print(f"Batch {(processed_count // BATCH_SIZE) + 1} completed:")
            print(f"  Processed: {processed_count:,} / {total_to_process:,}")
            print(f"  Remaining: {records_remaining:,}")
            print(f"  SUITABLE: {suitable_count} | UNSUITABLE: {unsuitable_count} | ERROR: {error_count}")
            print(f"  Elapsed: {elapsed:.1f}s | Avg: {avg_time_per_record:.2f}s/record")
            print()

            # Clear batch
            batch_results = []

        # Delay between API calls (except for last one)
        if idx < total_to_process:
            time.sleep(API_DELAY)

    # Step 5: Final summary
    total_time = time.time() - start_time

    print("=" * 80)
    print("BATCH RUN SUMMARY")
    print("=" * 80)
    print()

    # Load full results file for final analysis
    results_df = pd.read_csv(output_file)

    total = len(results_df)
    suitable = len(results_df[results_df['classification'] == 'SUITABLE'])
    unsuitable = len(results_df[results_df['classification'] == 'UNSUITABLE'])
    errors = len(results_df[results_df['classification'] == 'ERROR'])

    print(f"Total processed: {total:,}")
    print(f"  SUITABLE: {suitable:,} ({suitable/total*100:.1f}%)")
    print(f"  UNSUITABLE: {unsuitable:,} ({unsuitable/total*100:.1f}%)")
    print(f"  ERROR: {errors:,} ({errors/total*100:.1f}%)")
    print()

    # Average confidence
    successful = results_df[results_df['classification'].isin(['SUITABLE', 'UNSUITABLE'])]
    if len(successful) > 0:
        avg_confidence = successful['confidence'].mean()
        print(f"Average Confidence: {avg_confidence:.3f}")
        print()

        # SUITABLE rate by platform
        print("SUITABLE Rate by Platform:")
        for platform in results_df['source_platform'].unique():
            platform_df = results_df[results_df['source_platform'] == platform]
            platform_successful = platform_df[platform_df['classification'].isin(['SUITABLE', 'UNSUITABLE'])]

            if len(platform_successful) > 0:
                platform_suitable = len(platform_df[platform_df['classification'] == 'SUITABLE'])
                suitable_rate = platform_suitable / len(platform_successful) * 100
                print(f"  {platform}: {platform_suitable}/{len(platform_successful)} ({suitable_rate:.1f}%)")
        print()

        # SUITABLE rate by is_achilles_related
        print("SUITABLE Rate by is_achilles_related:")
        for achilles_value in [True, False]:
            achilles_df = results_df[results_df['is_achilles_related'] == achilles_value]
            achilles_successful = achilles_df[achilles_df['classification'].isin(['SUITABLE', 'UNSUITABLE'])]

            if len(achilles_successful) > 0:
                achilles_suitable = len(achilles_df[achilles_df['classification'] == 'SUITABLE'])
                suitable_rate = achilles_suitable / len(achilles_successful) * 100
                print(f"  is_achilles_related={achilles_value}: {achilles_suitable}/{len(achilles_successful)} ({suitable_rate:.1f}%)")
        print()

    # Timing stats
    print(f"Total time elapsed: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    print(f"Average time per record: {total_time/processed_count:.2f}s")
    print()

    print("=" * 80)
    print("BATCH RUN COMPLETE")
    print("=" * 80)
    print(f"\n✓ Results saved to {output_file}")


if __name__ == '__main__':
    main()
