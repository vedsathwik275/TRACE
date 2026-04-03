"""
Pilot runner for NBA Achilles injury post classification.
Samples 100 records stratified across platforms and runs Gemini classification.
"""

import pandas as pd
import time
from text_sanitizer import sanitize_dataframe
from gemini_classifier import classify_record

# Configuration
SAMPLE_SIZE = 100
API_DELAY = 0.5  # seconds between API calls

print("=" * 80)
print("NBA ACHILLES INJURY CLASSIFICATION - PILOT RUN")
print("=" * 80)
print()

# Step 1: Load and sanitize data
print("Step 1: Loading and sanitizing data...")
df = pd.read_csv('data/sentiment_results.csv')
print(f"Loaded {len(df):,} rows")
print()

df = sanitize_dataframe(df)

# Step 2: Stratified sampling across platforms
print("Step 2: Stratified sampling across platforms...")
platforms = df['source_platform'].value_counts()
print(f"\nPlatform distribution in full dataset:")
for platform, count in platforms.items():
    print(f"  {platform}: {count:,} rows")

# Calculate target per platform
num_platforms = len(platforms)
target_per_platform = SAMPLE_SIZE // num_platforms
remainder = SAMPLE_SIZE % num_platforms

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
if len(sample_df) < SAMPLE_SIZE:
    shortage = SAMPLE_SIZE - len(sample_df)
    print(f"\n⚠ Short by {shortage} samples, supplementing from larger platforms...")

    # Get platforms with room to sample more
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
print()

# Step 3: Run classification
print("=" * 80)
print("Step 3: Running classification (with 0.5s delay between calls)...")
print("=" * 80)
print()

results = []
for idx, (row_idx, row) in enumerate(sample_df.iterrows(), 1):
    print(f"[{idx}/{len(sample_df)}] Row {row_idx}")
    print(f"  Platform: {row['source_platform']}")
    print(f"  Is Achilles Related: {row['is_achilles_related']}")

    text_preview = str(row['text_content'])[:80]
    print(f"  Text: {text_preview}...")

    try:
        result = classify_record(row)

        # Print classification results
        print(f"  → Classification: {result['classification']}")
        print(f"  → Confidence: {result['confidence']:.2f}")
        print(f"  → Reasoning: {result['reasoning'][:100]}...")

        # Store result
        results.append({
            'row_index': row_idx,
            'source_platform': row['source_platform'],
            'is_achilles_related': row['is_achilles_related'],
            'text_preview': text_preview,
            'classification': result['classification'],
            'confidence': result['confidence'],
            'reasoning': result['reasoning'],
            'recovery_phase': result['recovery_phase'],
            'key_entities': ', '.join(result.get('key_entities', [])),
            'error': result.get('error', '')
        })

    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        print(f"  ✗ Exception: {error_msg}")

        results.append({
            'row_index': row_idx,
            'source_platform': row['source_platform'],
            'is_achilles_related': row['is_achilles_related'],
            'text_preview': text_preview,
            'classification': 'ERROR',
            'confidence': 0.0,
            'reasoning': '',
            'recovery_phase': 'unknown',
            'key_entities': '',
            'error': error_msg
        })

    print()

    # Delay between API calls (except for last one)
    if idx < len(sample_df):
        time.sleep(API_DELAY)

# Step 4: Save results
print("=" * 80)
print("Step 4: Saving results...")
print("=" * 80)

results_df = pd.DataFrame(results)
output_path = 'data/pilot_results.csv'
results_df.to_csv(output_path, index=False)
print(f"\n✓ Results saved to {output_path}")
print()

# Step 5: Print summary
print("=" * 80)
print("PILOT RUN SUMMARY")
print("=" * 80)
print()

# Overall stats
total = len(results_df)
successful = len(results_df[results_df['classification'].isin(['SUITABLE', 'UNSUITABLE'])])
errors = len(results_df[results_df['classification'] == 'ERROR'])

print(f"Total processed: {total}")
print(f"Successful: {successful} ({successful/total*100:.1f}%)")
print(f"Errors: {errors} ({errors/total*100:.1f}%)")
print()

if successful > 0:
    # Classification breakdown
    suitable = len(results_df[results_df['classification'] == 'SUITABLE'])
    unsuitable = len(results_df[results_df['classification'] == 'UNSUITABLE'])

    print(f"Classification Breakdown:")
    print(f"  SUITABLE: {suitable} ({suitable/successful*100:.1f}%)")
    print(f"  UNSUITABLE: {unsuitable} ({unsuitable/successful*100:.1f}%)")
    print()

    # Average confidence
    avg_confidence = results_df[results_df['classification'].isin(['SUITABLE', 'UNSUITABLE'])]['confidence'].mean()
    print(f"Average Confidence: {avg_confidence:.3f}")
    print()

    # Breakdown by platform
    print("Classification by Platform:")
    for platform in results_df['source_platform'].unique():
        platform_df = results_df[results_df['source_platform'] == platform]
        platform_successful = platform_df[platform_df['classification'].isin(['SUITABLE', 'UNSUITABLE'])]

        if len(platform_successful) > 0:
            platform_suitable = len(platform_df[platform_df['classification'] == 'SUITABLE'])
            platform_total = len(platform_df)
            suitable_rate = platform_suitable / len(platform_successful) * 100

            print(f"  {platform}:")
            print(f"    Total: {platform_total}")
            print(f"    SUITABLE: {platform_suitable} ({suitable_rate:.1f}%)")
            print(f"    Avg Confidence: {platform_successful['confidence'].mean():.3f}")
    print()

    # Recovery phase distribution
    print("Recovery Phase Distribution:")
    phase_counts = results_df[results_df['classification'].isin(['SUITABLE', 'UNSUITABLE'])]['recovery_phase'].value_counts()
    for phase, count in phase_counts.items():
        print(f"  {phase}: {count} ({count/len(phase_counts)*100:.1f}%)")
    print()

    # SUITABLE rate by is_achilles_related
    print("SUITABLE Rate by is_achilles_related:")
    for achilles_value in [True, False]:
        achilles_df = results_df[results_df['is_achilles_related'] == achilles_value]
        achilles_successful = achilles_df[achilles_df['classification'].isin(['SUITABLE', 'UNSUITABLE'])]

        if len(achilles_successful) > 0:
            achilles_suitable = len(achilles_df[achilles_df['classification'] == 'SUITABLE'])
            suitable_rate = achilles_suitable / len(achilles_successful) * 100

            print(f"  is_achilles_related={achilles_value}:")
            print(f"    Total: {len(achilles_df)}")
            print(f"    SUITABLE: {achilles_suitable} ({suitable_rate:.1f}%)")
            print(f"    Avg Confidence: {achilles_successful['confidence'].mean():.3f}")

print()
print("=" * 80)
print("PILOT RUN COMPLETE")
print("=" * 80)
