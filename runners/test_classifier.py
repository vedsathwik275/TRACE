import pandas as pd
from gemini_classifier import classify_record
import json

# Load the dataset
print("Loading sentiment_results.csv...")
df = pd.read_csv('data/sentiment_results.csv')
print(f"Loaded {len(df)} rows\n")

# Select 5 sample rows
# Using a mix: some that are achilles_related=True and some False
achilles_true = df[df['is_achilles_related'] == True].sample(min(3, len(df[df['is_achilles_related'] == True])))
achilles_false = df[df['is_achilles_related'] == False].sample(min(2, len(df[df['is_achilles_related'] == False])))
test_rows = pd.concat([achilles_true, achilles_false])

print(f"Testing classifier on {len(test_rows)} rows...\n")
print("=" * 80)

# Process each row
results = []
for idx, (row_idx, row) in enumerate(test_rows.iterrows(), 1):
    print(f"\n[Row {idx}/{len(test_rows)}] - Index: {row_idx}")
    print(f"Platform: {row['source_platform']}")
    print(f"Is Achilles Related: {row['is_achilles_related']}")
    print(f"Text Preview: {str(row['text_content'])[:100]}...")
    print("\nClassifying...")

    try:
        result = classify_record(row)
        results.append({
            'row_index': row_idx,
            'original_is_achilles': row['is_achilles_related'],
            'classification_result': result
        })

        print(f"\n✓ Classification: {result['classification']}")
        print(f"  Confidence: {result['confidence']:.2f}")
        print(f"  Recovery Phase: {result['recovery_phase']}")
        print(f"  Reasoning: {result['reasoning']}")
        print(f"  Key Entities: {', '.join(result['key_entities'][:5])}")

        if 'error' in result:
            print(f"  ⚠ Error occurred: {result['error']}")

    except Exception as e:
        print(f"\n✗ Exception: {type(e).__name__}: {str(e)}")
        results.append({
            'row_index': row_idx,
            'original_is_achilles': row['is_achilles_related'],
            'classification_result': {'error': str(e)}
        })

    print("-" * 80)

# Summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

successful = [r for r in results if r['classification_result'].get('classification') not in ['ERROR', None]]
errors = [r for r in results if r['classification_result'].get('classification') == 'ERROR' or 'error' in r['classification_result']]

print(f"\nTotal processed: {len(results)}")
print(f"Successful: {len(successful)}")
print(f"Errors: {len(errors)}")

if successful:
    suitable = sum(1 for r in successful if r['classification_result']['classification'] == 'SUITABLE')
    unsuitable = sum(1 for r in successful if r['classification_result']['classification'] == 'UNSUITABLE')
    avg_confidence = sum(r['classification_result']['confidence'] for r in successful) / len(successful)

    print(f"\nClassification breakdown:")
    print(f"  SUITABLE: {suitable}")
    print(f"  UNSUITABLE: {unsuitable}")
    print(f"  Average confidence: {avg_confidence:.2f}")

# Save results to JSON
output_file = 'data/classifier_test_results.json'
with open(output_file, 'w') as f:
    json.dump(results, f, indent=2)
print(f"\nDetailed results saved to: {output_file}")
