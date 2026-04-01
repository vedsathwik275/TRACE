import pandas as pd

# Read the CSV file
df = pd.read_csv('data/sentiment_results.csv')

# Print shape
print("Shape:", df.shape)
print()

# Print value counts for source_platform
print("Value counts for source_platform:")
print(df['source_platform'].value_counts())
print()

# Print value counts for is_achilles_related
print("Value counts for is_achilles_related:")
print(df['is_achilles_related'].value_counts())
print()

# Show 3 sample rows
print("3 sample rows:")
print(df.sample(3))
