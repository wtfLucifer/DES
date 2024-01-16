from pyspark.sql import SparkSession
from itertools import combinations

# Create Spark session
spark = SparkSession.builder.appName("AppInsights").getOrCreate()

# Load data from the provided datasheet (assuming it's a CSV file)
data = spark.read.csv("google_playstore.csv", header=True)

# Selecting relevant columns for insights (modify this based on relevance)
relevant_columns = [
    "free", "genre", "releasedYear", "price", "adSupported", "ratings", "minInstalls"
]
selected_data = data.select(relevant_columns)

# Binning numerical fields
def create_bins(df, column, num_bins):
    # Your binning logic here based on column and desired number of bins
    # ...
    return df.withColumn(column, ...)  # Update the column with binned values

# Assuming 'releasedYear', 'price', 'ratings', 'minInstalls' need binning
bins = {
    "releasedYear": 5,  # Create bins of 5-year ranges
    "price": 4,  # Define bins for price
    "ratings": 5,  # Define bins for ratings
    "minInstalls": 6  # Define bins for installs
}

for col_name, num_bins in bins.items():
    selected_data = create_bins(selected_data, col_name, num_bins)

# Generate combinations of selected columns
insights = {}
for r in range(1, len(relevant_columns) + 1):
    for subset in combinations(relevant_columns, r):
        # Perform count for each combination
        group_by_columns = list(subset)
        counts = (
            selected_data.groupBy(*group_by_columns)
            .count()
            .withColumnRenamed("count", "count_apps")
        )
        insights[subset] = counts

# Filter out combinations smaller than 2% of total volume
total_apps = selected_data.count()
filtered_insights = {k: v for k, v in insights.items() if v.count() / total_apps > 0.02}

# Write the insights into a CSV file
for subset, count_df in filtered_insights.items():
    file_name = "_".join(subset) + ".csv"
    count_df.write.csv(file_name, sep=";", header=True)
