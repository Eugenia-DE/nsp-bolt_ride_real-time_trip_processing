import pandas as pd

# Load the CSV files
trip_start_df = pd.read_csv(r"C:\Users\j\Desktop\nsp-bolt-ride\data\Project 7\data\trip_start.csv")
trip_end_df = pd.read_csv(r"C:\Users\j\Desktop\nsp-bolt-ride\data\Project 7\data\trip_end.csv")

# Print schema info for trip_start.csv
print("Schema for trip_start.csv:")
trip_start_df.info()

print("\nSchema for trip_end.csv:")
trip_end_df.info()
