import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV file
df = pd.read_csv("Scalable\\yelp\\code\\results (3).csv")

df["value"] = pd.to_numeric(df["value"], errors="coerce")

# Plot histogram
plt.figure(figsize=(8, 5))
plt.hist(df["value"], bins=20, color='skyblue', edgecolor='black')
plt.xlabel("Temporal Trends of Reviews")
plt.ylabel("Frequency")
plt.title("Distribution of Business Temporal Trends")
plt.show()


