import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV file
df = pd.read_csv("Projects\\Scalable\\yelp\\code\\results (1).csv")

df["value"] = pd.to_numeric(df["value"], errors="coerce")

# Plot histogram
plt.figure(figsize=(8, 5))
plt.hist(df["value"], bins=20, color='skyblue', edgecolor='black')
plt.xlabel("Average Rating of Reviews")
plt.ylabel("Frequency")
plt.title("Distribution of Business Average Ratings")
plt.show()


