import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load CSV
df = pd.read_csv("C:\\Users\\INTEL C\\Downloads\\results (8).csv")

df["value"] = pd.to_numeric(df["value"], errors="coerce")

# Sort keywords by frequency descending
df_sorted = df.sort_values("value", ascending=False)

# Plot
plt.figure(figsize=(10, 6))
sns.barplot(
    x="value",
    y="key",
    data=df_sorted,
    palette="viridis"
)

plt.title("Top Positive-Negative Keywords Frequency", fontsize=16, weight="bold")
plt.xlabel("Frequency", fontsize=12)
plt.ylabel("Keyword", fontsize=12)

# Add value labels on bars
for index, value in enumerate(df_sorted["value"]):
    plt.text(
        value + 1000,             
        index,                    
        f"{value:,}",             
        va='center',
        fontsize=10,
        color='black'
    )

plt.tight_layout()
plt.show()
