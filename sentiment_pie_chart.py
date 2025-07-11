import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV
df = pd.read_csv("results (7).csv")

df["value"] = pd.to_numeric(df["value"], errors="coerce")

# Categorize sentiment scores
def categorize_sentiment(score):
    if score < 0.4:
        return "Negative"
    elif score < 0.6:
        return "Neutral"
    else:
        return "Positive"

df["Sentiment Category"] = df["value"].apply(categorize_sentiment)

# Count each category
category_counts = df["Sentiment Category"].value_counts()

plt.figure(figsize=(6,6))
plt.pie(
    category_counts,
    labels=category_counts.index,
    autopct='%1.1f%%',
    startangle=140,
    colors=["#ff9999", "#ffc107", "#8bc34a"] 
)
plt.title("Sentiment Distribution of Customer Reviews")
plt.axis('equal') 
plt.show()
