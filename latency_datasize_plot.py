import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

data = [
    ['500MB', 'Parallel', 'Average rating', 7.32, 93897.38],
    ['500MB', 'Parallel', 'Keyword Analysis', 17.32, 39673.72],
    ['500MB', 'Parallel', 'Temporal Trends', 4.55, 150914.50],
    ['500MB', 'Parallel', 'Sentimental Analysis', 316.23, 2172.81],
    ['500MB', 'Sequential', 'Average rating', 1070.16, 642.05],
    ['500MB', 'Sequential', 'Keyword Analysis', 1070.79, 641.68],
    ['500MB', 'Sequential', 'Temporal Trends', 1072.33, 640.76],
    ['500MB', 'Sequential', 'Sentimental Analysis', 1070.74, 641.71],

    ['1024MB', 'Parallel', 'Average rating', 5.76, 244124.92],
    ['1024MB', 'Parallel', 'Keyword Analysis', 23.73, 59308.43],
    ['1024MB', 'Parallel', 'Temporal Trends', 5.02, 280471.68],
    ['1024MB', 'Parallel', 'Sentimental Analysis', 335.6, 4192.57],
    ['1024MB', 'Sequential', 'Average rating', 2184.92, 644.00],
    ['1024MB', 'Sequential', 'Keyword Analysis', 2185.72, 643.77],
    ['1024MB', 'Sequential', 'Temporal Trends', 2187.95, 643.11],
    ['1024MB', 'Sequential', 'Sentimental Analysis', 2185.66, 643.79],

    ['2048MB', 'Parallel', 'Average rating', 8.03, 350665.57],
    ['2048MB', 'Parallel', 'Keyword Analysis', 16.59, 169775.35],
    ['2048MB', 'Parallel', 'Temporal Trends', 5.31, 530099.50],
    ['2048MB', 'Parallel', 'Sentimental Analysis', 226.74, 12418.40],
    ['2048MB', 'Sequential', 'Average rating', 4421.49, 636.84],
    ['2048MB', 'Sequential', 'Keyword Analysis', 4421.28, 646.86],
    ['2048MB', 'Sequential', 'Temporal Trends', 4423.82, 636.50],
    ['2048MB', 'Sequential', 'Sentimental Analysis', 4421.49, 636.84],

    ['3072MB', 'Parallel', 'Average rating', 7.5, 562722.13],
    ['3072MB', 'Parallel', 'Keyword Analysis', 20.01, 211020.80],
    ['3072MB', 'Parallel', 'Temporal Trends', 5.5, 767348.36],
    ['3072MB', 'Parallel', 'Sentimental Analysis', 280.83, 16881.66],
    ['3072MB', 'Sequential', 'Average rating', 6574.08, 636.84],
    ['3072MB', 'Sequential', 'Keyword Analysis', 6574.18, 645.86],
    ['3072MB', 'Sequential', 'Temporal Trends', 6574.21, 645.50],
    ['3072MB', 'Sequential', 'Sentimental Analysis', 6574.19, 648.84],

    ['5000MB', 'Parallel', 'Average rating', 8.5, 827772.24],
    ['5000MB', 'Parallel', 'Keyword Analysis', 25.11, 281442.56],
    ['5000MB', 'Parallel', 'Temporal Trends', 6.32, 972677.33],
    ['5000MB', 'Parallel', 'Sentimental Analysis', 302.1, 23453.55],
    ['5000MB', 'Sequential', 'Average rating', 10956.2, 642.84],
    ['5000MB', 'Sequential', 'Keyword Analysis', 10956.3, 648.86],
    ['5000MB', 'Sequential', 'Temporal Trends', 10956.5, 651.55],
    ['5000MB', 'Sequential', 'Sentimental Analysis', 10957.8, 652.74],
]

# ---- Create DataFrame ----
df = pd.DataFrame(
    data,
    columns=['DataSize', 'Processing', 'Task', 'Latency', 'Throughput']
)

size_order = ['500MB', '1024MB', '2048MB', '3072MB', '5000MB']

# ---- Plot 1: Latency vs DataSize ----
sns.set(style="whitegrid", palette="viridis")

plt.figure(figsize=(10, 6))
sns.lineplot(
    data=df,
    x='DataSize',
    y='Latency',
    hue='Processing',
    style='Processing',
    markers=True,
    hue_order=['Parallel', 'Sequential'],
    linewidth=2.5,
    marker='o'
)
plt.yscale('log')
plt.title('Latency vs Data Size', fontsize=16)
plt.ylabel('Latency (ms, log scale)')
plt.xlabel('Data Size')
plt.tight_layout()
plt.show()

# ---- Plot 2: Throughput vs DataSize ----
plt.figure(figsize=(10, 6))
sns.lineplot(
    data=df,
    x='DataSize',
    y='Throughput',
    hue='Processing',
    style='Processing',
    markers=True,
    hue_order=['Parallel', 'Sequential'],
    linewidth=2.5,
    marker='o'
)
plt.yscale('log')
plt.title('Throughput vs Data Size', fontsize=16)
plt.ylabel('Throughput (records/sec, log scale)')
plt.xlabel('Data Size')
plt.tight_layout()
plt.show()

# ---- Plot 3: Barplot of Latency by Task & Processing ----
plt.figure(figsize=(12, 6))
sns.barplot(
    data=df,
    x='Task',
    y='Latency',
    hue='Processing',
    hue_order=['Parallel', 'Sequential'],
    ci=None,
    palette='viridis'
)
plt.yscale('log')
plt.title('Latency by Task and Processing Mode')
plt.ylabel('Latency (ms, log scale)')
plt.xlabel('Task')
plt.tight_layout()
plt.show()

# ---- Plot 4: Barplot of Throughput by Task & Processing ----
plt.figure(figsize=(12, 6))
sns.barplot(
    data=df,
    x='Task',
    y='Throughput',
    hue='Processing',
    hue_order=['Parallel', 'Sequential'],
    ci=None,
    palette='viridis'
)
plt.yscale('log')
plt.title('Throughput by Task and Processing Mode')
plt.ylabel('Throughput (records/sec, log scale)')
plt.xlabel('Task')
plt.tight_layout()
plt.show()
