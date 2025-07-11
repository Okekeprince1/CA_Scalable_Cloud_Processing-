import matplotlib.pyplot as plt

data = [
    ['500MB', 'Average rating', 146.20],
    ['500MB', 'Keyword Analysis', 61.82],
    ['500MB', 'Temporal Trends', 235.68],
    ['500MB', 'Sentimental Analysis', 3.39],
    ['1024MB', 'Average rating', 379.33],
    ['1024MB', 'Keyword Analysis', 92.11],
    ['1024MB', 'Temporal Trends', 435.85],
    ['1024MB', 'Sentimental Analysis', 6.51],
    ['2048MB', 'Average rating', 550.62],
    ['2048MB', 'Keyword Analysis', 266.50],
    ['2048MB', 'Temporal Trends', 833.11],
    ['2048MB', 'Sentimental Analysis', 19.50],
    ['3072MB', 'Average rating', 876.54],
    ['3072MB', 'Keyword Analysis', 328.54],
    ['3072MB', 'Temporal Trends', 1195.31],
    ['3072MB', 'Sentimental Analysis', 23.41],
    ['5000MB', 'Average rating', 1288.96],
    ['5000MB', 'Keyword Analysis', 436.33],
    ['5000MB', 'Temporal Trends', 1733.62],
    ['5000MB', 'Sentimental Analysis', 36.27],
]

import numpy as np

sizes = sorted(list(set(row[0] for row in data)), key=lambda x: int(x.replace('MB', '')))
operations = sorted(list(set(row[1] for row in data)))

size_indices = {size: i for i, size in enumerate(sizes)}

# Prepare plot data
plot_data = {op: [None]*len(sizes) for op in operations}

for row in data:
    size, op, speedup = row
    idx = size_indices[size]
    plot_data[op][idx] = speedup

# Plot
plt.figure(figsize=(10, 6))

for op, speedups in plot_data.items():
    plt.plot(
        sizes,
        speedups,
        marker='o',
        label=op
    )

plt.yscale('log')
plt.xlabel('Data Size')
plt.ylabel('Speedup (Sequential / Parallel)')
plt.title('Speedup Curves for Different Operations')
plt.legend()
plt.grid(True, which="both", ls="--", linewidth=0.5)
plt.tight_layout()
plt.show()
