import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

csv_dir = Path(__file__).parent

ext_files = sorted(csv_dir.glob('ext_*.csv'))

fig, axes = plt.subplots(3, 2, figsize=(15, 12))
axes = axes.flatten()

for idx, file_path in enumerate(ext_files):
    df = pd.read_csv(file_path)
    
    ticker = file_path.stem.replace('ext_', '')
    
    ax = axes[idx]
    ax.plot(pd.to_datetime(df['Date']), df['Open'])
    ax.set_title(f'{ticker} Open Prices')
    ax.set_xlabel('Date')
    ax.set_ylabel('Open Price')
    ax.grid(True, alpha=0.3)
    ax.tick_params(axis='x', rotation=45)

plt.tight_layout()
plt.show()