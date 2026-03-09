# Model Persistence Guide

## Overview

Two new cells have been added to `popularity.ipynb` to save trained models:

- **Cell 18**: Saves XGBoost models (after training completes)
- **Cell 27**: Saves LSTM models (after training completes)

## File Locations

Models are saved to: `project/local/models/`

- `xgb_models.pkl` - All XGBoost classifiers and regressors
- `lstm_models.pt` - All LSTM models with state_dicts and metrics

## Usage

### First Run (Training from Scratch)

1. Run all cells up to and including the training cells
2. The save cells will automatically execute and create `.pkl` and `.pt` files
3. Training time saved for subsequent runs!

### Loading Pre-trained Models (Skip Training)

To skip training and load existing models:

**For XGBoost (Cell 18):**

```python
# Comment out the training loop cell (Cell 17)
# Uncomment these lines in Cell 18:
with open(xgb_path, "rb") as f:
    xgb_results = pickle.load(f)
    print(f"✓ Loaded {len(xgb_results)} XGBoost models from {xgb_path}")
```

**For LSTM (Cell 27):**

```python
# Comment out the training loop cell (Cell 25)
# Uncomment these lines in Cell 27:
lstm_save_data = torch.load(lstm_path, map_location=device)
lstm_results = {}
for key, data in lstm_save_data.items():
    model = PopularityLSTM(
        data["n_features"],
        data["hidden_size"],
        data["num_layers"],
        task=data["task"]
    ).to(device)
    model.load_state_dict(data["state_dict"])
    model.eval()
    lstm_results[key] = (model, data["metrics"], None)
print(f"✓ Loaded {len(lstm_results)} LSTM models from {lstm_path}")
```

## Data Quality Improvements

Recent changes to address sparse data issues:

### Date Filtering

- **What Changed**: Training data now filtered to **2026-01-01 onwards** (last ~2 months)
- **Why**: Original 20-year data span (2005-2026) caused:
  - Extremely sparse time series
  - Misleading growth rates (0→100 listens = artificially high percentile)
  - Poor prediction quality (~40% accuracy)
- **Location**: Data loading cell sets `MIN_DATE = "2026-01-01"`

### Minimum Observations Filter

- **What Changed**: Entities with too few data points are removed
  - Artists: minimum 3 observations
  - Tracks: minimum 5 observations
- **Why**: Prevents spurious predictions from entities with 1-2 data points
- **Location**: Added `filter_min_observations()` function before target generation

## Re-running with New Filters

After the data quality fixes, you should:

1. Delete old model files: `rm project/local/models/*.{pkl,pt}`
2. Run notebook from data loading cells forward
3. Verify improved metrics (PR-AUC gain, F1 scores)
4. Check "top 20 predictions" now have adequate observations

##Backup
Original notebook backed up to: `popularity.ipynb.backup`
