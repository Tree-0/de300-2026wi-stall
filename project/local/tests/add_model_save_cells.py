#!/usr/bin/env python3
"""Add model persistence cells to popularity.ipynb"""

import json
from pathlib import Path

# Load notebook
nb_path = Path(__file__).parent / "popularity.ipynb"
with open(nb_path) as f:
    nb = json.load(f)

# Find the cells after XGBoost and LSTM training
# Look for cells containing the final print statement
xgb_idx = None
lstm_idx = None

for i, cell in enumerate(nb['cells']):
    if cell['cell_type'] == 'code' and 'source' in cell:
        src = ''.join(cell['source'])
        if 'print(f"\\nTrained {len(xgb_results)} XGBoost models.")' in src:
            xgb_idx = i
            print(f"Found XGBoost training cell at index {i}, id: {cell.get('id', 'N/A')}")
        if 'print(f"\\nTrained {len(lstm_results)} LSTM models.")' in src:
            lstm_idx = i
            print(f"Found LSTM training cell at index {i}, id: {cell.get('id', 'N/A')}")

if xgb_idx is None or lstm_idx is None:
    print("ERROR: Could not find training cells!")
    import sys
    sys.exit(1)

# XGBoost save cell
xgb_save_cell = {
    "cell_type": "code",
    "execution_count": None,
    "id": "xgb_save_models",
    "metadata": {},
    "outputs": [],
    "source": [
        "import pickle\n",
        "import os\n",
        "from pathlib import Path\n",
        "\n",
        "# Create models directory\n",
        "MODELS_DIR = Path(\"../models\")\n",
        "MODELS_DIR.mkdir(parents=True, exist_ok=True)\n",
        "\n",
        "# Save XGBoost models\n",
        "xgb_path = MODELS_DIR / \"xgb_models.pkl\"\n",
        "with open(xgb_path, \"wb\") as f:\n",
        "    pickle.dump(xgb_results, f)\n",
        "print(f\"✓ Saved {len(xgb_results)} XGBoost models to {xgb_path}\")\n",
        "\n",
        "# To load models later (skip training):\n",
        "# with open(xgb_path, \"rb\") as f:\n",
        "#     xgb_results = pickle.load(f)\n",
        "#     print(f\"✓ Loaded {len(xgb_results)} XGBoost models from {xgb_path}\")"
    ]
}

# LSTM save cell
lstm_save_cell = {
    "cell_type": "code",
    "execution_count": None,
    "id": "lstm_save_models",
    "metadata": {},
    "outputs": [],
    "source": [
        "# Save LSTM models (PyTorch state_dicts + metrics)\n",
       " lstm_path = MODELS_DIR / \"lstm_models.pt\"\n",
        "lstm_save_data = {}\n",
        "\n",
        "for key, (model, metrics, preds) in lstm_results.items():\n",
        "    if model is not None:\n",
        "        lstm_save_data[key] = {\n",
        "            \"state_dict\": model.state_dict(),\n",
        "            \"metrics\": metrics,\n",
        "            # Store architecture params for reconstruction\n",
        "            \"n_features\": model.lstm.input_size,\n",
        "            \"hidden_size\": model.lstm.hidden_size,\n",
        "            \"num_layers\": model.lstm.num_layers,\n",
        "            \"task\": model.task,\n",
        "        }\n",
        "\n",
        "torch.save(lstm_save_data, lstm_path)\n",
        "print(f\"✓ Saved {len(lstm_save_data)} LSTM models to {lstm_path}\")\n",
        "\n",
        "# To load models later (skip training):\n",
        "# lstm_save_data = torch.load(lstm_path, map_location=device)\n",
        "# lstm_results = {}\n",
        "# for key, data in lstm_save_data.items():\n",
        "#     model = PopularityLSTM(\n",
        "#         data[\"n_features\"],\n",
        "#         data[\"hidden_size\"],\n",
        "#         data[\"num_layers\"],\n",
        "#         task=data[\"task\"]\n",
        "#     ).to(device)\n",
        "#     model.load_state_dict(data[\"state_dict\"])\n",
        "#     model.eval()\n",
        "#     lstm_results[key] = (model, data[\"metrics\"], None)  # preds not saved\n",
        "# print(f\"✓ Loaded {len(lstm_results)} LSTM models from {lstm_path}\")"
    ]
}

# Insert cells
nb['cells'].insert(xgb_idx + 1, xgb_save_cell)
# lstm_idx now shifted by 1
nb['cells'].insert(lstm_idx + 2, lstm_save_cell)  

# Save backup
backup_path = nb_path.with_suffix('.ipynb.backup')
nb_path.rename(backup_path)
print(f"Backup saved to {backup_path}")

# Save modified notebook
with open(nb_path, 'w') as f:
    json.dump(nb, f, indent=1)

print(f"✓ Added 2 model save cells to {nb_path}")
print(f"  - XGBoost save cell inserted at index {xgb_idx + 1}")
print(f"  - LSTM save cell inserted at index {lstm_idx + 2}")
