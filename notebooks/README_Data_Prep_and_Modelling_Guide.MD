# Data Preprocessing and Modelling: Implementation Guide
This README provides a step-by-step breakdown of the data preprocessing and model training pipeline used in the COMP1884 project.

---

## Step 0 – Time-Based Dataset Splitting

Split the historical dataset into train/validation/test sets, ensuring temporal integrity:

* **Training**: 1 Feb 2017 – 31 Jan 2025
* **Validation**: 1 Feb 2025 – 31 Mar 2025
* **Test**: 1 Apr 2025 – 30 Apr 2025 (optional)

**Sub-steps:**

```python
# 0.1 Confirm datetime index and sort
# 0.2 Define chronological splits
# 0.3 Subset dataframe with .loc[]
# 0.4 Apply .asfreq('h') to enforce hourly frequency
# 0.5 Do not convert timezones (already in Europe/London)
```

---

## Step 1 – Outlier Masking with Preliminary Isolation Forest

Detect anomalies in training data before feeding to LSTM-AE.

**Sub-steps:**

```python
# 1.1 Select features: temperature_2m, surface_pressure, wind_speed_10m, precipitation
# 1.2 Apply 60-day rolling z-score to temperature & pressure (window=1440)
# 1.3 Apply 60-day rolling IQR to wind speed (no smoothing)
# 1.4 Apply log1p to precipitation, then 12-hour rolling z-score
# 1.5 Subset transformed features, dropna()
# 1.6 Train IsolationForest(n_estimators=100, contamination=0.01)
# 1.7 Assign is_anomaly = score > 97th percentile
# 1.8 Join is_anomaly mask back to df_train
```

---

## Step 2 – Feature Transformation and Normalisation

Tailor input transformations to each model type.

**Isolation Forest Inputs:**

```python
# temperature_2m_z: 60-day rolling z-score
# surface_pressure_z: 60-day rolling z-score
# wind_r: 3-hour smoothing + 60-day rolling IQR
# precip_z_12h: log1p + 12-hour rolling z-score
```

**LSTM-AE Inputs:**

```python
# Raw (temp, pressure, wind) → per-sequence median + IQR scaling
# Precipitation → log1p only (no further scaling)
# If log1p proves unstable, try global z-score as fallback
```

---

## Step 3 – Time-Based Feature Engineering

Add cyclical encodings of time for LSTM-AE only:

```python
# 3.1 Extract hour → hour_sin, hour_cos
# 3.2 Extract month → month_sin, month_cos
# 3.3 Do not include these in Isolation Forest inputs
```

---

## Step 4 – LSTM Sequence Construction

Generate 30-day clean sequences from non-anomalous training data:

```python
# 4.1 Filter df_train where is_anomaly == 0
# 4.2 Enforce hourly frequency with .asfreq('h')
# 4.3 Use window=720, stride=1 or 24
# 4.4 Loop through and construct clean, null-free windows
# 4.5 Stack into tensor (n_sequences, 720, n_features)
```

---

## Step 5 – Validation Sequence Generation

Repeat Step 4 on validation set:

```python
# 5.1 Apply log1p and model-specific transformations to df_val
# 5.2 Reapply Isolation Forest mask to df_val
# 5.3 Generate validation sequences using same logic as training
```

---

## Step 6 – Per-Sequence Scaling (LSTM-AE Only)

```python
# 6.1 For each sequence: apply robust scaling (median + IQR)
# 6.2 Skip scaling on log1p(precipitation) due to sparsity
```

---

## Step 7 – Train the LSTM Autoencoder

```python
# 7.1 Build symmetric encoder-decoder with RepeatVector
# 7.2 Compile with Adam optimiser and MAE loss
# 7.3 Train with early stopping (patience=5)
# 7.4 Save model as lstm_autoencoder.h5
```

---

## Step 8 – Train Final Isolation Forest

```python
# 8.1 Use full historical data (masked), apply all preprocessing
# 8.2 Train IsolationForest(n_estimators=100, contamination=0.01, bootstrap=True)
# 8.3 Save model as if_model.joblib
```

---

## Step 9 – Evaluate and Threshold Anomaly Scores

```python
# 9.1 For LSTM-AE: score test sequences by mean reconstruction error
#     → Label as anomaly if error > 95th percentile
# 9.2 For IF: score and label based on 97th percentile of anomaly scores
# 9.3 Save both sets of scores to CSV
```

---

## Step 10 – Save All Outputs

```python
# Save models: models/if_model.joblib, models/lstm_autoencoder.h5
# Save outputs: if_scores.csv, lstm_scores.csv
```

---

## References

Sources include: Trinh (2022), Antwarg et al. (2021), Kulkarni (2024), Tawalkuli (2024), Darban (2024), Yang (2023), Bâra et al. (2024), Liao et al. (2022)

For detailed justifications and citations, refer to the full COMP1884 data prep notes.
