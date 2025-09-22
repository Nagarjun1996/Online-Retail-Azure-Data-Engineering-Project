# ADF Pipeline — Excel to CSV (per sheet)

This template uses **Get Metadata** → **ForEach** over `structure` (sheets) → **Copy** to write each sheet as CSV to ADLS Gen2 bronze.

If Excel → Parquet fails due to mixed types, CSV first is more robust.

Parameters:
- source Excel path handled at dataset-level; update `DS_Source_Excel`.

Linked services:
- `LS_ADLS` (update account + auth).
