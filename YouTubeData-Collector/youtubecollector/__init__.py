from pathlib import Path

FP_PKG = Path(__file__).parent
FP_REPO = FP_PKG.parent
FP_DATA = FP_REPO / "data"
FP_DATA_RAW = FP_DATA / "raw"
FP_DATA_PROC = FP_DATA / "proc"
FP_DATA_TMP = FP_DATA / "tmp"
FP_LOGS = FP_REPO / 'logs'
FP_FIGS = FP_REPO / 'figs'
fps = [FP_DATA_PROC, FP_DATA_RAW, FP_LOGS, FP_FIGS, FP_DATA_TMP]
for fp in fps:
    Path.mkdir(fp, exist_ok=True)
