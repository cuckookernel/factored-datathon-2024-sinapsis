"""Utiltities for data exploration"""
from matplotlib.axis import Axis
from ydata_profiling import ProfileReport  # type: ignore [import-untyped]

from data_proc.common import GdeltV1Type, gdelt_base_data_path, suffix_for_typ
from shared import DataFrame, Path, Series, date, logging, np, pd, re, runpyfile

L = logging.getLogger("x-util")
# %%

def _interactive_testing() -> None:
    # %%
    runpyfile("exploration/utils.py")

    # %%
    typ: GdeltV1Type = "events"
    rel_dir = Path("last_1y_events")
    start_date = date(2024, 5, 13)
    end_date = date(2024, 8, 13)
    fraction = 0.01
    gen_ydata_profiling(typ, rel_dir, start_date, end_date, fraction)
    # %%
    typ = "gkgcounts"
    rel_dir = Path("last_1y_gkgcounts")
    start_date = date(2024, 2, 13)
    end_date = date(2024, 8, 13)
    fraction = 0.1
    gen_ydata_profiling(typ, rel_dir, start_date, end_date, fraction)
    # %%
    typ = "gkg"
    rel_dir = Path("last_1y_gkg")
    start_date = date(2024, 5, 13)
    end_date = date(2024, 8, 13)
    fraction = 0.01
    gen_ydata_profiling(typ, rel_dir, start_date, end_date, fraction)
    # %%

def gen_ydata_profiling(typ: GdeltV1Type, rel_dir: Path,
                        start_date: date, end_date: date,
                        fraction: float) -> None:
    """Get a sample of data of the given type and produce a report using ydata_profiling lib"""
    # %%
    sample_df = sample_data(typ, rel_dir=rel_dir,
                            start_date=start_date, end_date=end_date,
                            fraction = fraction)
    # Ad hoc fixes
    if typ == "gkgcounts":
        sample_df["log_reported_count"] = np.log(sample_df["reported_count"])
        del sample_df["reported_count"]

    if typ in ["gkg", "gkgcounts"]:
        sample_df = sample_df.drop(["source_urls", "event_ids"], axis=1)

    # %%
    report_title = (f"Events profiling report {fraction * 100:.1}%  of records of type `{typ}` "
                    f"for date range: {start_date} - {end_date}")

    profile = ProfileReport(sample_df, title=report_title)
    # %%
    report_path = (gdelt_base_data_path()
                   / f"ydata-profile-{typ}-{start_date}-{end_date}-frac-{fraction}.html")

    with report_path.open("wt") as f_out:
        f_out.write(profile.html)
        L.info(f"Wrote report to: {report_path}")
# %%


def sample_data(typ: GdeltV1Type, *, rel_dir: Path,
                start_date: date, end_date: date, fraction: float) -> DataFrame:
    """Find parquet files under `rel_dir` directory (assumed to be under $DATA_DIR/GDELT)"""
    full_path = gdelt_base_data_path() / rel_dir / 'raw_parquet'
    suffix  = f".{suffix_for_typ(typ)}."
    typ_parquets = sorted([ p for p in full_path.glob("*.parquet") if suffix in str(p)])

    L.info(f"{len(typ_parquets)} parquets for typ:`{typ}` found under: `{full_path}`")
    if len(typ_parquets) == 0:
        total_files = list(full_path.glob("*.parquet"))
        raise RuntimeError(f"No parquets? full_path={full_path}\n"
                           f"num total parquet: {len(total_files)}")

    ret_dfs = []
    for path in typ_parquets:
        dates = extract_dates(path.name)
        if any(start_date <= d <= end_date for d in dates):
            data_df = pd.read_parquet(path)
            sample = data_df.sample(frac=fraction)
            ret_dfs.append(sample)

    row_counts = [df.shape[0] for df in ret_dfs]
    L.info(f"Loaded {len(ret_dfs)} parquets within time range ({start_date} - {end_date}) "
           f" - row_counts = {row_counts}")

    return pd.concat(ret_dfs)
# %%



# %%
def extract_dates(a_str: str) -> list[date]:
    """Extract dates from string in either YYYYMMDD or YYYY-MM-DD formats.

    Might return empty list, if no dates found!
    """
    ret: list[date] = []
    for piece in re.split('[-.]', a_str):
        tups = re.findall("(?P<year>[1-9][0-9]{3})-?(?P<month>[0-9]{2})-?(?P<day>[0-9]{2})", piece)
        ret.extend(date(*[int(el) for el in tup]) for tup in tups)

    return ret
# %%

def plot_null_pcts(data_df: DataFrame) -> tuple[DataFrame, Axis]:
    """Return data frame and bar plot of pct of nulls for each column"""
    n_rows = data_df.shape[0]
    null_pcts = pd.DataFrame(data_df.isna().sum() / n_rows * 100).reset_index()
    null_pcts.columns = ['column', 'pct_nulls'] # type: ignore [assignment]
    null_pcts = null_pcts.sort_values('pct_nulls', ascending=False)  # type: ignore [assignment]

    ax = null_pcts.plot(y="pct_nulls", kind="barh", figsize=(3, 15))
    ax.set_yticklabels(null_pcts["column"])

    return null_pcts, ax


def top_frequent(series: Series, top: int | None = None,
                 total_pct: float | None = None) -> DataFrame:
    """Return top most frequent, limiting by number or by total relative frequency"""
    total_n = len(series)
    val_pct_series = (series.fillna('__NULL__')
                      .value_counts(dropna=False).sort_values(ascending=False) / total_n) * 100

    val_pct = pd.DataFrame(val_pct_series).reset_index()
    n_unique = val_pct.shape[0]
    val_pct.columns = ['value', 'pct'] # type: ignore [assignment]

    val_pct['cum_pct'] = val_pct['pct'].cumsum()

    if total_pct is not None:
        cut = np.where(val_pct['cum_pct'] >= total_pct)
        idx = cut[0][0]
    elif top is not None:
        idx = min(top, n_unique) - 1
    else:
        raise ValueError("Need to provide either top or total_pct argument")

    other_vals_label = f"*OTHER* ({n_unique - idx - 1} distinct values)"
    other_vals_pct = val_pct['pct'].iloc[idx + 1:].sum()

    last_row = pd.DataFrame([[other_vals_label, other_vals_pct]], columns=['value', 'pct'])

    return pd.concat([val_pct.iloc[:idx + 1][['value', 'pct']], last_row])
