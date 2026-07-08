"""
Generate synthetic technical replicates for genomic_counts (batch-processed).

For every sample (sequenceuuid) in `genomic_counts`, this creates N synthesized
technical replicates. Each replicate:
  - gets its own new UUID (replicate_uuid)
  - keeps the original sample's sequenceuuid as a foreign key (parent_sequenceuuid)
  - has the same feature_id rows as the parent sample, with each count perturbed
    by random noise in the range [-noise_pct, +noise_pct] (default +/- 7%)
  - keeps the original row's row_id as a foreign key (source_row_id), so every
    synthesized row traces back to the exact genomic_counts row it came from

Source table schema (public.genomic_counts):
    row_id                   uuid   -- PK
    sequenceuuid              text   -- sample id (FK candidate -> genomic_metadata)
    feature_id                 text   -- DNA sequence / feature id
    count                       double precision

Output table (public.genomic_counts_technical_replicates):
    replicate_uuid            text  -- PK, new uuid per synthesized replicate-sample
    parent_sequenceuuid       text  -- FK -> genomic_counts.sequenceuuid (original sample)
    source_row_id              text  -- FK -> genomic_counts.row_id (original row)
    replicate_number           int   -- 1, 2, or 3
    feature_id                 text
    count                       double precision  -- noised count
    original_count              double precision  -- original count, kept for QC

BATCH PROCESSING
-----------------
genomic_counts is ~1M rows across ~20k samples. Processing the whole table in
memory at once is wasteful and risky for a long-running job, so this script:

  - Processes samples in batches, accumulating whole samples until a batch
    reaches roughly `--batch-size` rows (default 10,000). Samples are never
    split across batches, since each sample's rows must be grouped together
    to build consistent replicates.
  - Writes a JSON checkpoint file after every batch recording which samples
    have been completed. If the script is interrupted or fails, re-running it
    will pick up where it left off instead of starting over.
  - Logs progress every time cumulative rows processed crosses a multiple of
    `--log-every` rows (default 100,000), plus a summary line per batch.

Usage:
    export GENOMICS_DB_URL="postgresql://user:password@localhost:5432/nicome"
    python generate_technical_replicates.py

    # resume automatically picks up from the checkpoint file if one exists
    python generate_technical_replicates.py --checkpoint-file progress.json
"""

import argparse
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# Connection string is read from an environment variable so credentials never
# live in source code. Set this before running, e.g.:
#   export GENOMICS_DB_URL="postgresql://user:password@localhost:5432/nicome"
DB_URL_ENV_VAR = "GENOMICS_DB_URL"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("generate_technical_replicates")


# ---------------------------------------------------------------------------
# Core replicate generation (operates on an in-memory chunk of samples)
# ---------------------------------------------------------------------------

def create_technical_replicates(
    df: pd.DataFrame,
    n_replicates: int = 3,
    noise_pct: float = 0.07,
    sample_col: str = "sequenceuuid",
    count_col: str = "count",
    row_id_col: str = "row_id",
    random_seed: Optional[int] = None,
) -> pd.DataFrame:
    """Create synthesized technical replicates for each sample in df.

    Parameters
    ----------
    df : DataFrame with (at minimum) columns [sample_col, feature_id, count_col,
        row_id_col].
    n_replicates : number of synthesized technical replicates to create per sample.
    noise_pct : max fractional noise applied to each count, e.g. 0.07 means each
        count is scaled by a random factor drawn uniformly from
        [1 - noise_pct, 1 + noise_pct].
    sample_col : name of the column identifying a sample (the sample-level FK source).
    count_col : name of the column holding the raw counts to perturb.
    row_id_col : name of the column holding each source row's unique id
        (the row-level FK source).
    random_seed : optional seed for reproducibility.

    Returns
    -------
    DataFrame with columns:
        replicate_uuid, parent_sequenceuuid, source_row_id, replicate_number,
        feature_id, count, original_count
    """
    rng = np.random.default_rng(random_seed)

    replicate_frames = []

    for sample_id, sample_df in df.groupby(sample_col):
        n_rows = len(sample_df)

        for rep_number in range(1, n_replicates + 1):
            replicate_uuid = str(uuid.uuid4())

            # noise factor drawn independently per row, uniform in [-noise_pct, +noise_pct]
            noise_factors = rng.uniform(1 - noise_pct, 1 + noise_pct, size=n_rows)

            rep_df = pd.DataFrame({
                "replicate_uuid": replicate_uuid,
                "parent_sequenceuuid": sample_df[sample_col].values,
                "source_row_id": sample_df[row_id_col].astype(str).values,
                "replicate_number": rep_number,
                "feature_id": sample_df["feature_id"].values,
                "original_count": sample_df[count_col].values,
            })
            rep_df["count"] = (rep_df["original_count"] * noise_factors).round(0).clip(lower=0)

            replicate_frames.append(rep_df)

    result = pd.concat(replicate_frames, ignore_index=True)

    return result[[
        "replicate_uuid",
        "parent_sequenceuuid",
        "source_row_id",
        "replicate_number",
        "feature_id",
        "count",
        "original_count",
    ]]


# ---------------------------------------------------------------------------
# Batching helpers
# ---------------------------------------------------------------------------

def get_sample_row_counts(engine, source_table: str, sample_col: str = "sequenceuuid") -> pd.DataFrame:
    """Return one row per sample with its row count, ordered deterministically."""
    query = text(
        f"SELECT {sample_col} AS sample_id, count(*) AS n_rows "
        f"FROM {source_table} "
        f"GROUP BY {sample_col} "
        f"ORDER BY {sample_col}"
    )
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def build_batches(sample_row_counts: pd.DataFrame, batch_size: int) -> list:
    """Greedily group samples into batches of ~batch_size rows each.

    A sample's rows are never split across batches, so actual batch sizes
    will vary somewhat around batch_size depending on sample sizes.
    """
    batches = []
    current_batch = []
    current_rows = 0

    for _, row in sample_row_counts.iterrows():
        current_batch.append(row["sample_id"])
        current_rows += int(row["n_rows"])

        if current_rows >= batch_size:
            batches.append(current_batch)
            current_batch = []
            current_rows = 0

    if current_batch:
        batches.append(current_batch)

    return batches


def load_samples(engine, source_table: str, sample_ids: list, sample_col: str = "sequenceuuid") -> pd.DataFrame:
    query = text(f"SELECT * FROM {source_table} WHERE {sample_col} = ANY(:sample_ids)")
    with engine.connect() as conn:
        return pd.read_sql(query, conn, params={"sample_ids": list(sample_ids)})


def write_replicates_chunk(
    replicates_df: pd.DataFrame,
    engine,
    target_table: str,
    first_write: bool,
    if_exists_on_first_write: str = "replace",
) -> None:
    """Append a chunk of replicates to the target table.

    On the very first write (when no checkpoint exists yet), the table is
    replaced/created according to if_exists_on_first_write. Every subsequent
    write appends.
    """
    mode = if_exists_on_first_write if first_write else "append"
    replicates_df.to_sql(target_table, engine, if_exists=mode, index=False)


# ---------------------------------------------------------------------------
# Checkpointing
# ---------------------------------------------------------------------------

def load_checkpoint(checkpoint_path: Path) -> dict:
    if checkpoint_path.exists():
        with open(checkpoint_path, "r") as f:
            return json.load(f)
    return {"completed_samples": [], "rows_written": 0}


def save_checkpoint(checkpoint_path: Path, checkpoint: dict) -> None:
    # write to a temp file then rename, so a crash mid-write can't corrupt
    # the checkpoint file itself
    tmp_path = checkpoint_path.with_suffix(checkpoint_path.suffix + ".tmp")
    with open(tmp_path, "w") as f:
        json.dump(checkpoint, f)
    tmp_path.replace(checkpoint_path)


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------

def run(
    engine,
    source_table: str,
    target_table: str,
    checkpoint_path: Path,
    batch_size: int = 10_000,
    log_every: int = 100_000,
    n_replicates: int = 3,
    noise_pct: float = 0.07,
    seed: Optional[int] = None,
    if_exists_on_first_write: str = "replace",
) -> None:
    checkpoint = load_checkpoint(checkpoint_path)
    completed_samples = set(checkpoint["completed_samples"])
    total_rows_written = checkpoint["rows_written"]
    last_logged_milestone = (total_rows_written // log_every) * log_every

    if completed_samples:
        logger.info(
            "Resuming from checkpoint: %d samples already completed, %d rows already written.",
            len(completed_samples), total_rows_written,
        )

    sample_row_counts = get_sample_row_counts(engine, source_table)
    sample_row_counts = sample_row_counts[~sample_row_counts["sample_id"].isin(completed_samples)]

    if sample_row_counts.empty:
        logger.info("Nothing to do - all samples already processed according to checkpoint.")
        return

    batches = build_batches(sample_row_counts, batch_size)
    total_batches = len(batches)
    logger.info("Processing %d remaining samples in %d batches (~%d rows/batch).",
                len(sample_row_counts), total_batches, batch_size)

    # if we've written before (resuming), never truncate the target table
    first_write = total_rows_written == 0 and not completed_samples

    for batch_num, sample_ids in enumerate(batches, start=1):
        try:
            source_chunk = load_samples(engine, source_table, sample_ids)

            replicates_chunk = create_technical_replicates(
                source_chunk,
                n_replicates=n_replicates,
                noise_pct=noise_pct,
                random_seed=seed,
            )

            write_replicates_chunk(
                replicates_chunk, engine, target_table,
                first_write=first_write,
                if_exists_on_first_write=if_exists_on_first_write,
            )
            first_write = False

            # update + persist progress
            completed_samples.update(sample_ids)
            total_rows_written += len(replicates_chunk)
            checkpoint = {
                "completed_samples": list(completed_samples),
                "rows_written": total_rows_written,
            }
            save_checkpoint(checkpoint_path, checkpoint)

            logger.info(
                "Batch %d/%d done: %d samples, %d replicate rows written this batch "
                "(%d rows written total).",
                batch_num, total_batches, len(sample_ids), len(replicates_chunk), total_rows_written,
            )

            # milestone logging every `log_every` rows
            current_milestone = (total_rows_written // log_every) * log_every
            if current_milestone > last_logged_milestone:
                logger.info(">>> Milestone: %d total rows written to %s.", current_milestone, target_table)
                last_logged_milestone = current_milestone

        except Exception:
            logger.exception(
                "Batch %d/%d failed. Progress up to the last successful batch is saved in %s - "
                "re-run the script to resume from here.",
                batch_num, total_batches, checkpoint_path,
            )
            raise

    logger.info(
        "All done. %d total rows written to '%s' across %d samples.",
        total_rows_written, target_table, len(completed_samples),
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--n-replicates", type=int, default=3)
    parser.add_argument("--noise-pct", type=float, default=0.07)
    parser.add_argument("--source-table", default="genomic_counts")
    parser.add_argument("--target-table", default="genomic_counts_technical_replicates")
    parser.add_argument("--batch-size", type=int, default=10_000, help="Approx. rows per batch (default 10,000).")
    parser.add_argument("--log-every", type=int, default=100_000, help="Log a milestone every N rows written (default 100,000).")
    parser.add_argument("--checkpoint-file", default="replicate_progress.json")
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument(
        "--if-exists",
        default="replace",
        choices=["replace", "append", "fail"],
        help="Behavior for the target table on a *fresh* run with no checkpoint (default: replace). "
             "Ignored when resuming from an existing checkpoint (always appends).",
    )
    args = parser.parse_args()

    db_url = os.environ.get(DB_URL_ENV_VAR)
    if not db_url:
        raise SystemExit(
            f"Set the {DB_URL_ENV_VAR} environment variable to a full "
            f"postgresql:// connection string before running this script."
        )

    engine = create_engine(db_url)
    checkpoint_path = Path(args.checkpoint_file)

    run(
        engine=engine,
        source_table=args.source_table,
        target_table=args.target_table,
        checkpoint_path=checkpoint_path,
        batch_size=args.batch_size,
        log_every=args.log_every,
        n_replicates=args.n_replicates,
        noise_pct=args.noise_pct,
        seed=args.seed,
        if_exists_on_first_write=args.if_exists,
    )


if __name__ == "__main__":
    main()
