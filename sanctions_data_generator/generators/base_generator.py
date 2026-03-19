"""
Base generator class providing common functionality for all data generators.

Handles:
  - Reproducible seeding (Faker, NumPy, stdlib random)
  - Surrogate key generation via SHA-256
  - Parquet output with optimal row-group sizing
  - Progress tracking with tqdm
"""

import hashlib
import random
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker
from tqdm import tqdm


class BaseGenerator(ABC):
    """Abstract base class for all synthetic data generators."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        self.fake = Faker()
        Faker.seed(seed)
        np.random.seed(seed)
        random.seed(seed)

    @abstractmethod
    def generate_batch(self, batch_size: int, **kwargs) -> pd.DataFrame:
        """Generate a batch of records. Must be implemented by subclasses."""

    @staticmethod
    def generate_surrogate_key(*args: Any) -> str:
        """Generate a deterministic surrogate key from input arguments."""
        key_string = "||".join(str(arg) for arg in args)
        return hashlib.sha256(key_string.encode()).hexdigest()[:32]

    def generate_to_parquet(
        self,
        output_dir: Path,
        total_records: int,
        batch_size: int = 1_000_000,
        file_prefix: str = "data",
        **kwargs: Any,
    ) -> List[Path]:
        """
        Generate data and write to Parquet files with optimal settings.

        Uses Snappy compression and configurable row-group sizes for
        efficient Snowflake COPY INTO ingestion.

        Args:
            output_dir: Directory to write Parquet files.
            total_records: Total number of records to generate.
            batch_size: Records per batch / file.
            file_prefix: Prefix for output file names.

        Returns:
            List of paths to generated Parquet files.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        files: List[Path] = []
        generated = 0
        file_idx = 0

        with tqdm(total=total_records, desc=f"Generating {file_prefix}") as pbar:
            while generated < total_records:
                current_batch = min(batch_size, total_records - generated)

                df = self.generate_batch(
                    batch_size=current_batch,
                    batch_offset=generated,
                    **kwargs,
                )

                file_path = output_dir / f"{file_prefix}_{file_idx:05d}.parquet"

                table = pa.Table.from_pandas(df, preserve_index=False)
                pq.write_table(
                    table,
                    file_path,
                    compression="snappy",
                    row_group_size=100_000,
                    use_dictionary=True,
                    write_statistics=True,
                )

                files.append(file_path)
                generated += len(df)
                file_idx += 1
                pbar.update(len(df))

        print(
            f"  Generated {generated:,} records in {len(files)} files "
            f"-> {output_dir}"
        )
        return files
