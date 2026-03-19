"""
Abstract base class for all data generators.

Provides common functionality: seeded randomness, surrogate key generation,
Parquet output with Snappy compression, and batch-level orchestration.
"""

import hashlib
import random
from abc import ABC, abstractmethod
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from faker import Faker


class BaseGenerator(ABC):
    """Abstract base for all domain-specific data generators."""

    def __init__(self, seed: int = 42):
        """
        Initialise generator with deterministic seed for reproducibility.

        Args:
            seed: Random seed for reproducible data generation.
        """
        self.seed = seed
        self.fake = Faker()
        Faker.seed(seed)
        np.random.seed(seed)
        random.seed(seed)

    @staticmethod
    def generate_surrogate_key(*args: str) -> str:
        """
        Generate a deterministic surrogate key using SHA-256 hash.

        Args:
            *args: Values to hash together.

        Returns:
            Hex digest of the SHA-256 hash (first 32 characters).
        """
        raw = "|".join(str(a) for a in args)
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]

    def generate_to_parquet(
        self,
        output_dir: Path | str,
        total_records: int,
        batch_size: int = 500_000,
        file_prefix: str = "data",
        **kwargs,
    ) -> list[Path]:
        """
        Generate data in batches and write to Parquet files.

        Optimised for Snowflake COPY INTO ingestion with Snappy compression
        and appropriate row-group sizing.

        Args:
            output_dir: Directory for output Parquet files.
            total_records: Total number of records to generate.
            batch_size: Records per batch / output file.
            file_prefix: Prefix for output file names.
            **kwargs: Additional arguments passed to generate_batch().

        Returns:
            List of Paths to generated Parquet files.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        files = []
        generated = 0
        file_index = 0

        while generated < total_records:
            current_batch = min(batch_size, total_records - generated)

            df = self.generate_batch(
                batch_size=current_batch,
                batch_offset=generated,
                **kwargs,
            )

            file_path = output_dir / f"{file_prefix}_{file_index:06d}.parquet"

            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(
                table,
                file_path,
                compression="snappy",
                row_group_size=min(100_000, current_batch),
                use_dictionary=True,
                write_statistics=True,
            )

            files.append(file_path)
            generated += current_batch
            file_index += 1

        return files

    @abstractmethod
    def generate_batch(self, batch_size: int, batch_offset: int = 0, **kwargs) -> pd.DataFrame:
        """
        Generate a batch of records.

        Args:
            batch_size: Number of records to generate.
            batch_offset: Offset for record numbering (for multi-batch runs).
            **kwargs: Domain-specific parameters.

        Returns:
            DataFrame containing the generated records.
        """
        ...
