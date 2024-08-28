# Copyright (c) 2024 Your Company Name
# Licensed under the MIT License

"""Google Cloud Storage implementation of PipelineStorage."""

import logging
import os
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from google.cloud import storage
from google.oauth2 import service_account
from datashaper import Progress

from graphrag.index.progress import ProgressReporter

from .typing import PipelineStorage

log = logging.getLogger(__name__)


class GCSPipelineStorage(PipelineStorage):
    """The Google Cloud Storage implementation."""

    _bucket_name: str
    _path_prefix: str
    _encoding: str
    _client: storage.Client

    def __init__(
        self,
        bucket_name: str,
        encoding: str | None = None,
        path_prefix: str | None = None,
    ):
        """Create a new GCSStorage instance."""
        # Check for GCS credentials in environment variables
        if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
            self._client = storage.Client()
        elif all(key in os.environ for key in ['GCS_PROJECT_ID', 'GCS_PRIVATE_KEY_ID', 'GCS_PRIVATE_KEY', 'GCS_CLIENT_EMAIL']):
            # Create credentials from environment variables
            credentials_dict = {
                "type": "service_account",
                "project_id": os.environ['GCS_PROJECT_ID'],
                "private_key_id": os.environ['GCS_PRIVATE_KEY_ID'],
                "private_key": os.environ['GCS_PRIVATE_KEY'].replace('\\n', '\n'),
                "client_email": os.environ['GCS_CLIENT_EMAIL'],
                "client_id": "",
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{os.environ['GCS_CLIENT_EMAIL']}"
            }
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            self._client = storage.Client(credentials=credentials, project=os.environ['GCS_PROJECT_ID'])
        else:
            raise ValueError("GCS credentials not found in environment variables")

        self._encoding = encoding or "utf-8"
        self._bucket_name = bucket_name
        self._path_prefix = path_prefix or ""
        log.info(
            "creating GCS storage at bucket=%s, path=%s",
            self._bucket_name,
            self._path_prefix,
        )
        self.create_bucket()

    def create_bucket(self) -> None:
        """Create the bucket if it does not exist."""
        if not self.bucket_exists():
            self._client.create_bucket(self._bucket_name)

    def delete_bucket(self) -> None:
        """Delete the bucket."""
        if self.bucket_exists():
            bucket = self._client.get_bucket(self._bucket_name)
            bucket.delete()

    def bucket_exists(self) -> bool:
        """Check if the bucket exists."""
        return storage.Bucket(self._client, name=self._bucket_name).exists()

    def find(
        self,
        file_pattern: re.Pattern[str],
        base_dir: str | None = None,
        progress: ProgressReporter | None = None,
        file_filter: dict[str, Any] | None = None,
        max_count=-1,
    ) -> Iterator[tuple[str, dict[str, Any]]]:
        """Find blobs in a bucket using a file pattern, as well as a custom filter function."""
        base_dir = base_dir or ""

        log.info(
            "search bucket %s for files matching %s",
            self._bucket_name,
            file_pattern.pattern,
        )

        def blobname(blob_name: str) -> str:
            if blob_name.startswith(self._path_prefix):
                blob_name = blob_name.replace(self._path_prefix, "", 1)
            if blob_name.startswith("/"):
                blob_name = blob_name[1:]
            return blob_name

        def item_filter(item: dict[str, Any]) -> bool:
            if file_filter is None:
                return True

            return all(re.match(value, item[key]) for key, value in file_filter.items())

        try:
            bucket = self._client.get_bucket(self._bucket_name)
            all_blobs = list(bucket.list_blobs(prefix=base_dir))

            num_loaded = 0
            num_total = len(all_blobs)
            num_filtered = 0
            for blob in all_blobs:
                match = file_pattern.match(blob.name)
                if match and blob.name.startswith(base_dir):
                    group = match.groupdict()
                    if item_filter(group):
                        yield (blobname(blob.name), group)
                        num_loaded += 1
                        if max_count > 0 and num_loaded >= max_count:
                            break
                    else:
                        num_filtered += 1
                else:
                    num_filtered += 1
                if progress is not None:
                    progress(
                        _create_progress_status(num_loaded, num_filtered, num_total)
                    )
        except Exception:
            log.exception(
                "Error finding blobs: base_dir=%s, file_pattern=%s, file_filter=%s",
                base_dir,
                file_pattern,
                file_filter,
            )
            raise

    async def get(
        self, key: str, as_bytes: bool | None = False, encoding: str | None = None
    ) -> Any:
        """Get a value from the cache."""
        try:
            key = self._keyname(key)
            bucket = self._client.get_bucket(self._bucket_name)
            blob = bucket.blob(key)
            blob_data = blob.download_as_bytes()
            if not as_bytes:
                coding = encoding or "utf-8"
                blob_data = blob_data.decode(coding)
        except Exception:
            log.exception("Error getting key %s", key)
            return None
        else:
            return blob_data

    async def set(self, key: str, value: Any, encoding: str | None = None) -> None:
        """Set a value in the cache."""
        try:
            key = self._keyname(key)
            bucket = self._client.get_bucket(self._bucket_name)
            blob = bucket.blob(key)
            if isinstance(value, bytes):
                blob.upload_from_string(value)
            else:
                coding = encoding or "utf-8"
                blob.upload_from_string(value.encode(coding))
        except Exception:
            log.exception("Error setting key %s", key)

    def set_df_json(self, key: str, dataframe: Any) -> None:
        """Set a json dataframe."""
        key = self._keyname(key)
        bucket = self._client.get_bucket(self._bucket_name)
        blob = bucket.blob(key)
        blob.upload_from_string(
            dataframe.to_json(orient="records", lines=True, force_ascii=False)
        )

    def set_df_parquet(self, key: str, dataframe: Any) -> None:
        """Set a parquet dataframe."""
        key = self._keyname(key)
        bucket = self._client.get_bucket(self._bucket_name)
        blob = bucket.blob(key)
        blob.upload_from_string(dataframe.to_parquet())

    async def has(self, key: str) -> bool:
        """Check if a key exists in the cache."""
        key = self._keyname(key)
        bucket = self._client.get_bucket(self._bucket_name)
        blob = bucket.blob(key)
        return blob.exists()

    async def delete(self, key: str) -> None:
        """Delete a key from the cache."""
        key = self._keyname(key)
        bucket = self._client.get_bucket(self._bucket_name)
        blob = bucket.blob(key)
        blob.delete()

    async def clear(self) -> None:
        """Clear the cache."""
        # In GCS, we don't have a direct "clear" operation.
        # We can either delete all objects or create a new bucket.
        # Here, we'll delete all objects.
        bucket = self._client.get_bucket(self._bucket_name)
        blobs = bucket.list_blobs(prefix=self._path_prefix)
        for blob in blobs:
            blob.delete()

    def child(self, name: str | None) -> "PipelineStorage":
        """Create a child storage instance."""
        if name is None:
            return self
        path = str(Path(self._path_prefix) / name)
        return GCSPipelineStorage(
            self._bucket_name,
            self._encoding,
            path,
        )

    def _keyname(self, key: str) -> str:
        """Get the key name."""
        return str(Path(self._path_prefix) / key)


def create_gcs_storage(
    bucket_name: str,
    base_dir: str | None,
) -> PipelineStorage:
    """Create a GCS based storage."""
    log.info("Creating GCS storage at %s", bucket_name)
    if bucket_name is None:
        msg = "No bucket name provided for GCS storage."
        raise ValueError(msg)
    return GCSPipelineStorage(
        bucket_name,
        path_prefix=base_dir,
    )


def validate_gcs_bucket_name(bucket_name: str):
    """
    Check if the provided GCS bucket name is valid based on Google Cloud rules.

        - Bucket names must contain only lowercase letters, numbers, dashes (-), underscores (_), and dots (.).
        - Bucket names must start and end with a number or letter.
        - Bucket names must contain 3-63 characters.
        - Bucket names cannot be represented as an IP address in dotted-decimal notation.
        - Bucket names cannot begin with the prefix "goog".
        - Bucket names cannot contain "google" or close misspellings, such as "g00gle".

    Args:
    -----
    bucket_name (str)
        The GCS bucket name to be validated.

    Returns
    -------
        bool: True if valid, False otherwise.
    """
    if len(bucket_name) < 3 or len(bucket_name) > 63:
        return ValueError(
            f"Bucket name must be between 3 and 63 characters in length. Name provided was {len(bucket_name)} characters long."
        )

    if not re.match("^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$", bucket_name):
        return ValueError(
            f"Bucket name must contain only lowercase letters, numbers, dashes (-), underscores (_), and dots (.). It must start and end with a number or letter. Name provided was {bucket_name}."
        )

    if re.match(r"\d+\.\d+\.\d+\.\d+", bucket_name):
        return ValueError(
            f"Bucket name cannot be represented as an IP address in dotted-decimal notation. Name provided was {bucket_name}."
        )

    if bucket_name.startswith("goog"):
        return ValueError(
            f'Bucket name cannot begin with the prefix "goog". Name provided was {bucket_name}.'
        )

    if "google" in bucket_name.lower() or any(
        misspelling in bucket_name.lower()
        for misspelling in ["g00gle", "go0gle", "g0ogle"]
    ):
        return ValueError(
            f'Bucket name cannot contain "google" or close misspellings. Name provided was {bucket_name}.'
        )

    return True


def _create_progress_status(
    num_loaded: int, num_filtered: int, num_total: int
) -> Progress:
    return Progress(
        total_items=num_total,
        completed_items=num_loaded + num_filtered,
        description=f"{num_loaded} files loaded ({num_filtered} filtered)",
    )