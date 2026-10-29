"""
Warehouse connection helper for the source loader.

The source tables (raw_*, ads, sessions) are no longer loaded as dbt seeds.
Instead they are populated directly in the warehouse by a small Python loader,
so the lineage shows real `source()` nodes per upstream system.

To avoid re-implementing warehouse auth, connection details are read from the
same dbt `profiles.yml` the pipeline already uses (profile `jaffle_shop_online`),
honoring the selected target. Both password and key-pair (snowflake_jwt) auth
are supported, since those are the two the demo relies on.
"""

import os
from typing import Optional

import yaml

PROFILE_NAME = "jaffle_shop_online"

# Repo root is three levels up from this file (data_creation/source_loading/..).
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DBT_PROJECT_DIR = os.path.join(REPO_ROOT, "jaffle_shop_online")


def _find_profiles_path(profiles_dir: Optional[str]) -> str:
    candidates = []
    if profiles_dir:
        candidates.append(os.path.join(profiles_dir, "profiles.yml"))
    candidates.append(os.path.join(os.getcwd(), "profiles.yml"))
    candidates.append(os.path.join(DBT_PROJECT_DIR, "profiles.yml"))
    candidates.append(os.path.join(os.path.expanduser("~"), ".dbt", "profiles.yml"))

    for path in candidates:
        if os.path.isfile(path):
            return path
    raise FileNotFoundError(
        "Could not locate profiles.yml. Looked in: " + ", ".join(candidates)
    )


def _resolve_output(profiles_dir: Optional[str], target: Optional[str]) -> dict:
    path = _find_profiles_path(profiles_dir)
    with open(path) as profiles_file:
        profiles = yaml.safe_load(profiles_file)

    if PROFILE_NAME not in profiles:
        raise KeyError(f"Profile '{PROFILE_NAME}' not found in {path}")

    profile = profiles[PROFILE_NAME]
    outputs = profile.get("outputs", {})
    selected_target = target or profile.get("target")
    if selected_target not in outputs:
        raise KeyError(
            f"Target '{selected_target}' not found under profile '{PROFILE_NAME}' in {path}"
        )
    return outputs[selected_target]


def _private_key_der(private_key_path: str, passphrase: Optional[str]) -> bytes:
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    with open(private_key_path, "rb") as key_file:
        private_key = serialization.load_pem_private_key(
            key_file.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )
    return private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


class WarehouseConnection:
    """Thin context manager wrapping a snowflake connection + target schema/database."""

    def __init__(self, target: Optional[str] = None, profiles_dir: Optional[str] = None):
        self._config = _resolve_output(profiles_dir, target)
        if self._config.get("type") != "snowflake":
            raise ValueError(
                "The source loader only supports Snowflake targets; got "
                f"type='{self._config.get('type')}'."
            )
        self.database = self._config["database"]
        self.schema = self._config["schema"]
        self._connection = None

    def _connect_kwargs(self) -> dict:
        cfg = self._config
        kwargs = dict(
            account=cfg["account"],
            user=cfg["user"],
            role=cfg.get("role"),
            warehouse=cfg.get("warehouse"),
            database=cfg["database"],
            schema=cfg["schema"],
        )
        private_key_path = cfg.get("private_key_path")
        if cfg.get("authenticator") == "snowflake_jwt" or private_key_path:
            kwargs["private_key"] = _private_key_der(
                private_key_path, cfg.get("private_key_passphrase")
            )
        else:
            kwargs["password"] = cfg["password"]
        return {k: v for k, v in kwargs.items() if v is not None}

    def __enter__(self):
        import snowflake.connector

        self._connection = snowflake.connector.connect(**self._connect_kwargs())
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def cursor(self):
        if self._connection is None:
            raise RuntimeError("Connection is not open; use inside a `with` block.")
        return self._connection.cursor()
