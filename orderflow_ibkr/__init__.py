"""IBKR realtime radar and executed order-flow engine."""

from pathlib import Path
import os

from dotenv import load_dotenv


# Load credentials from the repository-local .env when present. Existing
# process environment variables win, so explicit deployment configuration is
# never overwritten by a local file. Scan parent directories so a detached
# worktree can still use the repository root's .env during local development.
_env_candidates = [Path.cwd() / ".env", *(parent / ".env" for parent in Path(__file__).resolve().parents)]
for _env_path in _env_candidates:
    if _env_path.is_file():
        load_dotenv(_env_path, override=False)
        break

# Accept the longer ALPACA_* spelling as a convenience for existing local
# configuration while keeping the APCA_* names used by the Alpaca SDKs.
for _name in ("API_KEY_ID", "API_SECRET_KEY"):
    if not os.getenv(f"APCA_{_name}") and os.getenv(f"ALPACA_{_name}"):
        os.environ[f"APCA_{_name}"] = os.environ[f"ALPACA_{_name}"]

__all__ = ["__version__"]
__version__ = "0.1.0"
