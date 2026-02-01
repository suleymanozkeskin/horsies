"""
Module import utilities.

This module provides explicit, deterministic import behavior:
- import_module_path(): Import using dotted module path (recommended)
- import_file_path(): Import from file path with explicit sys.path setup
- find_project_root(): Find pyproject.toml location for convenience

No implicit heuristics - the caller controls sys.path and module naming.
"""

from __future__ import annotations

import hashlib
import importlib
import importlib.util
import os
import sys
from typing import Any

from horsies.core.logging import get_logger

logger = get_logger("imports")


def find_project_root(start_dir: str) -> str | None:
    """
    Check if start_dir itself contains pyproject.toml, setup.cfg, or setup.py.

    NOTE: Does NOT traverse up - only checks the given directory.
    This avoids dangerous behavior in monorepos where parent pyproject.toml
    could cause module collisions between services.

    Returns start_dir if it contains a marker file, None otherwise.
    """
    start_dir = os.path.abspath(start_dir)
    for marker in ("pyproject.toml", "setup.cfg", "setup.py"):
        if os.path.exists(os.path.join(start_dir, marker)):
            return start_dir
    return None


def setup_sys_path_from_cwd() -> str | None:
    """
    If cwd contains pyproject.toml (not parent dirs), add cwd to sys.path.

    This is safe because:
    - Only checks cwd, not parent directories
    - User explicitly chose to run from this directory
    - No risk of monorepo root collision

    Returns cwd if it was added to sys.path, None otherwise.
    """
    cwd = os.getcwd()
    if find_project_root(cwd) and cwd not in sys.path:
        sys.path.insert(0, cwd)
        logger.debug(f"Added cwd to sys.path: {cwd}")
        return cwd
    return None


def import_module_path(module_path: str) -> Any:
    """
    Import a module using its dotted path.

    This is the recommended approach - like Celery, we expect the user to:
    1. Provide a valid dotted module path
    2. Have their PYTHONPATH/sys.path set up correctly

    Args:
        module_path: Dotted module path (e.g., "app.configs.horsies")

    Returns:
        The imported module

    Raises:
        ModuleNotFoundError: If the module cannot be found
    """
    return importlib.import_module(module_path)


def _compute_synthetic_module_name(path: str) -> str:
    """
    Generate stable synthetic module name for standalone files.

    Uses sha256 hash of realpath to ensure:
    - Same realpath → same hash → same module name (across processes)
    - No collision with user code (under horsies._dynamic namespace)
    - Deterministic (not random)
    """
    realpath = os.path.realpath(path)
    hash_prefix = hashlib.sha256(realpath.encode()).hexdigest()[:12]
    return f"horsies._dynamic.{hash_prefix}"


def import_file_path(
    file_path: str,
    module_name: str | None = None,
    add_parent_to_path: bool = True,
) -> Any:
    """
    Import a module from a file path.

    This is explicit and simple:
    1. Optionally add parent directory to sys.path
    2. Load module with given name (or synthetic name if not provided)
    3. Execute and return module

    Args:
        file_path: Path to the Python file
        module_name: Module name to use (default: synthetic name based on path hash)
        add_parent_to_path: Whether to add the file's parent directory to sys.path

    Returns:
        The imported module

    Raises:
        FileNotFoundError: If the file doesn't exist
        ImportError: If the module can't be loaded
    """
    file_path = os.path.realpath(file_path)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Module file not found: {file_path}")

    # Check if already loaded
    for name, mod in list(sys.modules.items()):
        mod_file = getattr(mod, "__file__", None)
        if mod_file and os.path.realpath(mod_file) == file_path:
            return mod

    # Add parent directory to sys.path if requested
    if add_parent_to_path:
        parent_dir = os.path.dirname(file_path)
        if parent_dir not in sys.path:
            sys.path.insert(0, parent_dir)

    # Use provided name or generate synthetic name
    if module_name is None:
        module_name = _compute_synthetic_module_name(file_path)

    # Load the module
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load module from path: {file_path}")

    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod

    # Also register under basename for compatibility
    basename = os.path.splitext(os.path.basename(file_path))[0]
    if basename != module_name and basename not in sys.modules:
        sys.modules[basename] = mod

    spec.loader.exec_module(mod)
    return mod


# Legacy alias for backward compatibility
def import_by_path(path: str, module_name: str | None = None) -> Any:
    """
    Legacy function - prefer import_file_path() or import_module_path().

    For file paths: delegates to import_file_path()
    For module paths: delegates to import_module_path()
    """
    if path.endswith(".py") or os.path.sep in path:
        return import_file_path(path, module_name)
    else:
        return import_module_path(path)


# Keep this for tests that import it directly
def compute_package_path_from_fs(file_path: str) -> tuple[str | None, str | None]:
    """
    Walk up directory tree looking for __init__.py to determine package structure.

    NOTE: This function is kept for backward compatibility with tests.
    New code should not rely on this for import resolution.

    Returns (dotted_module_name, package_root) if a package chain is found,
    otherwise (None, None).
    """
    file_path = os.path.realpath(file_path)
    module_name = os.path.splitext(os.path.basename(file_path))[0]
    current_dir = os.path.dirname(file_path)

    components = [module_name]
    while True:
        init_path = os.path.join(current_dir, "__init__.py")
        if not os.path.exists(init_path):
            break
        package_name = os.path.basename(current_dir)
        components.append(package_name)
        current_dir = os.path.dirname(current_dir)

    if len(components) == 1:
        return (None, None)

    components.reverse()
    dotted_name = ".".join(components)
    package_root = current_dir
    return (dotted_name, package_root)
