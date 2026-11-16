#!/usr/bin/env bash
set -euo pipefail

# Publish all public horsies packages to crates.io in dependency order.
# Usage: ./scripts/publish.sh
#
# Reads CRATES_IO_TOKEN from .env in the project root.
#
# horsies-macros has a circular dev-dep on horsies (for compile tests).
# The script temporarily removes it during packaging, then restores it.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
MACROS_TOML="$ROOT_DIR/macros/Cargo.toml"

# Load token from .env
TOKEN=$(grep CRATES_IO_TOKEN "$ROOT_DIR/.env" | cut -d= -f2 | tr -d ' "')
if [ -z "$TOKEN" ]; then
    echo "error: CRATES_IO_TOKEN not found in .env"
    exit 1
fi

CRATES=(
    horsies-macros
    horsies
)

for crate in "${CRATES[@]}"; do
    echo "=== Publishing $crate ==="

    if [ "$crate" = "horsies-macros" ]; then
        # Temporarily remove circular dev-dep on horsies facade.
        cp "$MACROS_TOML" "$MACROS_TOML.bak"
        sed -i '' '/^horsies = {.*path.*horsies/d' "$MACROS_TOML"
        cargo publish -p "$crate" --token "$TOKEN" --allow-dirty --no-verify
        mv "$MACROS_TOML.bak" "$MACROS_TOML"
    else
        cargo publish -p "$crate" --token "$TOKEN" --allow-dirty
    fi

    echo "$crate published."
    echo

    # Wait for crates.io index to update before publishing the facade,
    # which depends on the freshly-published macros crate.
    if [ "$crate" != "horsies" ]; then
        echo "Waiting 30s for index update..."
        sleep 30
    fi
done

echo "All crates published."
