#!/usr/bin/env bash
# Point this clone's git hooks at the version-controlled scripts/git-hooks dir.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
git config core.hooksPath scripts/git-hooks
echo "Hooks enabled: $(git config core.hooksPath)"
