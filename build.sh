#!/bin/sh
set -eu

DAEMON_NAME="bsh-daemon"
XDG_DATA_HOME="${XDG_DATA_HOME:-$HOME/.local/share}"
INSTALL_DIR="$XDG_DATA_HOME/bsh"
BIN_PATH="$HOME/.local/bin"
ZSHRC_PATH="${ZDOTDIR:-$HOME}/.zshrc"
ZSH_INIT_FILE="scripts/bsh_init.zsh"

need_cmd() { command -v "$1" >/dev/null 2>&1 || { echo "Error: required '$1' not found." >&2; exit 1; }; }

echo "Preparing BSH installation..."
mkdir -p "$BIN_PATH" "$INSTALL_DIR/scripts"

echo "Building C++ binary..."
need_cmd cmake
need_cmd c++

rm -rf build

# Prefer Ninja if available, otherwise fall back to default generator (usually Makefiles)
if command -v ninja >/dev/null 2>&1; then
  cmake -S . -B build -G Ninja -Wno-dev
else
  cmake -S . -B build -Wno-dev
fi

cmake --build build --target "$DAEMON_NAME"

echo "Installing binaries and scripts..."
cp "build/$DAEMON_NAME" "$BIN_PATH/$DAEMON_NAME"
cp "$ZSH_INIT_FILE" "$INSTALL_DIR/scripts/"

# Only update zshrc if zsh is present (or if user is actually using zsh)
if command -v zsh >/dev/null 2>&1; then
  echo "Updating $ZSHRC_PATH..."
  INIT_LINE="source $INSTALL_DIR/scripts/bsh_init.zsh"
  touch "$ZSHRC_PATH"
  if ! grep -Fqx "$INIT_LINE" "$ZSHRC_PATH"; then
    printf '\n# BSH History Integration (Added by build.sh)\n%s\n' "$INIT_LINE" >> "$ZSHRC_PATH"
    echo "Success! Please run: . \"$ZSHRC_PATH\" (or restart your terminal)."
  else
    echo "Note: BSH initialization already found in $ZSHRC_PATH. Skipping update."
  fi
else
  echo "zsh not found; skipping .zshrc update."
  echo "To enable manually in zsh later, add this line to ~/.zshrc:"
  echo "  source $INSTALL_DIR/scripts/bsh_init.zsh"
fi
