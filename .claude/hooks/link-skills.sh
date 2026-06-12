#!/bin/bash
# SessionStart hook: enlaza skills del repo central `skills` en .claude/skills.
#
# Claude Code solo descubre skills en .claude/skills/<nombre>/SKILL.md (un solo
# nivel de profundidad) — un symlink al root del repo NO expone los skills
# anidados (global-skills/*, project-skills/*), por eso se enlaza skill por skill.
#
# Solo corre en Claude Code on the web (CLAUDE_CODE_REMOTE=true). Requiere que
# el repo `rafael1599/skills` esté agregado al environment: queda clonado como
# directorio hermano del proyecto. En local (Mac) no hace nada.
set -euo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"

SKILLS_REPO=""
for cand in "$ROOT/../skills" "$HOME/skills"; do
  if [ -d "$cand/global-skills" ]; then
    SKILLS_REPO="$(cd "$cand" && pwd)"
    break
  fi
done
if [ -z "$SKILLS_REPO" ]; then
  exit 0
fi

DEST="$ROOT/.claude/skills"
if [ -L "$DEST" ]; then
  rm -f "$DEST"
fi
mkdir -p "$DEST"

# Skills habilitados para este repo (rutas relativas dentro del repo skills).
# Cada descripción de skill ocupa contexto en cada sesión: agregar solo los necesarios.
SKILLS="
global-skills/commit-craft
"

for rel in $SKILLS; do
  src="$SKILLS_REPO/$rel"
  dst="$DEST/$(basename "$rel")"
  if [ ! -f "$src/SKILL.md" ]; then
    continue
  fi
  if [ -L "$dst" ] || [ ! -e "$dst" ]; then
    ln -sfn "$src" "$dst"
  fi
done

# reloadSkills: carga los skills enlazados en esta misma sesión (sin esperar rescan).
# suppressOutput: el stdout no se agrega al contexto de Claude (cero tokens).
echo '{"hookSpecificOutput": {"hookEventName": "SessionStart", "reloadSkills": true}, "suppressOutput": true}'
