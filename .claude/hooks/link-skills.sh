#!/bin/bash
# link-skills.sh — enlaza skills del repo central `skills` en .claude/skills, UNA POR UNA.
#
# Claude Code solo descubre skills en .claude/skills/<nombre>/SKILL.md (un solo nivel de
# profundidad): un symlink al root del repo NO expone global-skills/*, project-skills/*
# ni external-skills/*. Verificado el 28 ago 2026 con `claude -p` — con el symlink al repo
# completo solo apareció `find-skills`, el único que está a un nivel.
#
# Dos modos:
#   - Hook SessionStart (sin argumentos): solo actúa en Claude Code web
#     (CLAUDE_CODE_REMOTE=true), donde el repo `rafael1599/skills` queda clonado como
#     directorio hermano del proyecto. En local no hace nada y no gasta tokens.
#   - `--local`: en esta máquina, apunta a $SKILLS_PATH (~/dev/skills por defecto).
#     Regenera los symlinks y borra los muertos. Correrlo tras mover el repo central o al
#     habilitar una skill nueva en la lista SKILLS.
#
# Fuente canónica: $SKILLS_PATH/global-skills/project-setup/scripts/link-skills.sh.
# Cada proyecto lleva una copia con su propia lista SKILLS; si cambia la lógica acá,
# cambiarla en las copias. La lista SKILLS es lo único propio de cada proyecto.
set -euo pipefail

MODE=hook
if [ "${1:-}" = "--local" ]; then MODE=local; fi
if [ "$MODE" = hook ] && [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

ROOT="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "$0")/../.." && pwd)}"

if [ "$MODE" = local ]; then
  CANDIDATES="${SKILLS_PATH:-$HOME/dev/skills}"
else
  CANDIDATES="$ROOT/../skills $HOME/skills"
fi
SKILLS_REPO=""
for cand in $CANDIDATES; do
  if [ -d "$cand/global-skills" ]; then
    SKILLS_REPO="$(cd "$cand" && pwd)"
    break
  fi
done
if [ -z "$SKILLS_REPO" ]; then
  if [ "$MODE" = local ]; then
    echo "link-skills: no encuentro el repo central (SKILLS_PATH='${SKILLS_PATH:-}')" >&2
    exit 1
  fi
  exit 0
fi

DEST="$ROOT/.claude/skills"
if [ -L "$DEST" ]; then
  rm -f "$DEST"   # esquema viejo: symlink al repo completo (no descubre nada anidado)
fi
mkdir -p "$DEST"

# Skills habilitadas para este proyecto (rutas relativas dentro del repo skills).
# Cada descripción de skill ocupa contexto en cada sesión: agregar solo las necesarias.
SKILLS="
global-skills/commit-craft
"

for rel in $SKILLS; do
  src="$SKILLS_REPO/$rel"
  dst="$DEST/$(basename "$rel")"
  if [ ! -f "$src/SKILL.md" ]; then
    if [ "$MODE" = local ]; then echo "link-skills: falta $rel (sin SKILL.md), la salto" >&2; fi
    continue
  fi
  # Solo rellena huecos: si el destino es un directorio real (skill vendorizada), se respeta.
  if [ -L "$dst" ] || [ ! -e "$dst" ]; then
    ln -sfn "$src" "$dst"
  fi
done

if [ "$MODE" = local ]; then
  for f in "$DEST"/*; do
    if [ -L "$f" ] && [ ! -e "$f" ]; then
      rm -f "$f"
      echo "link-skills: eliminado symlink muerto $(basename "$f")" >&2
    fi
  done
  echo "link-skills: $(find "$DEST" -mindepth 1 -maxdepth 1 | wc -l | tr -d ' ') skills en $DEST" >&2
  exit 0
fi

# reloadSkills: carga los skills enlazados en esta misma sesión (sin esperar rescan).
# suppressOutput: el stdout no se agrega al contexto de Claude (cero tokens).
echo '{"hookSpecificOutput": {"hookEventName": "SessionStart", "reloadSkills": true}, "suppressOutput": true}'
