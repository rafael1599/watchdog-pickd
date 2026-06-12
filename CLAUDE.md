# Watchdog PickD

Daemon que monitorea una carpeta (`~/send-to-pickd/`) por archivos PDF de ordenes de compra, extrae el texto con pdfplumber, parsea los datos (numero de orden, cliente, items/SKUs), y los inserta en Supabase como picking lists para la app web de PickD.

## Funcionalidades principales

- Extraccion de texto de PDFs (pdfplumber)
- Deteccion de duplicados via hash SHA-256
- Creacion, append, reopen y combinacion de ordenes
- Resolucion de SKUs contra inventario (con fuzzy matching)
- Asignacion automatica de ubicaciones (prioridad: PALLET > LINE > TOWER)
- Auto-start via launchd (macOS)

## Como correr

```bash
# 1. Crear y activar virtualenv
python3 -m venv venv
source venv/bin/activate

# 2. Instalar dependencias
pip install -r requirements.txt

# 3. Configurar variables de entorno
cp .env.example .env
# Editar .env con las credenciales de Supabase

# 4. Ejecutar
python3 watcher.py
```

El watcher se instala automaticamente como LaunchAgent en macOS (`com.antigravity.watchdog-pickd`).

## Actualizar (un solo comando)

```bash
./scripts/update.sh        # rama actual: git pull + deps + reinicia LaunchAgents
./scripts/update.sh main   # o una rama especifica
```

Hace `git pull` (fast-forward), reinstala dependencias en el venv, **aplica las
migraciones de esquema** (`migrations.py`, idempotente) y reinicia los
LaunchAgents (`com.antigravity.watchdog-pickd` y `com.antigravity.pickd-app`).
El botón "⟳ Update app" de la UI dispara el mismo script.

### Migraciones de esquema (`migrations.py`)

El watcher escribe columnas en la tabla compartida `picking_lists` de PickD (ej.
`source_order_date`). PostgREST **descarta silenciosamente** columnas inexistentes
en los inserts, así que la columna debe existir o el dato se pierde sin error.
`migrations.py` aplica el DDL requerido (`ADD COLUMN IF NOT EXISTS`, idempotente)
vía una conexión directa a Postgres (`SUPABASE_DB_URL`). El service role key NO
sirve para DDL (PostgREST no expone DDL). Corre en `update.sh` después del `git
pull`; si `SUPABASE_DB_URL` no está seteada, se omite sin fallar. Coexiste con la
migración propia de PickD (ambas usan `IF NOT EXISTS`).

## Estructura

| Archivo | Descripcion |
|---------|-------------|
| `watcher.py` | Daemon principal — observa carpeta, orquesta pipeline |
| `extractor.py` | Extraccion de texto y hash de PDFs |
| `parser.py` | Parseo de texto a datos estructurados (orden, cliente, items) |
| `supabase_client.py` | Operaciones contra Supabase (CRUD picking lists, clientes, inventario) |
| `tests/` | Tests del proyecto |

## Variables de entorno

- `SUPABASE_URL` — URL del proyecto Supabase
- `SUPABASE_SERVICE_ROLE_KEY` — Service role key (bypass RLS)
- `PDF_IMPORT_USER_ID` — User ID para asociar imports
- `WATCH_PATH` — Carpeta a monitorear (default: `./inbox`)
- `SUPABASE_DB_URL` — Connection string directo a Postgres (URI), solo para aplicar
  migraciones de esquema en el update. Opcional; si falta, el paso se omite.

## Linting

```bash
ruff check .
ruff format --check .
```

## Skills

Los skills viven en el repo central `rafael1599/skills` y se conectan con un symlink **por skill** en `.claude/skills/<nombre>/` (Claude Code solo descubre SKILL.md a un nivel de profundidad).

- **Local (Mac):** symlinks hacia el repo central. Para actualizar: `git pull` en ese repo.
- **Claude Code web:** el hook SessionStart `.claude/hooks/link-skills.sh` crea los symlinks automaticamente al iniciar la sesion. Requiere el repo `skills` agregado al environment. Para habilitar mas skills, editar la lista `SKILLS` del script.

### Preferencias de conexion
- Siempre usar **symlink** para conectar skills (nunca git clone dentro del proyecto)
