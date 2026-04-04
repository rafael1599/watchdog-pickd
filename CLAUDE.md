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

## Linting

```bash
ruff check .
ruff format --check .
```

## Skills

Este proyecto usa skills de `.claude/skills/` (symlink a repo central). Para actualizar: `cd .claude/skills && git pull`

### Preferencias de conexion
- Siempre usar **symlink** para conectar skills (nunca git clone dentro del proyecto)
