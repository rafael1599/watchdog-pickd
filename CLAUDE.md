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

### Cuenta AS400 y ship-to → la llave de FedEx (`fedex_recipient_id`)

El header `Order Number: 880036   Account Number: 0010495 00` trae la cuenta bill-to
(7 dígitos) y el sufijo ship-to (2). `parser.split_account_number` los separa en
`("10495", "00")` y `parse_order()` los expone como `as400_account` y `as400_ship_to`
(`account_number` sigue siendo el valor crudo). El Recipient ID que el ship station
teclea en FedEx Ship Manager es `cuenta sin ceros + sufijo` = `1049500`, y FSM ya tiene
951 destinatarios con esa convención — por eso la llave se guarda y no se inventa.

- **Cuenta → `customers.as400_account`.** `_resolve_customer` busca **primero por
  cuenta**; solo sin match cae al nombre + calle de siempre, y al encontrar o crear la
  fila **sella** la cuenta (`UPDATE … IS NULL`: rellena si está vacía, nunca la pisa).
- **Sufijo → `customer_addresses.as400_ship_to`.** El watcher **nunca escribe
  `fedex_recipient_id`**: lo deriva un trigger de la DB a partir del sufijo y la cuenta
  del cliente. Localmente solo se calcula para *buscar* el slot.
- **Regla del slot que se mudó:** la llave identifica un ship-to, no un cliente (dos
  tiendas = `xxxx00` y `xxxx01`). Si ya existe una dirección con ese `fedex_recipient_id`
  y otra calle, es el mismo dealer que se mudó → se **actualiza esa fila** (el trigger
  pone `fedex_synced_at = NULL`), no se crea otra. Con la misma dirección no se escribe
  nada. Sin slot → upsert por `(customer_id, normalized_address)` con `as400_ship_to`.
- **`customers.ship_to_varies`** (consumidor directo, Facebook, garantía, eBay…): el
  destinatario cambia en cada orden, así que sus direcciones se guardan como siempre y
  **nunca** llevan `as400_ship_to` ni Recipient ID. También se omite si el cliente no
  tiene cuenta sellada o el header no trae sufijo.
- **La orden** guarda `picking_lists.as400_account_number` (crudo, para auditoría) y
  `ship_to_address_id` (la fila que devuelve `_save_shipping_address`). Ambas se omiten
  del insert cuando no se conocen.

Las columnas, los CHECKs, el índice único parcial y el trigger viven en la migración
`20260826230000_fedex_recipient_key.sql` de Pickd — se aplica en prod **antes** de
desplegar el watcher. `migrations.py` solo repite los `ADD COLUMN IF NOT EXISTS` por si
el watcher se actualiza antes que ella.

**Backfill, una sola vez tras actualizar en la MacBook de Bay 2** (donde vive
`.scanned_orders.json` con el `raw_text` de cada captura):

Desde la UI: **⋯ → Maintenance → Backfill AS400 accounts**, primero *Preview* (no escribe
nada, muestra los conteos y el detalle por orden) y luego *Apply*. La lógica vive en
`maintenance.py` (registro `ACTIONS`: cada acción nueva es una entrada ahí y nada más; el
panel las pinta solo). Por terminal, lo mismo:

```bash
./venv/bin/python3 scripts/backfill_account_numbers.py          # dry-run, solo imprime
./venv/bin/python3 scripts/backfill_account_numbers.py --apply
```

Re-parsea el header de cada orden en caché, busca la fila por `order_number` y rellena
lo que esté NULL (header crudo, cuenta del cliente, sufijo de la dirección, enlace
orden → dirección). Es idempotente: una segunda pasada no toca nada.

## Estructura

| Archivo | Descripcion |
|---------|-------------|
| `watcher.py` | Daemon principal — observa carpeta, orquesta pipeline |
| `extractor.py` | Extraccion de texto y hash de PDFs |
| `parser.py` | Parseo de texto a datos estructurados (orden, cliente, items) |
| `supabase_client.py` | Operaciones contra Supabase (CRUD picking lists, clientes, inventario) |
| `pipeline.py` | Texto de orden → Supabase (create/append/reopen/combine); lo usan watcher y app |
| `migrations.py` | DDL idempotente que el watcher necesita (`ADD COLUMN IF NOT EXISTS`) |
| `maintenance.py` | Acciones de mantenimiento del panel ⋯ → Maintenance (registro `ACTIONS`, dry-run/apply, un lock) |
| `scripts/backfill_account_numbers.py` | CLI de la acción "Backfill AS400 accounts" (la lógica está en `maintenance.py`) |
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
