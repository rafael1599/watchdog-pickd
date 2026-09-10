# Watchdog PickD

Daemon que monitorea una carpeta (`~/send-to-pickd/`) por archivos PDF de ordenes de compra, extrae el texto con pdfplumber, parsea los datos (numero de orden, cliente, items/SKUs), y los inserta en Supabase como picking lists para la app web de PickD.

> **El auto-deploy se paró solo, y así se ve (10 sep 2026).** Un push es el deploy
> (`auto_update.py`), pero `update.sh` hace `mkdir -p logs`, `logs/` no estaba en `.gitignore` y el
> gate leía `git status --porcelain` — que cuenta lo no trackeado. O sea: **el primer auto-update
> exitoso creó el directorio que bloqueó todos los siguientes**. Bay 2 estuvo 18 h en `812012d`
> diciendo «there are uncommitted changes here» cada 5 min a nadie (el poller solo loguea un motivo
> **nuevo**, que es la regla correcta y es también por lo que pasó desapercibido). Arreglado en los dos
> sitios: `logs/` ignorado y `dirty` con `--untracked-files=no` — `git pull --ff-only` **no** se niega
> por archivos sin trackear que no va a sobrescribir, así que el gate solo debe mirar trabajo local
> real. **Para saber qué corre de verdad:** `select version, seen_at from as400_watcher_heartbeat`; la
> versión se sella al arrancar el proceso, así que si no se mueve es que no se ha reiniciado.

> **El watcher tampoco decide de dónde se recoge (10 sep 2026).** `_to_cart_items` ya no asigna
> `location` / `sublocation` / `location_hint` / `distribution` ni publica `available_qty`: manda la
> línea con `location: None` y **PickD la planifica** al tomar la orden (`planPickForList`), contra el
> stock vivo y descontando lo que otras órdenes abiertas ya tienen apartado. Con eso murió el espejo
> `_is_return_to_stock` — la copia a mano de `isReturnToStock` de pickd — junto con el ranking
> PALLET > LINE > TOWER y la reserva por ubicación. Lo que **queda** aquí es transcripción y resolución
> de SKU: `_pick_by_stock` sigue eligiendo entre hermanos de variante por stock total, y
> `insufficient_stock` sigue saliendo del total menos lo reservado — nunca dependió de una ubicación,
> así que significa exactamente lo mismo que antes. Ojo: la reserva por SKU **ya no exige** que la
> línea tenga ubicación, o una orden sin planificar sería invisible para esa cuenta.

> **El watcher no combina órdenes (9 sep 2026).** Tenía un auto-combine por cliente en ventana de
> 24 h (`find_combinable_order_by_customer` + `combine_into_order`, paso 4 del pipeline) que metía la
> orden nueva en el `group_id` de otra del mismo cliente — incluida una en `double_checking`, o sea
> una que alguien estaba verificando en ese momento. PickD tuvo el mismo agujero por su lado
> (`auto_group_fedex_orders`, bug-023: #881394 se completó a los 5 segundos de nacer, sin una sola
> línea verificada) y allá se cerró con `group_is_held`. Acá se quitó entero, que era lo que
> correspondía: decidir que dos órdenes son un solo envío es una decisión de negocio, y la toma una
> persona en PickD con el botón Combine. `STOCK_HOLDING_STATUSES` es lo único que sobrevive de ese
> código, con su nombre real: los estados en los que una orden todavía reserva stock.

## Funcionalidades principales

- Extraccion de texto de PDFs (pdfplumber)
- Deteccion de duplicados via hash SHA-256
- Creacion, append y reopen de ordenes (**combinar NO**: es decisión de PickD, con una persona confirmando)
- Resolucion de SKUs contra inventario (con fuzzy matching)
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

## La puerta: pickd ve las capturas, Bay 2 ejecuta (8 sep 2026)

Rafael: *«el watcher se encarga de la parte operativa y pickd de la visual»*. `door.py` publica
cada entrada de la caché en `as400_captures` (migración `20260909030530` de pickd) — parseo,
ítems con `is_bike` embebido y grafía canónica, texto crudo — para que el Live Board pinte la
orden FedEx/regular con cliente, pallets, bicis y partes **antes** de que nadie la traiga. «Traer»
sólo marca `requested`; `door.py` la sondea cada 10 s y envía por `process_order_text`, el mismo
camino del botón Send. **Nada viaja de pickd a Bay 2**: este Mac está detrás del NAT.

Tres decisiones de diseño que no son obvias y hay que respetar al tocarlo:
- **No se publica desde `run_scan_step`.** Con la tabla ausente PostgREST daría 404 en cada
  captura y el escáner pasaría a una orden cada 5 min. Publica un reconciliador en su hilo,
  idempotente, con backoff — que además es el backfill.
- **El lock de envío es `door.sending`, por número de orden**, compartido con `/api/orders/<id>/send`.
  Sin él, «Traer» y el botón de Bay 2 a la vez crearían dos filas (no hay UNIQUE en
  `order_number`).
- **`door.busy` frena `auto_update`**: el envío no toma `capture_lock` y un reinicio a medias
  dejaría la fila en `sending` (se recupera a los 2 min, pero mejor no fabricarlo).

Las reglas de basura viven en `door.classify` (ebay → junk; sin cliente, página perdida, stale →
held; 8 días → archived) porque antes sólo corrían cuando alguien abría la UI de Bay 2.
`picking_lists.source` es ahora `as400` para todo lo que viene del terminal
(`supabase_client.source_for`). Apagar todo: `AS400_DOOR=0` + restart.

## Actualizar: un push es el deploy (8 sep 2026)

`auto_update.py` mira `origin` cada 5 minutos desde Bay 2 y, cuando es seguro, corre el
mismo `scripts/update.sh` que el botón ⟳. **Pregunta esta máquina; GitHub no llama.**

**Lo que cuesta, medido en el repo real:** `git ls-remote` = **0,26 s por sondeo**, casi
todo espera de red y no CPU, 288 veces al día — menos de una décima de porcentaje de la
máquina y menos que dos capturas del AS400. `ls-remote` en vez de `fetch` **no escribe
nada** en el repo (ni objetos, ni `FETCH_HEAD`, ni refs), que es lo que corresponde a un
proceso que corre en una Mac donde alguien está trabajando. El `fetch` además sobraba:
`update.sh` hace su propio `git pull`.

**Que avisara GitHub sería mejor forma en abstracto y peor aquí**, y se mide igual: toda
vía de "que avise" exige o exponer esta Mac a internet o mantener un demonio con conexión
permanente (`cloudflared`, o un runner self-hosted de Actions, que es un long-poll
disfrazado de servicio). Cualquiera de los dos es **más** corriendo en la máquina, no
menos.

Se niega a actualizar **durante una captura** (`update.sh` reinicia los LaunchAgents y
dejaría a Mocha en una pantalla desconocida, perdiendo la orden), **mientras alguien usa
el Mac** (un reinicio le quita la UI de delante) y **con cambios sin commitear** (el pull
es `--ff-only` y fallaría igual; lo dice una vez, no cada cinco minutos). Si lanza una
actualización y `HEAD` no se mueve, no lo repite: algo va mal y repetirlo sólo enterraría
el motivo.

La decisión vive en `why_not_now()`, que es pura y está testeada — un argumento de
seguridad que sólo existiera dentro de un hilo en una Mac del almacén no lo podría
revisar nadie. Se apaga con `AUTO_UPDATE=0`.

## Actualizar a mano (un solo comando)

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

### La clase de envío cuenta BICIS, no unidades (8 sep 2026)

`classify_shipping` (pipeline.py) es el **tercer espejo** de una regla que vive además en
`src/utils/shippingClassification.ts` de pickd y en la función `classify_picking_list_fedex`
de la DB. El archivo de pickd lleva una nota de «keep both in sync» que este lado nunca vio,
y este lado se desvió: contaba **todas** las unidades, así que **cinco pedales salían como
camión**. Lo cazó Rafael comparándolo con Double Check View.

La regla real, en palabras de pickd: *«Parts never make an order 'regular' on their own: an
order of 50 small parts still ships FedEx. Only bike volume (or a heavy item) forces a
truck.»* Ahora `preview_order` cuenta bicis con `get_bike_skus()` (cacheado con TTL) y
devuelve `shipping_type_basis` = `bikes` o `units-fallback`, para que un respaldo nunca se
confunda con una respuesta.

**No portada a propósito:** la regla 1 de pickd, «cualquier ítem de más de 50 lb → camión».
Necesita pesos por SKU que este lado no tiene al previsualizar, y adivinarlos sería una
cuarta respuesta en vez de una tercera. Sólo AÑADE órdenes `regular`, así que su ausencia
puede dejar una parte pesada como FedEx **en el color local**, nunca en el envío.

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
| `pipeline.py` | Texto de orden → Supabase (create/append/reopen); lo usan watcher y app |
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

Las skills **globales** vienen del plugin `globals@rafael-skills` (marketplace `rafael1599/skills`), declarado en `.claude/settings.json`: Claude Code lo instala solo al abrir el proyecto, también en Claude Code web y en otra máquina. Se invocan como `/globals:<skill>` (p. ej. `/globals:commit-craft`). Tras un push al repo de skills: `claude plugin marketplace update rafael-skills && claude plugin update globals@rafael-skills`. Nunca copiar ni enlazar skills globales a mano (el esquema de symlinks + hook `link-skills.sh` se retiró el 29 ago 2026).

