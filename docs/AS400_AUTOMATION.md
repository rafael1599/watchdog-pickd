# Automatización AS400 → PickD (documento de trabajo)

> Estado: **EN PAUSA / DISEÑO**. Documento vivo para retomar e ir refinando.
> No hay código de automatización todavía. Esto captura el análisis y el plan del MVP.

## 1. Objetivo

Eliminar el ritual manual de sacar una orden del AS400 hacia PickD. Hoy el usuario hace:

> abrir AS400 → loguearse → elegir qué hacer → buscar orden → imprimir → guardar como PDF → guardar

Meta: que el usuario solo **elija la orden que quiere enviar a PickD** y use PickD como siempre.

Enfoque elegido: **automatización ASISTIDA** (un humano presente dispara la acción), no
desatendida. Esto desactiva por diseño la mayoría de los edge cases de fiabilidad.

## 2. Entorno

- **Emulador:** Mocha TN5250 (Mac). Solo GUI — sin HLLAPI/macros/scripting documentado.
- **Acceso a DB2/ODBC:** descartado (IT no lo habilita).
- **Vía técnica:** simular teclas a nivel macOS (AppleScript / `System Events` / `cliclick`)
  + leer el clipboard (`pbpaste`). El parser actual ya entiende el formato de la pantalla.
- PickD downstream (Supabase) **no cambia**: "enviar a PickD" equivale al actual "guardar PDF".

## 3. Flujo real del AS400 (confirmado por el usuario)

### 3.1 Login
Secuencia de teclas tras abrir el emulador:

```
ROMAN  →  TAB  →  STOU  →  ENTER  →  3
```

Esto deja la sesión en la pantalla donde se buscan los números de orden.

### 3.2 Números de orden
- Son **secuenciales**. Referencia: ~880004 (actualizar a medida que avanza).

### 3.3 Lectura de una orden (secuencia CONFIRMADA)

```
1. Teclear el número de orden.
   → Al poner el ÚLTIMO dígito, aparece sola la PÁGINA 1 = encabezado:
     dirección + info importante del cliente.
2. Esperar ~0.5–1 s (refresco de pantalla; normalmente instantáneo pero hay que esperar).
3. Cmd+A → Cmd+C → pbpaste → acumular   (captura del encabezado/cliente)
4. F5   → cambia a la vista de ÍTEMS (lo que hay que recoger).  [se presiona UNA sola vez]

   ┌──────── LOOP DE PÁGINAS DE ÍTEMS ────────────────────┐
5. │  Esperar ~0.5–1 s                                    │
6. │  Cmd+A → Cmd+C → pbpaste → acumular                  │
7. │  ¿el texto acumulado contiene "END OF ORDER"?        │
   │     SÍ → PARAR y enviar la orden                     │
   │     NO → ENTER (siguiente página) → volver a paso 5  │
   └──────────────────────────────────────────────────────┘
```

**Reglas clave:**
- **F5 una sola vez**, justo después de capturar la página 1 (encabezado). Pasa del
  encabezado del cliente al detalle de ítems.
- La paginación de ítems es con **ENTER**, no PgDn.
- **"END OF ORDER"** puede aparecer en la 1ª, 2ª, 3ª… página de ítems, según el tamaño de
  la orden. La regla es única: apenas aparece, se detiene y se envía. El parser ya tiene
  `has_end_of_order()`.
- Esperar **~0.5–1 s** tras teclear el número, tras el F5 y tras cada ENTER, antes de copiar,
  para no copiar la pantalla anterior.

## 4. Estado actual de PickD (lo que se reutiliza tal cual)

Pipeline hoy basado en PDF, todo gira sobre **texto → `parse_order()` → Supabase**:

```
PDF en ~/send-to-pickd/ → watchdog → extractor (pdfplumber) → compute_hash (SHA-256)
   → parser (regex) → supabase_client (create/append/reopen/combine) → processed|errors
```

Hallazgo clave: el PDF que se procesa hoy **es un print de esta misma pantalla del AS400**.
Por eso `parser.py` ya parsea el formato del clipboard con poco/ningún cambio.

Componentes reutilizables sin tocar:
- `parser.parse_order()` — order_number, account, customer, items, `is_last_page` (END OF ORDER).
- `supabase_client` — dedup por hash, `find_existing_order`, delta por SKU, create/append/
  reopen/combine, resolución de SKUs + fuzzy, asignación de ubicación.

## 5. Arquitectura del MVP

Nueva fuente de ingesta que **reemplaza al watchdog de PDF** como puerta de entrada,
reutilizando todo lo demás:

```
as400_capture (nuevo)
  ├─ driver Mocha (AppleScript/cliclick): activar ventana, teclear número
  ├─ loop de páginas: Cmd+A → Cmd+C → pbpaste → acumular → ENTER, hasta "END OF ORDER"
  ├─ texto acumulado completo
  ├─ parse_order(texto)        ← REUTILIZADO
  └─ pipeline supabase_client  ← REUTILIZADO (dedup, create/append/combine, SKUs, ubicación)
```

MVP mínimo: **un comando que recibe un número de orden**, lo captura del AS400 siguiendo la
secuencia confirmada en §3.3 (encabezado → F5 → loop de ítems con ENTER hasta "END OF ORDER"),
lo parsea y lo manda a PickD. La lista/UI para elegir órdenes va encima después.

Pseudocódigo del MVP:

```
def capturar(numero):
    activar_mocha()
    teclear(numero)              # al último dígito aparece la pág. 1 (encabezado)
    sleep(0.5..1)
    texto = copiar_pantalla()    # Cmd+A, Cmd+C, pbpaste
    tecla("F5")                  # pasa a ítems  (una sola vez)
    while True:
        sleep(0.5..1)
        texto += copiar_pantalla()
        if "END OF ORDER" in texto:
            break
        tecla("ENTER")           # siguiente página de ítems
        # (tope de seguridad de N páginas para no quedar en loop infinito)
    enviar_a_pickd(parse_order(texto))
```

## 6. Backlog / puntos ciegos parqueados (para mejorar después)

Marcados por quién los resuelve y prioridad.

### Resueltos por el enfoque asistido (no requieren código)
- Loop 24/7, Mac bloqueada, salvapantallas, timeout de sesión → el humano está presente.
- "Hueco vs orden futura" (números saltados/anulados) → el humano elige el número.
- Órdenes que no deben recogerse / ruido en PickD → filtro humano en el origen.
- Combine accidental entre órdenes buenas y basura → solo se envía lo elegido.

### Pendientes reales (chicos) — automatización
- [ ] **Paginación robusta**: confirmar marcador exacto de "END OF ORDER" y cortar bien;
      tope de páginas de seguridad para no quedar en loop si nunca aparece.
- [ ] **Permisos macOS**: Accesibilidad + Automatización para la terminal/python (setup único).
- [ ] **Timing**: esperas entre teclas y entre páginas (la pantalla debe refrescar antes de copiar).
- [ ] **Re-login**: detectar pantalla de login si la sesión expiró y rehacer la secuencia.
- [ ] **Detección de "orden no encontrada"** vs orden válida (para feedback al usuario).
- [ ] **Contención de clipboard**: el usuario no debe copiar otra cosa mientras corre.

### Pendientes — parser / datos
- [ ] Blindar `parse_items` ante cantidades negativas (devoluciones/credit memos).
- [ ] `warehouse="LUDLOW"` está hardcodeado en `supabase_client._to_cart_items`.
- [ ] Dedup: hoy por hash de texto; el clipboard puede variar el espaciado vs PDF.
      Conviene dedup por `order_number` (ya hay `find_existing_order` + delta que protege).
- [ ] Observabilidad sin archivos: dead-letter en Supabase para capturas que no parsean.

### Pendientes — FUERA de este repo (PickD web app + esquema Supabase)
- [ ] **Reserva diferida a "Start Picking"**: hoy reservar = asignar ubicación al importar,
      y `COMBINABLE_STATUSES` incluye `ready_to_double_check` (reserva al importar).
      Requisito nuevo: no reservar hasta presionar Start Picking, y **re-validar/re-asignar
      stock y ubicación en ese momento** (lo calculado al enviar queda stale).
- [ ] Estado inicial nuevo (ej. `available`) y expiración/archivado de órdenes no iniciadas.

## 6b. Mejoras de UI / captura (hechas)

- **Anti-loop por vista**: antes de paginar, `capture_order` valida que el encabezado sea
  una pantalla `ORDER INQUIRY` (`_looks_like_order_screen`). Si no lo es (estamos en un menú
  u otra vista, o la orden no existe), corta con un mensaje claro en vez de pulsar F5 y entrar
  al loop. Recuperación manual sugerida: **F7 → 3** para volver a búsqueda de orden.
- **Órdenes enviadas ocultas**: en la UI, las órdenes ya enviadas se mueven a una sección
  colapsable "✓ Sent to PickD (n)" para no saturar la lista de pendientes.

### Backlog de captura (pendiente, requiere ejemplos de pantallas)
- Auto-recuperación: si la vista es incorrecta, pulsar **F7 → 3** automáticamente y reintentar.
- Detectar las distintas vistas del AS400 (menú principal, búsqueda, etc.) por marcadores.
- Extraer del encabezado: **Ship-to** (dirección/cliente), y **Order Comments** → notas, para
  usarlos en PickD (definir columnas en Supabase).

## 7. Decisiones abiertas
- Cómo se puebla la lista de órdenes elegibles (UI): (A) el usuario teclea/elige el número
  vs (B) auto-listar un rango. El MVP arranca con A (1 orden por comando).
- Login: ¿la automatización teclea el login o asume sesión Mocha ya abierta? (MVP: asume abierta).
- Multi-usuario / multi-máquina: definir más adelante (dedup en Supabase protege duplicados).

### Resueltas (confirmadas por el usuario)
- F5 se presiona **una sola vez**, tras capturar el encabezado, para pasar a los ítems.
- Paginación de ítems con **ENTER**; "END OF ORDER" puede caer en cualquier página → al
  aparecer, parar y enviar.
- Esperar **~0.5–1 s** tras teclear/F5/ENTER antes de copiar.

## 8. MVP construido (cómo correrlo)

Interfaz web local. **Correr en la Mac con Mocha TN5250 abierto y logueado.**

```bash
pip install -r requirements.txt
python3 app.py
# abrir http://127.0.0.1:5000
```

Flujo en la UI:
1. Teclear el número de orden → **Capturar** (maneja Mocha: encabezado → F5 → ENTER hasta
   "END OF ORDER"). No tocar el teclado durante la captura.
2. La orden aparece en la lista con preview: **número, cliente, conteo de ítems**.
3. **Enviar a PickD** una por una, la que elijas, tras revisar el preview.

Permiso macOS necesario (una vez): dar **Accesibilidad** y **Automatización** a la terminal
desde la que corre `python3` (Ajustes → Privacidad y seguridad).

### Conectar AS400 desde la UI (lanzar + login)
Botón **Conectar AS400** en la interfaz: lanza el emulador con `open -a` (robusto, no usa
Spotlight/Cmd+Space), espera ~5 s y corre el macro de login
`ROMAN → TAB → STOU → ENTER → 3 → ENTER`, dejando la sesión en la pantalla de búsqueda.
Todo sin salir de la interfaz.

Variables de entorno:
- `MOCHA_APP_NAME` — nombre de la app para enfocar/teclear (default "Mocha TN5250").
- `AS400_LAUNCH_TARGET` — qué abrir: nombre de app o ruta a un `.app`/archivo de sesión
  (default = `MOCHA_APP_NAME`).
- `AS400_LAUNCH_WAIT` — segundos a esperar tras lanzar antes del login (default 5).

El macro de login está en `DEFAULT_LOGIN_STEPS` (`as400_capture.py`) — fácil de editar si la
secuencia/tiempos reales difieren (ej. si "3" no necesita ENTER después).

### Auto-arranque al prender la Mac (sin Terminal)
Para que al iniciar sesión se abra el AS400, arranque la app y se abra Safari en la UI —
sin ventana de Terminal — instalar el LaunchAgent **una vez** en la Mac del AS400:

```bash
cd ~/watchdog-pickd
python3 scripts/install_autostart.py
```

Qué hace en cada login (vía `scripts/start_pickd.py`, ejecutado por el python del venv):
1. Abre el emulador (`open -b <AS400_LAUNCH_TARGET>`).
2. Levanta `app.py` en segundo plano (sin Terminal).
3. Espera a que el servidor responda y abre **Safari** en `http://127.0.0.1:5000`.

No hace login automático en el AS400 (eso se hace a mano o con el botón Connect). Corre una
vez por login (`KeepAlive=False`) para no reabrir Safari/Mocha en bucle.

> Nota TCC: el launcher es **Python ejecutado por el python del venv** (no `bash`). macOS
> bloquea a `bash` al leer scripts dentro de carpetas protegidas como `~/Documents`
> (error "Operation not permitted"); el python del venv ya tiene acceso (igual que el watcher).

Para quitarlo:
```bash
launchctl unload ~/Library/LaunchAgents/com.antigravity.pickd-app.plist
rm ~/Library/LaunchAgents/com.antigravity.pickd-app.plist
```

Logs: `logs/app-stdout.log` y `logs/app-stderr.log`.

### Archivos del MVP
| Archivo | Rol |
|---------|-----|
| `app.py` | UI web local (Flask): capturar, preview, enviar una por una |
| `scripts/start_pickd.py` | Launcher: abre Mocha + app + Safari (para el LaunchAgent) |
| `scripts/install_autostart.py` | Instala el LaunchAgent de auto-arranque al login |
| `as400_capture.py` | Driver Mocha (AppleScript) + loop de captura (testeable) |
| `pipeline.py` | Lógica compartida texto → Supabase (la usan watcher y app) |
| `tests/test_as400_capture.py` | Tests de la lógica del loop (sin GUI) |

### Próximos pasos (pendientes del backlog §6)
- Probar en la Mac la captura real (timing, marcador exacto, paginación multi-página).
- Login automático opcional (`ROMAN+TAB+STOU+ENTER+3`) si se quiere arrancar desde cero.
- Robustez de clipboard (detectar pantalla no refrescada en vez de espera fija).
- Reforma de reservas en PickD web app (no reservar hasta Start Picking).
