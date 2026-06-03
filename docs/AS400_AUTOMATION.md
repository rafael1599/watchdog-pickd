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

### 3.3 Lectura de una orden (con paginación por ENTER)

```
1. Teclear el número de orden.
   → Al poner el ÚLTIMO dígito, la orden aparece sola en pantalla (no requiere ENTER extra).
2. Cmd+A  (seleccionar todo)
3. Cmd+C  (copiar)  → contenido al clipboard
4. Consumir el clipboard (pbpaste) y acumular el texto.
5. ENTER  → avanza a la SIGUIENTE PÁGINA  (NO se usa PgDn; en AS400 se pagina con ENTER)
6. ¿Hay más ítems? → repetir Cmd+A, Cmd+C, consumir, ENTER
7. Repetir hasta encontrar el mensaje "END OF ORDER".
```

**Importante:** la paginación es con **ENTER**, no PgDn. El fin de la orden se detecta por el
texto **"END OF ORDER"** (el parser ya tiene `has_end_of_order()`).

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

MVP mínimo: **un comando que recibe un número de orden**, lo captura del AS400 (con
paginación), lo parsea y lo manda a PickD. La lista/UI para elegir órdenes va encima después.

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

## 7. Decisiones abiertas
- Cómo se puebla la lista de órdenes elegibles (UI): (A) el usuario teclea/elige el número
  vs (B) auto-listar un rango. El MVP arranca con A (1 orden por comando).
- Login: ¿la automatización teclea el login o asume sesión Mocha ya abierta? (MVP: asume abierta).
- Multi-usuario / multi-máquina: definir más adelante (dedup en Supabase protege duplicados).

## 8. Próximo paso
Construir el MVP: comando `capturar <numero_orden>` → maneja Mocha, pagina con ENTER hasta
END OF ORDER, parsea y manda a PickD. Refinar la picture con el usuario antes de codear.
