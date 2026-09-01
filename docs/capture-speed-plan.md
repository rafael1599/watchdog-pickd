# Plan: la captura AS400, el doble de rápido (sin cortar el servicio)

> Estado: **PROPUESTA**, 1 sep 2026. Nada desplegado todavía.
> Objetivo del operador (Rafael, 1 sep 2026): *«hacerlo el doble de rápido sin crashearlo»*,
> y lo que quiere que baje es **la captura en sí** — los ~10-15 s que tarda en leer las
> pantallas de Mocha y el rato que le roba el teclado mientras lo hace.

## 1. Qué se acelera y cómo se mide

**Alcance:** `capture_order` + `MochaDriver` (as400_capture.py) y el ritmo del bucle que las
llama (auto_scanner.py). Nada de parser, nada de Supabase, nada de UI.

**Criterio de aceptación**, medido en la MacBook de Bay 2 con el log de la Fase 0:

| | hoy (modelo) | meta |
|---|---|---|
| orden de 2 páginas de ítems | ~9,1 s | **≤ 4,5 s** |
| orden de 4 páginas | ~13,3 s | **≤ 7 s** |
| una lectura de pantalla | ~1,19 s | ≤ 0,5 s |
| órdenes con `total_mismatch` en 3 días de observación | 0 | **0** |
| capturas `incomplete` nuevas en 3 días | — | **0** |

La última fila es la que importa: `total_mismatch` (el Sub-Total del header contra la suma de
las líneas parseadas, ya implementado en `pipeline.preview_order`) es **el detector de una
página de ítems perdida**, que es exactamente como una mejora de velocidad rompió este sistema
antes (§3.1). No es una métrica nueva: es la red que ya existe, mirada a propósito.

## 2. Dónde se va el tiempo hoy

Costes medidos (esta Mac, Apple Silicon; la de Bay 2 puede ser 2-3× más lenta por spawn):
`osascript` trivial **43 ms**, `osascript` con System Events **128-146 ms** (máx 522),
`pbcopy`/`pbpaste` **10 ms**.

Una lectura de pantalla (`MochaDriver.copy_screen`) = 3 procesos `osascript` (focus, Cmd+A,
Cmd+C) + 3 sleeps fijos (0,4 + 0,15 + 0,2) + pbcopy/pbpaste ≈ **1,19 s**.

Una captura de 2 páginas = 4 lecturas + 4 teclas + 3 sleeps de `page_wait` (0,8) ≈ **9,1 s**:

- **6,6 s (73 %) son `time.sleep()`** — esperas fijas que no miran la pantalla.
- **2,2 s (25 %) es lanzar `osascript`** — 16 procesos por orden.
- **0,2 s (2 %) es trabajo real** (portapapeles + parseo).

O sea: la captura no está haciendo nada caro, está *esperando a ciegas*. Ahí está el doble.

## 3. Por qué las mejoras anteriores cortaron el servicio

Seis cortes reales, del propio historial del repo. Cada uno deja una regla.

### 3.1 — 5 jun (`38663da`): «más rápido» borró ítems de una orden
El paginado dormía un rato fijo y copiaba una vez. Si el 5250 no había refrescado, copiaba la
página anterior, no veía END OF ORDER, pulsaba ENTER otra vez y **se saltaba una página entera
de ítems**. No hubo error: hubo órdenes incompletas.
→ **Regla A: ninguna espera se acorta sin comprobar que la pantalla cambió *y* está quieta.**

### 3.2 — 9 jun (`30199ff`): la app no arrancaba en Bay 2
`dict | None` en anotaciones: Python 3.9 en esa Mac, ventana en blanco.
→ **Regla B: el entorno de destino no es esta Mac.** `from __future__ import annotations`,
y se verifica el arranque real, no sólo los tests.

### 3.3 — 9 jun (`2ef330e`): el scanner bloqueaba la UI entera
Flask single-thread; el ciclo del scanner (~70 s) dejaba la ventana muerta.
→ **Regla C: scanner y UI comparten proceso y `capture_lock`.** Nada nuevo dentro del lock.

### 3.4 — 11 jun (`157bf63` → `ade5b26`): el terminal quedó inservible para el humano
Una orden VOID + F5 mete a Mocha en «ADDITIONAL MESSAGE INFORMATION», **donde ninguna tecla
funciona y hace falta re-login manual**. El intento de «recuperarse» con F6 desde dentro no
bastó; la solución fue no entrar nunca (detectar VOID en el header, antes del F5).
→ **Regla D: el recurso compartido es físico y se puede dejar roto para la persona.**
No se toca la secuencia de teclas ni el orden de los guardias — sólo los tiempos *entre* ellos.

### 3.5 — 18 jun (`4ff6c1c`, `550b594`, `85271dc`): tres caídas el mismo día, ninguna de lógica
Cliente Supabase por llamada → «Resource deadlock avoided»; el puerto 5000 perdido contra
AirPlay; `launchctl unload/load` que no reiniciaba nada.
→ **Regla E: el camino de despliegue es la mayor fuente de caídas.** Un cambio por despliegue,
`scripts/status.sh` después, y build servido == HEAD (ya lo verifica `update.sh`).

### 3.6 — 22 jun (`51843fe`): el bucle rápido multiplicó una fuga que ya estaba
Re-capturaba órdenes que ya tenía en caché (**robándole el teclado al operador** para traer algo
que ya estaba), y el auto-archive re-archivaba en cada poll hasta 48 MB / 9.210 entradas.
→ **Regla F: antes de acelerar un bucle, mirar qué crece dentro de él.**

## 4. Reglas del juego para esta tanda

1. **Cada constante de tiempo pasa a `.env`, con el valor de hoy como default.** Hoy
   `page_wait`, `poll_interval` y `refresh_timeout` están hardcodeados en la firma de
   `capture_order`: ajustar un número es un despliegue, y cada despliegue es una visita a
   Bay 2. Después de la Fase 1, revertir es editar `.env` y reiniciar.
2. **Una fase por despliegue**, con un día de logs entre medias y un número antes/después.
3. **Ninguna aceleración afloja un guardia de corrección.** Los tres que no se tocan:
   clasificar la pantalla antes de escribir, no pulsar F5 sobre un header VOID, y no paginar
   sin que la pantalla haya cambiado.
4. **Todo camino rápido tiene que fallar ruidoso y en el momento.** El centinela del
   portapapeles ya lo hace para la copia; ninguna ruta nueva puede fallar en silencio.
5. **Botón de pánico:** `AUTO_SCAN=0` para el scanner, `.env` a los valores viejos para la
   captura. Ambos sin tocar git.

## 5. Las fases, en orden de despliegue

### F0 — Instrumentar (se despliega esto solo; 0 s de ganancia) · **implementada, sin desplegar**
Sin esto, «el doble» no es verificable, y ese fue literalmente el fallo de §3.1: se cambió el
paginado sin saber cuánto tarda en refrescar el 5250 real.

- `capture_order` loguea al terminar: total ms, nº de lecturas, nº de páginas, ms esperando
  refresco, lecturas «stale».
- `MochaDriver` loguea ms por lectura.
- El scanner loguea (agregado cada N minutos, no por vuelta) cuánto tiempo estuvo pausado por
  actividad del operador.
- Se deja un día. **Salida: el número real de la Mac de Bay 2.** Si es Intel, el spawn de
  `osascript` puede costar 300-400 ms y la captura de hoy ser de 15-20 s, no de 9.

Una línea por captura, al terminar, en `logs/app-stdout.log`:

```
AS400 #881310 captured in 1.76s — 8 reads (0.42s), 3 item pages, 0.47s waiting for refresh, 3 stale reads
```

Sale también en los dos finales que importan (`stalled`, `hit the page cap`), así que una captura que
falla dice cuánto esperó antes de rendirse. Y el scanner escribe una línea **por pausa**, no por
vuelta: `auto-scan: resumed after 340s paused (the operator was on the keyboard)`.

### F1 — Timeouts y tunabilidad (0 s de ganancia; es el seguro de todo lo demás) · **implementada, sin desplegar**
- **`MochaDriver._osascript` no tiene `timeout`.** Un `osascript` colgado (un diálogo modal, un
  prompt de permisos) bloquea el hilo del scanner **con `capture_lock` tomado**: el scanner se
  queda mudo *y* las capturas manuales cuelgan detrás. `timeout=10` → `CaptureError`.
- `page_wait`, `poll_interval`, `refresh_timeout` → env vars con **los valores de hoy** como
  default (`AS400_PAGE_WAIT=0.8`, `AS400_POLL_INTERVAL=0.3`, `AS400_REFRESH_TIMEOUT=5.0`,
  `AS400_OSASCRIPT_TIMEOUT=10`), documentadas en `.env.example`. Se leen **en cada llamada**, no al
  importar, así que un `.env` cargado después gana; pasar un número por argumento sigue mandando,
  que es lo que mantiene los tests independientes del entorno.
- El número de orden se valida (son dígitos) antes de llegar al terminal, y `type_text` escapa
  comillas y barras: hasta ahora un `"` rompía el AppleScript y salía como un 500 sin explicación.
- Las llamadas a `pbcopy`/`pbpaste` también llevan timeout.

### F1b — Continuar desde la orden que ya está en pantalla (−3,1 s) · **implementada, sin desplegar**
Reportado por Rafael el 1 sep 2026: *«cuando busco una orden, aunque ya esté en AS400 y el watcher
la vea, la vuelve a buscar, retrocediendo un paso en el proceso en vez de continuar»*.

Es literal. `capture_order` lee la pantalla para clasificarla, **tira ese texto**, pulsa F6 (RETURN
TO SELECT), teclea otra vez el mismo número y vuelve a leer la misma página:

```
driver.copy_screen()   # ahí ya está la orden — sólo se usa para clasificar
driver.key("f6")       # retrocede a la pantalla de búsqueda
driver.type_text(n)    # teclea el número que ya estaba en pantalla
driver.copy_screen()   # lee otra vez la misma página
```

El rodeo cuesta F6 (0,14 + 0,8) + teclear (0,14 + 0,8) + una lectura (1,19) = **3,1 s de una
captura de 9,1 s**, más dos pulsaciones en un terminal compartido con una persona.

Ahora, si la pantalla leída **es el header de exactamente esa orden**, se continúa desde ella
(`_is_order_header_screen`, `AS400_REUSE_HEADER=0` lo apaga sin desplegar). Las dos mitades del
guardia importan:

- **el mismo número de orden** — una pantalla con la orden anterior no es la nuestra;
- **y el header, no una página de ítems.** La vista de ítems repite la misma línea
  `Order Number:`, así que continuar desde una capturaría la orden **empezando por la mitad** —
  el fallo de junio otra vez, por otra puerta. Lo que las distingue es el encabezado de columnas
  (`Quant Quant Stock # W/H`).

Todos los guardias posteriores (VOID, pantalla de mensajes, número inválido, vista equivocada)
corren sobre ese mismo texto igual que antes: no se salta ninguno, sólo el viaje de ida y vuelta.

A quién beneficia: **a la captura manual**, que es donde Rafael lo ve (mira la orden en Mocha y la
captura desde la UI). El scanner casi nunca entra por ahí — al terminar una orden la pantalla queda
en su última página de ítems, y para la siguiente el número no coincide.

Verificación: 6 tests nuevos, incluidos los tres que importan — no continuar desde otra orden, no
continuar desde una página de ítems de la misma orden, y una orden VOID en pantalla sigue saltando
**antes** del F5. Suite entera: 357 verdes, ruff limpio.

### F1c — Salir sola de una pantalla atascada · **implementada, sin desplegar**
Aporte de Rafael el 1 sep 2026: *«creo que aún nos quedamos trabados a veces. Para salir al menú
principal es presionando primero F6 dos veces y después F7, desde cualquier menú en el que se esté»*.

Hasta ahora, una pantalla que `classify_screen` no reconocía terminaba en
`AS400ManualLoginRequired`: el daemon se callaba y esperaba a que alguien caminara hasta Bay 2. Con
la receta del operador puede desatascarse solo. `bootstrap_session` la intenta **una vez** por
arranque (`unstick_to_menu`); si tras eso la pantalla sigue sin reconocerse, entonces sí hace falta
una persona.

Dos guardias, y los dos vienen de la historia:

- **Nunca sobre la pantalla-callejón.** En `ADDITIONAL MESSAGE INFORMATION` no funciona ninguna
  tecla (11 jun 2026): ahí se detecta y se pide re-login, sin pulsar nada. Teclear en una pantalla
  que no puede contestar es exactamente lo que costó aquel corte.
- **Un solo intento.** Una pantalla que no cambia no se aporrea; el bucle tiene tope y el segundo
  paso ya no reintenta.

De paso, **CUSTOMER DISPLAY deja de ser «desconocida»**: es la opción 01 del menú, el operador puede
dejar el terminal ahí, y ahora se reconoce y se sale con `Cmd7` (su propia legenda), sin gastar el
intento genérico.

### F1d — Una página sin líneas no es una orden vacía · **implementada, sin desplegar**
Rafael pulsó ENTER después de `END OF ORDER` y mandó lo que salió: **la misma pantalla, sin líneas,
con el `END OF ORDER` y el total todavía puestos**. Esa pantalla tiene la forma exacta que el
scanner usaba para decidir «orden VOID/vacía → saltar este número **para siempre**». Sobre la 880996
habría descartado en silencio una orden real de 3.965,50 $.

Ahora el salto exige además que el encabezado esté de acuerdo: `not total_mismatch`. Una orden VOID
no trae Sub-Total y sigue saltándose; una real lo trae, la suma no cuadra y el número se reintenta.

**Es prerrequisito de F4, no un extra.** El loop se para en el primer `END OF ORDER`, así que hoy no
debería llegar a esa pantalla — pero **una página de ítems a medio pintar se lee igual**, y F4
consiste precisamente en leer antes. Sin este guardia, acelerar la lectura convierte un frame a
medio pintar en un número saltado para siempre, en silencio.

### F2 — Una sola llamada a `osascript` por lectura · **implementada, sin desplegar**
Un script que hace las tres cosas: activar Mocha si hace falta, Cmd+A, `delay`, Cmd+C.
De 3 procesos a 1 por lectura.

Además **cierra un agujero real**: hoy pasan ~140 ms entre el Cmd+A y el Cmd+C, en procesos
distintos; si el foco cambia en medio, el Cmd+C se ejecuta en otra app. Menos ventana, no más.

**Medido en esta Mac** (mismos Apple events, sin pulsar teclas): una lectura pasa de
**1.259 ms a 529 ms**, es decir **−730 ms por lectura** — más de lo estimado. Con 4 lecturas en una
orden de 2 páginas son **~2,9 s**.

Verificación: la suite entera verde, y **los scripts se compilan dentro de los tests**
(`osacompile`, que compila sin ejecutar — ejecutarlos robaría el teclado de la máquina donde corra
la suite). Ese test ya sirvió: `tell application id "…"` se resuelve **al compilar**, así que en una
máquina sin ese bundle instalado no fallaba la activación, fallaba **la lectura entera**. La
activación por bundle id ahora se resuelve en tiempo de ejecución y, si el emulador no está
corriendo, el error es una frase en vez de un error de sintaxis.

### F3 — No re-enfocar en cada lectura · **implementada, sin desplegar** (va dentro de F2)
`copy_screen` llamaba a `focus()` siempre: un System Events (140 ms) + `sleep(0.4)`. Dentro del
script único de F2 se comprueba `frontmost` y sólo se activa cuando no lo está — y **el settle se
espera dentro del script**, así que un emulador que ya estaba delante no cuesta ningún sleep. Los
730 ms medidos arriba son las dos fases juntas: no se pueden separar, porque son el mismo script.

Riesgo: si Mocha no está al frente y no lo detectamos, Cmd+A/Cmd+C copian **otra app** → texto
basura. Tres guardias ya existentes lo atrapan, ninguno se afloja: el centinela del portapapeles,
`classify_screen` → UNKNOWN, y la validación de la captura (número + ítems + END OF ORDER).

### F4 — Esperar a la pantalla, no al reloj (−3 s; el grueso, y el paso delicado)
Hoy: `sleep(0.8)` y copiar. Nuevo: copiar ya, y repetir hasta que la pantalla **(a) sea distinta
de la anterior y (b) se lea igual dos veces seguidas**.

Las dos condiciones juntas son **más fuertes que el sleep de hoy**, no más débiles:

- (a) es la invariante que estableció §3.1 — no paginar sobre un frame viejo. Se mantiene tal cual.
- (b) es lo que un `sleep(0.8)` nunca garantizó: 800 ms no significan que el 5250 terminó de
  pintar, dos lecturas idénticas sí. Sin (b), quitar el sleep abre el fallo simétrico del de
  junio: aceptar un frame **a medio pintar** y perder las líneas que faltaban por dibujar.

**Y el detalle que convertiría esta mejora en el siguiente corte:** hoy el contador de timeout
suma sólo `poll_interval` (`waited += 0.3`), pero cada relectura cuesta ~1,2 s de reloj. O sea
que `refresh_timeout=5.0` en la práctica tolera ~17 relecturas ≈ **25 s** de AS400 lento. Si las
lecturas bajan a 0,3 s y el contador se queda igual, esa tolerancia cae a ~10 s **sin que nadie
lo haya decidido** → más capturas `incomplete` → 5 minutos de espera por orden y el scanner
atrás. Por eso el deadline pasa a ser **de reloj explícito**, arrancando en 25 s
(`AS400_REFRESH_TIMEOUT=25`): igual de tolerante que hoy, y se aprieta después con los datos.

**Prerrequisito:** F1d. Un frame a medio pintar tiene la misma forma que una orden vacía, y esa
forma hace que el scanner salte el número para siempre.

**Lo que hay que tener antes** no es una captura de una orden multipágina —el paginado ya funciona:
26 de 798 órdenes de los últimos 90 días pasan de una página y todas entraron completas— sino el
**tiempo real de repintado** entre páginas, que sólo sale del log de F0. Un texto pegado no lleva
tiempos; el log sí, y ve todas las transiciones de todas las órdenes.

Verificación: `LaggyTerminalDriver` (tests/test_as400_capture.py) hoy expresa el lag **en número
de lecturas**, que ya no significa lo mismo cuando una lectura cuesta 4× menos. Pasa a lag **en
segundos**, con casos 0 / 0,5 / 2 / 6 s y uno que no refresca nunca.

### F5 — El ritmo alrededor de la captura (sólo `.env` + un memo local)
- `SCAN_FOUND_DELAY_SEC` 5 → 1. La Mac de Bay 2 está casi siempre libre (confirmado por Rafael,
  1 sep 2026) y el gate de 60 s de inactividad sigue protegiendo al operador.
- `scanned_store.load()` parsea el JSON entero (con el `raw_text` de cada orden dentro) en **cada**
  `get()` y `next_scan_number()`; el bucle de skip puede hacerlo hasta 500 veces por paso y
  `/api/orders` lo toca cada 8 s. Memo por `mtime`. Es la Regla F: con la captura 2× más rápida el
  bucle da el doble de vueltas.

## 6. Lo que NO se hace en esta tanda, y por qué

- **No se toca la detección de VOID, el F5, ni `classify_screen`.** Son los frenos que costaron
  dos cortes (§3.4). Cambian los tiempos entre teclas, nunca las teclas.
- **No se paralelizan capturas ni se abre una segunda sesión de Mocha.** El teclado, el foco y el
  portapapeles son globales y de un solo dueño; la concurrencia es lo que rompe aquí.
- **No se reescribe a lectura por protocolo TN5250** (sin teclado, sin foco, sin portapapeles).
  Es la respuesta de verdad a largo plazo, pero es otro proyecto: ver ❓3.
- **No se retira `watcher.py` ni la carpeta de PDFs**, aunque lleve sin usarse desde el 16 jul
  (0 imports por esa vía en 45 días; 813 de 871 entran por «Send to PickD»). Quitar código y
  acelerar en la misma tanda borra la posibilidad de aislar una regresión.
- **No se toca `pnpm`/PickD.** Ninguna fase cambia el esquema ni el cliente de Supabase.

## 7. Preguntas abiertas (❓ con su default)

1. ❓ **¿Cómo se despliega cada fase?** *Default:* desde la UI de Bay 2 (⋯ → ⟳ Update app), con
   Rafael delante, y `scripts/status.sh` después para confirmar que el build servido == HEAD.
   No hay acceso remoto a esa Mac.
2. ❓ **¿Cuánto se le tolera a un AS400 lento?** *Default:* 25 s, que es lo que tolera hoy sin
   que nadie lo escribiera. Se aprieta con los datos de F0, no antes.
3. ❓ **¿Leer el 5250 por protocolo algún día?** *Default:* **no ahora.** IT ya negó DB2/ODBC
   (`docs/AS400_AUTOMATION.md` §2). Telnet 23 es otra vía y esa Mac ya llega, pero necesita
   credenciales propias, permiso, y confirmar que no consume una licencia de sesión. Si algún día
   entra, es lo único que elimina de raíz el robo de teclado — el scanner podría capturar mientras
   el operador trabaja.
4. ❓ **¿Se aprovecha para bajar `SCAN_NOT_FOUND_WAIT_SEC` (20 min)?** *Default:* **no en esta
   tanda.** Es la palanca más grande del reloj *total* (una orden nueva tarda de media 10 min en
   aparecer), pero Rafael pidió la captura; mezclarlo estropea la medición de esta.

## 8. Decisiones fechadas

- **1 sep 2026** — Rafael: «el doble de rápido» = **la captura en sí** (~10-15 s por orden y el
  rato de teclado robado), no la latencia de descubrimiento ni el Send.
- **1 sep 2026** — Rafael: la MacBook de Bay 2 está **casi siempre libre**. El gate de
  inactividad de 60 s no está matando al scanner, así que no se toca.
- **1 sep 2026** — Rafael reporta que la captura retrocede a la pantalla de búsqueda aunque la
  orden ya esté delante. Confirmado en `capture_order` y corregido (F1b): la pantalla ya leída se
  reutiliza cuando es el header de esa misma orden. Es −3,1 s de −4,9 s del objetivo total.
- **1 sep 2026** — F2 y F3 escritas: son un solo script, así que se implementan y se despliegan
  juntas. Medida en esta Mac: 1.259 ms → 529 ms por lectura. El test de compilación con `osacompile`
  atrapó que `tell application id` se resuelve al compilar; la activación por bundle id pasó a
  resolverse en tiempo de ejecución.
- **1 sep 2026** — F0 y F1 escritas y commiteadas. Ya son cinco fases en el repo y **cero
  desplegadas**: el orden de despliegue (1: F0+F1 · 2: F1b+F1c+F1d · 3: F2+F3 · 4: F4) importa
  precisamente porque el número base tiene que salir antes de que F1b lo cambie.
- **1 sep 2026** — Rafael manda dos órdenes reales completas (881310 y 880996) «para que no tomes
  esa anterior como patrón repetitivo definitivo». Entran como fixtures y de ahí salen F1d, un bug
  del parser (`Ship Via` vacío se tragaba la columna vecina), `R&L` como transportista de camión, y
  el mapa de pantallas `docs/as400-screen-map.md`.
- **1 sep 2026** — Medido contra prod: 813 de 871 órdenes de los últimos 90 días entran por
  «Send to PickD» de la UI; la carpeta de PDFs no recibe nada desde el 16 jul. El daemon vivo es
  `app.py` + `auto_scanner`; `watcher.py` está de hecho retirado (pero se deja, ver §6).
