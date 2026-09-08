# PRD: El catálogo de las bicis, llenado en los huecos del escáner

**Estado:** PROPUESTA — decisiones de Rafael tomadas en sesión el 2026-09-02, sin implementar
· **Fecha:** 2026-09-02 · **Autor:** Rafael + PickD · **Backlog:** idea-176 (por crear)
· **Relacionado:** `docs/customer-enrichment.md` (pelea por el mismo hueco), `docs/as400-screen-map.md`
§2.4 y §6, `docs/capture-speed-plan.md`, `auto_scanner.run_scan_step`, `maintenance.ACTIONS`,
`src/utils/fedexCarton.ts`, `src/features/reports/utils/fedexDimensions.ts`, `bug-018`

---

## 1) Contexto y problema

El escáner de órdenes está parado casi todo el día. Con **9,5 órdenes por día hábil** (208 en los
últimos 30 días, medido en prod hoy) y capturas de ~6 s, el terminal trabaja **un minuto por
jornada**; lo demás es `not_found` cada 20 minutos. Ese hueco ya tiene un candidato escrito
—las fichas de cliente, `docs/customer-enrichment.md`, 1 sep— y ahora un segundo: traer del AS400
el nombre de catálogo de las bicis que en PickD no lo tienen.

El motivo declarado era el export de dimensiones a FedEx: `model` es su llave de agrupación, y sin
él la caja no sale en el archivo (`bug-018`).

**La medición de hoy dice que ese motivo, hoy, no se sostiene** — y eso reordena el PRD entero.
Ver §3. Lo que sí aparece, y no estaba en el pedido, es otra cosa: el AS400 tiene un **peso por
SKU**, y 187 de las 264 bicis con demanda real llevan las 45 lb que escribe el trigger.

## 2) Objetivo

Que ninguna bici que PickD tiene o mueve se quede sin **nombre de catálogo partido**
(`model` / `size` / `color`) ni con **el peso genérico del trigger**, usando tiempo del terminal
que hoy se tira, y **sin quitarle un segundo de teclado al operador ni una orden al escáner**.

**Métrica:** cero bicis con stock y sin `model`; y la cuenta de bicis de la cola de medición
(`get_bike_demand_ranking`) que siguen a 45 lb sin verificar, bajando desde 187.

## 3) Lo que dice prod hoy (2026-09-02)

Medido con `PROD_DB_URL` + node/`postgres`. Los números del pedido eran de otra pasada; estos son
los de hoy y son los que manda el diseño.

### 3.1 El `model` no es el cuello de botella que parecía

| | |
|---|---|
| SKUs de bici (`is_bike`) | 830 |
| …sin `model` | **54** |
| …de esos, con forma de SKU de catálogo `DD-NNNN[CCC]` — lo único que el AS400 sabe buscar | **20** |
| …guías de FedEx/USPS (12+ dígitos) | **19** — *nunca* deben recibir modelo |
| …seriales (`Y22A016211`, `G220509521`, `M21I014707`…) y un `S/D06-4482BL` | **15** |
| **…sin `model` que además estén medidos**, o sea que el modelo les desbloquee el export | **5 — y los cinco con stock 0** |

**El orden del gate lo explica.** `fedexCartonGap` (`src/utils/fedexCarton.ts:66`) comprueba
`dimensions_verified` **antes** que `model`: una caja sin medir ya está fuera del archivo, tenga
modelo o no. Consecuencia dura:

> **Rellenar `model` desde el AS400 hoy cambia exactamente 0 filas del export a FedEx.**
> Lo que bloquea el archivo es la cinta métrica, no el nombre — y la cinta métrica el AS400 no la
> tiene (§4.3). El `model` sólo empieza a valer el día que alguien mide la caja; entonces sí, la
> fila pasa de `unverified` a exportable en vez de a `no_model`.

Y `get_bike_demand_ranking()` —las **264** bicis con stock, no-S&D, pedidas en 12 meses, que es la
lista que alimenta MeasureCartonsScreen— devuelve **0 filas sin `model`**. Toda bici que a la vez
está en stock y se mueve ya tiene nombre. Las 54 son guías, seriales, o stock muerto.

### 3.2 Doce de esos veinte ya tienen el nombre dentro de PickD

`picking_lists.items[].description` guarda la descripción que el propio watchdog capturó del AS400:

| SKU | Lo que ya está en PickD | Fuente |
|---|---|---|
| `03-4865GY` | `HUDSON E1 19 2026 DAKOTA GREY` | descripción de orden |
| `03-3803BL` | `VENTURA A2 L48 2025 BLUE VAPOR` | descripción de orden |
| `03-3994BL` | `CITIZEN 1 17 2025 DEEP BLUE` | descripción de orden |
| `02-3680GY` | `PORTAL A2 17 2025 STORM GREY` | descripción de orden |
| `07-3680PD` | `JUV X.24 DISC 2025 PALLADIUM` | descripción de orden |
| … | (12 en total, más 1 sólo por `inventory.item_name`) | |

**Reparto de los 20 de forma de catálogo:** 12 con descripción de orden, 1 sólo con `item_name`
(`03-3709BL` → `KOMODO 29 Riptide`), y **7 sin ninguna fuente gratis** — de esos 7, **2 están en el
piso**: `03-3492BL` (ROW 32) y `03-4473BK` (ROW 37). Esos 7 son el trabajo real del AS400 hoy.

### 3.3 …pero la descripción de la orden viene cortada a 30 caracteres

Sobre las 935 descripciones distintas de los últimos 12 meses: el **largo máximo es exactamente 30**
y **319 (34 %) miden exactamente 30**. Un tercio del catálogo llega mutilado:

```
orden:          EXPLORER A2 19 2025 GLOSS BLAC     ← 03-4070BK, cortado
Stock Inquiry:  CODA S2 L16 2026 GLOSS BLACK       ← 28 caracteres, sobrevivió entero
```

Ese corte es lo que le da valor a la pantalla del AS400 **más allá de los 7 SKUs**: si su campo
`Description` es más ancho, es la única fuente de un nombre completo para un tercio del catálogo.
No está demostrado que lo sea — el ejemplo que tenemos cabe en 30. Es ❓Q1, y se resuelve con un
Peek sobre `03-4070BK`.

### 3.4 El peso: donde de verdad hay volumen

| | |
|---|---|
| Bicis con stock y peso **sin verificar** | **445** |
| …de ellas, exactamente en las 45 lb del trigger | **441** |
| Dentro de la cola de demanda (264 filas): a 45 lb sin verificar | **187** |
| Bicis alguna vez **pesadas en báscula** (`weight_verified`) | 93 (media 43,7 lb; rango 1–80) |
| Bicis con stock **sin medir** (`dimensions_verified = false`) | 302 · dentro de la cola de 264: **100** |

## 4) La pantalla: `02. Stock File Inquiry`

Estaba marcada «❓ sin explorar» en `docs/as400-screen-map.md` §2.4. **Rafael la abrió y la pegó el
2 sep 2026**; esto es su captura, no una suposición. Entra al mapa como pantalla verificada.

### 4.1 El recorrido, tecla a tecla

```
menú SALESN ──2 + ENTER──▶ Stock Inquiry
   ──parte numérica del SKU sin guion──▶ TAB
   ──código de color de 2 (o 3) caracteres──▶ TAB
   ──X + ENTER──▶ S T O C K   I N Q U I R Y   (leer aquí)
   ──F6 · F6 · F7──▶ menú ──3 + ENTER──▶ búsqueda de orden
```

`03-3933BK` → se teclea `033933`, TAB, `BK`, TAB, `X`. La pantalla lo repinta como
`Stock Number: 03 3933 BK`. **El mapeo desde la grafía canónica de PickD es mecánico**: quitar el
guion da los seis dígitos, y las letras van al segundo campo. `canonical_sku` ya admite 0–3 letras
(`parser.py:31`), que es exactamente el «2 dígitos, o 3 para casos especiales» de Rafael.

### 4.2 Lo que trae

```
  Stock Number: 03 3933 BK      B-Bike/P-Part: B    Model Year: 2025
  Description:  CODA S2 L16 2026 GLOSS BLACK

     Inventory  NJ       FL       CA                 Price  Quantity
  On Hand       56        0        0       Each    380.95         49
  ...
  Unit Meas:   EA
  First Cost                Bin Location:  1        Status Code:
  Freight                 Stock Location:
  Duty %                          Weight:    36
```

| En pantalla | Ejemplo | Qué es para PickD |
|---|---|---|
| `Description` | `CODA S2 L16 2026 GLOSS BLACK` | **el dato que buscábamos** → `model`/`size`/`color` vía `parseBikeName` |
| `Weight` | `36` | candidato para `weight_lbs` — **no es báscula**, ver §4.3 |
| `B-Bike/P-Part` | `B` | la clasificación **autoritativa**; hoy PickD la adivina con un trigger por prefijo |
| `Model Year` | `2025` | el año, que `parseBikeName` descarta a propósito |
| `On Hand NJ/FL/CA` | `56 / 0 / 0` | otra fuente de verdad del stock — fuera de alcance, §5 |
| `Vendor No`, `Bin Location`, `Unit Meas`, precios por nivel | | fuera de alcance |
| **largo / ancho / alto** | **no está** | **la pantalla no trae medidas** |
| Teclas | `Cmd10 NOTES`, `Cmd7 EXIT` | `Cmd10` sin explorar |

### 4.3 La calibración que cambia el diseño del peso

Único par de datos que tenemos de la misma bici en las dos fuentes:

| `03-3933BK` | AS400 Stock Inquiry | PickD |
|---|---|---|
| Nombre | `CODA S2 L16 2026 GLOSS BLACK` | `item_name` = `CODA S2 L16 2025 GLOSS BLACK` |
| `model` / `size` | — | `CODA S2 L16` / **NULL** ← la talla se comió el modelo |
| **Peso** | **36** | **33,6 con `weight_verified = true`** (báscula) |
| Stock | `On Hand NJ 56` | 56 en ROW 1 ✅ cuadra clavado |

**El peso del AS400 no es el de la báscula**: 36 contra 33,6, un 7 % arriba. Puede ser neto, o
nominal de catálogo. Es mejor que un 45 genérico y peor que una lectura real, y el diseño lo trata
exactamente así (R4). El año tampoco coincide entre las dos pantallas del propio AS400
(`Description` dice 2026, `Model Year` dice 2025), lo cual da igual porque el año se tira.

## 5) Alcance

**Dentro:** `model`, `size`, `color` y `weight_lbs` de SKUs de bici; el backfill desde lo que PickD
ya tiene; el paso de AS400 en el hueco del escáner; la cola y su orden; la convivencia con el
escáner y con las fichas de cliente.

**Fuera, a propósito:**
- **Medidas de caja.** La pantalla no las trae (§4.2). Las 100 cajas sin medir de la cola siguen
  siendo trabajo de cinta métrica en MeasureCartonsScreen. Este PRD no las toca.
- **`On Hand` contra `inventory`.** Es una segunda fuente de verdad del stock y merece su propio
  PRD (conteo cíclico), no una escritura de paso.
- **`B-Bike/P-Part` para corregir `is_bike`.** Es autoritativo y tentador, pero cambiar `is_bike`
  mueve defaults de peso y medidas de golpe. Se **loguea** en F2 y se decide con datos.
- **Guías de FedEx Returns y seriales.** Ver R6.
- **Reparar splits ya existentes** (`model = 'CODA S2 L16'`, `size` NULL). Es sobrescribir la llave
  del export sin red — ❓Q4.
- **Órdenes, clientes, `Cmd10 NOTES`, opciones 04/06/10 del menú.**

## 6) Las reglas de escritura

Rafael, 2 sep 2026: **directo a `sku_metadata`, sólo si el hueco está vacío.** Mismo pacto que
`customers.phone` y el sellado de `as400_account`: rellenar un hueco es seguro, pisar lo que
escribió una persona no. Sin tabla intermedia y sin confirmación manual.

| Columna | Se escribe cuando | Nunca |
|---|---|---|
| `model`, `size`, `color` | los tres están NULL o vacíos | si `model` ya tiene algo |
| `weight_lbs` | `weight_verified = false` | si `weight_verified = true` |
| `weight_verified` | **jamás se pone en `true`** por este flujo | — |
| `is_bike`, `dimensions_*`, precios, stock | — | **nunca** |

**El hueco del peso es `weight_verified = false`, no `weight_lbs IS NULL`** (decisión de Rafael, 2
sep): la columna nunca está NULL porque el trigger escribe 45 en toda bici, así que el 45 es un
marcador de posición y no el dato de nadie. Una lectura de báscula no se toca jamás.

**Y una regla que no se negocia:** si `Stock Number` de la pantalla no es el SKU que pedimos, **no
se escribe nada**. Es la única defensa contra haber aterrizado en otra ficha, y es la misma que ya
gobierna las fichas de cliente.

## 7) Requerimientos funcionales

- **R1 — Backfill desde lo que ya tenemos, sin AS400.** Una acción nueva en `maintenance.ACTIONS`
  («Backfill bike catalog names») con Preview/Apply, que la UI pinta sola. Para cada bici sin
  `model`: toma la descripción **más reciente** de `picking_lists.items[].description`, la parte con
  la misma regla que `buildNewSkuPrefill`/`parseBikeName`, y escribe `model`/`size`/`color` sólo si
  el nombre **parte de verdad** (si no hay talla, no se escribe nada: un nombre entero dentro de
  `model` es la basura legacy de `bug-018`). `inventory.item_name` sólo como respaldo y sólo si
  parte — está más sucio (`07-3606GP` guarda `"P"`, `03-4865GY` guarda `"17 / Hudson E1 19 Dakota
  Grey"`). Cero AS400, cero teclado, cero viaje a Bay 2.
- **R2 — Un paso de AS400 en el hueco.** Cuando `run_scan_step` devuelve `not_found`, en vez de
  dormir 20 minutos enteros se hace **un** SKU y se vuelve a dormir lo que tocaba. Entra por el
  mismo `capture_lock` y respeta el mismo gate de inactividad de 60 s.
- **R3 — La vuelta es parte del paso.** El paso no termina hasta que una lectura confirme que el
  terminal está en la búsqueda de órdenes. Si no lo está, se aplica `F6·F6·F7` → menú → `3`, y
  **hasta 3 intentos antes de rendirse** (Rafael, 2 sep). Al cuarto, el escáner se declara
  `unavailable` y no toca el terminal hasta el próximo `bootstrap_session`.
- **R4 — El peso entra como estimación, nunca como medida.** `weight_lbs` del AS400 sólo sobre
  `weight_verified = false`, dejando `weight_verified` en `false`. Un `?` ámbar ya delata en la UI
  un peso no verificado, así que la señal de «esto no lo pesó nadie» se conserva intacta.
- **R5 — Orden de la cola.** Por valor, y se recalcula en cada paso: (1) bicis **con stock en el
  piso** sin `model`; (2) el resto sin `model` que tengan forma de SKU de catálogo; (3) bicis de la
  cola de demanda (`get_bike_demand_ranking`) con el peso sin verificar, en el orden de la propia
  RPC — más pedidas primero.
- **R6 — Lo que nunca entra a la cola.** Un SKU cuya grafía no case `^\d{2}-\d{4}[A-Z]{0,3}$`:
  las 19 guías de FedEx/USPS y los 15 seriales. El AS400 no los conoce y una guía con modelo
  entraría a la tabla Dimensions de FedEx como si fuera un cartón. **La forma del SKU es el filtro,
  no una lista de excepciones.**
- **R7 — Un SKU que el AS400 no encuentra se marca y no se vuelve a pedir.** Se anota en el
  almacén local del watchdog (junto a `.scanned_orders.json`), con fecha, y no vuelve a la cola.
  Sin esto la cola se atasca en el primer SKU que el ERP no tenga.
- **R8 — Todo paso deja línea de log**, con el mismo formato de tiempos que `capture_order`
  (`AS400 SKU 03-3492BL … en N,NNs — N reads`), porque es lo único que dirá si el paso encarece el
  hueco.
- **R9 — Ningún cambio en la grafía de SKUs.** Este flujo *lee* `canonical_sku` y no la redefine,
  así que los tres espejos (SQL, TS, `parser.py`) y sus tres tablas de casos **no se tocan**.

## 8) Fases

- **F1 — El backfill gratis (sin deploy de lógica de terminal).** R1. Toca 12 SKUs de forma de
  catálogo + 1 por `item_name`, y 10 más de forma no-catálogo si el nombre parte. Se ejecuta desde
  ⋯ → Maintenance, primero *Preview*. **Es lo primero porque no cuesta un viaje a Bay 2 y porque
  deja al AS400 sólo el trabajo que nadie más puede hacer.**
- **F2 — Leer del AS400 y no escribir.** El recorrido de §4.1 sobre los **7 SKUs sin fuente**,
  parseando y **logueando** lo que habría escrito. Cero escrituras. Un día así dice si el recorrido
  es fiable antes de que toque la base de datos, y de paso contesta ❓Q1 (¿se corta la descripción?),
  ❓Q2 (¿qué hace el campo de color vacío?) y ❓Q6 (`B-Bike/P-Part`).
- **F3 — Escribir el nombre.** R2–R7 sobre `model`/`size`/`color`. La cola son 7 SKUs: con ~25
  huecos al día se vacía en una jornada.
- **F4 — El peso.** R4 sobre la cola de demanda, 187 filas. Es la fase con volumen real, y va la
  última a propósito: entra cuando el recorrido ya lleva días sin romper nada.

## 9) La cadencia, y quién gana el hueco

**Un paso por hueco, nunca una ráfaga** (decisión del 10 jun 2026, intacta). No hay presupuesto de
«5 minutos»: con ~25 huecos al día y una cola de 7 SKUs, un SKU por hueco la vacía en un día, y un
presupuesto por minutos sólo añadiría estado que persistir y una forma nueva de pelear el teclado.

**Órdenes y SKUs se revisan intercalados** (Rafael, 2 sep): el hueco no reemplaza la búsqueda de la
orden siguiente, se mete dentro de ella. La secuencia de cada ciclo es *intentar la orden* →
`not_found` → *un SKU* → *volver a la búsqueda* → dormir. La orden siempre va primero: el escáner
existe para eso.

**Contra las fichas de cliente: bicis primero, hasta vaciar** (Rafael, 2 sep). La cola de bicis es
finita y corta (7 SKUs, más 187 de peso en F4); la de clientes son 628 filas que no se acaban
nunca. Vaciar lo corto primero deja el hueco libre para lo largo y evita dos recorridos de pantalla
vivos a la vez. `docs/customer-enrichment.md` arranca cuando esta cola llegue a cero, y su §2 no
cambia — sólo su momento.

**Y las tres restricciones que nada de esto puede romper:** el gate de inactividad de 60 s no se
toca; el `capture_lock` es el mismo; y si el operador agarra el teclado a mitad, se abandona
**después** de devolver el terminal a la búsqueda de órdenes, no en medio.

## 10) Qué se retunea con `.env` y qué exige un viaje a Bay 2

Cada cambio del watchdog cuesta un viaje, y ya hay commits pendientes de llevar. Lo que este diseño
deja retuneable **con editar `.env` y reiniciar el LaunchAgent**, sin desplegar código:

| Variable | Para qué | Default |
|---|---|---|
| `SKU_ENRICH` | apagar el paso entero sin tocar el escáner | `1` |
| `SKU_ENRICH_MAX_PER_GAP` | SKUs por hueco (la decisión es 1; la palanca existe por si el hueco resulta más barato de lo medido) | `1` |
| `SKU_ENRICH_WEIGHT` | separar F4 de F3 sin desplegar | `0` hasta F4 |
| `AS400_UNSTICK_TRIES` | los 3 intentos de `F6·F6·F7` de R3 | `3` |

Se leen **en tiempo de llamada**, como los tunables de `as400_capture` — no en el import, o un
cambio en `.env` exigiría el deploy que intenta evitar.

**Exige deploy:** el recorrido de §4.1, el parseo de la pantalla, la cola de R5, el marcado de R7,
y —ojo— **R3, porque `unstick_to_menu` es código compartido con la captura de órdenes**: pasar de
un intento a tres cambia el comportamiento del escáner en sus propios atascos, no sólo en éste. Es
el único cambio de esta tanda que toca una ruta ya en producción, y quiere su propio test.

## 11) Riesgos y frenos

| Riesgo | Freno |
|---|---|
| Aterrizar en otro SKU y escribir su nombre | Se compara `Stock Number` con el pedido antes de escribir (§6) |
| Quedarse fuera de la búsqueda de órdenes | R3: se comprueba por lectura, `F6·F6·F7` hasta 3 veces, luego `unavailable` |
| Robarle el teclado al operador | Mismo `capture_lock` y mismo gate de 60 s; se sale por la búsqueda, no a media pantalla |
| Perder capturas de órdenes | Sólo tras un `not_found`, y un SKU por hueco |
| **Una escritura mala no tiene red** (service_role se salta RLS) | Sólo sobre huecos vacíos (§6); F2 entero sin escribir; F1 con Preview/Apply |
| Un nombre cortado a 30 entra como `color` truncado (`GLOSS BLAC`) | ❓Q1 en F2; si el AS400 corta igual, F1 y F2 comparten el defecto y el `color` truncado es preferible a NULL — pero se decide con la evidencia, no ahora |
| Meter una guía de FedEx en la tabla Dimensions | R6, por forma del SKU |
| Entrar en una pantalla nueva sin querer | `classify_screen` antes de teclear, más un `STATE_STOCK_INQUIRY` nuevo con su marcador `STOCKINQUIRY` |
| El paso encarece el hueco sin que se note | R8 |

## 12) ❓ Preguntas abiertas — cada una con lo que se hará si la respuesta es «ok»

| # | Pregunta | Propuesta por defecto |
|---|---|---|
| ~~Q1~~ | ~~¿El `Description` de Stock Inquiry se corta a 30 caracteres?~~ | **CONTESTADA el 2 sep — §15.** No se corta: da el nombre completo. |
| ~~Q2~~ | ~~Los 126 SKUs de bici sin sufijo de color: ¿qué acepta el campo de 2 caracteres?~~ | **CONTESTADA el 2 sep — §15.** Se pulsa TAB directo, sin llenarlo. |
| Q3 | El `Weight: 36` contra los 33,6 de báscula, ¿es peso **neto** de la bici o **de la caja**? | Se trata como estimación y nunca se marca verificado (R4). Cuando F4 lleve ~20 SKUs que además tengan báscula, la comparación contesta sola si es un sesgo constante. |
| Q4 | ¿Se arreglan los splits malos ya existentes (`model='CODA S2 L16'`, `size` NULL)? | **No.** Es sobrescribir la llave de agrupación del export sin red. Es un PRD aparte, con Preview/Apply y revisión humana. |
| Q5 | Un SKU con `model` pero sin `color`, ¿se completa el color solo? | **No** en esta tanda: los tres campos salen de partir un nombre, y escribir uno sin los otros mezcla dos fuentes en una misma fila. Se escribe el trío o nada. |
| Q6 | `B-Bike/P-Part` es autoritativo y PickD adivina `is_bike` por prefijo. ¿Se corrige? | Sólo **loguear** las discrepancias en F2 y F3. Cambiar `is_bike` mueve defaults de peso y medidas de golpe; si el log muestra que hay discrepancias reales, es su propio PRD. |
| Q7 | ¿Se refresca un SKU ya leído? | **No.** Una vez leído no se vuelve. Si un nombre cambia, se corrige a mano. |
| Q8 | Las 19 guías de FedEx tienen `is_bike = true` (el trigger las adivinó por prefijo). ¿Se limpian? | Fuera de este PRD, pero **anotado**: una etiqueta de retorno no es una bici, y hoy arrastra los defaults de una (45 lb, 55×8,5×30,5). |

## 13) Criterios de aceptación

1. **F1 Preview** sobre prod lista 12 SKUs de forma de catálogo con su nombre partido, y
   `03-4865GY` sale como `HUDSON E1` / `19` / `DAKOTA GREY` — sin el año.
2. **F1 no escribe** en ningún SKU cuyo nombre no parta en modelo + talla; `07-3606GP` con
   `item_name = "P"` no se toca.
3. **F1 Apply** es idempotente: una segunda pasada no cambia nada.
4. **F2** captura la pantalla de `03-3492BL` y loguea lo que habría escrito, con el terminal de
   vuelta en la búsqueda de órdenes y **cero** escrituras en la base.
5. Con `Stock Number` distinto del pedido, **no se escribe nada** y el paso lo dice en el log.
6. Ningún SKU de forma no-catálogo (`792270149760`, `Y22A016211`, `S/D06-4482BL`) entra nunca a la
   cola — con test sobre esa tabla de casos.
7. Un SKU que el AS400 no encuentra queda marcado y no reaparece en el paso siguiente.
8. Con el terminal atascado, `F6·F6·F7` se intenta **3** veces; a la cuarta el paso devuelve
   `unavailable` y no vuelve a teclear.
9. **`run_scan_step` sigue siendo pura** y sus tests con driver falso siguen pasando sin tocarlos;
   el paso nuevo se prueba igual, con su propio driver falso.
10. **F4:** una bici con `weight_verified = true` no cambia de peso ni aunque el AS400 diga otra
    cosa; una a 45 lb sin verificar sí, y sigue con `weight_verified = false`.
11. El operador toca el teclado a mitad de un paso: el terminal termina en la búsqueda de órdenes,
    y la orden siguiente se captura en el ciclo siguiente sin pérdida.

## 14) Decisiones fechadas

- **2 sep 2026** — Rafael abre `02. Stock File Inquiry` y pega la pantalla real (`03-3933BK`,
  CODA S2). Cierra la ❓ más cara del mapa de pantallas y aporta el recorrido tecla a tecla. La
  pantalla entra a `docs/as400-screen-map.md` §2.12 como verificada, con su fixture.
- **2 sep 2026** — Medición en prod que **reordena el PRD**: el `model` no desbloquea ninguna fila
  del export porque `dimensions_verified` es el primer gate, y 12 de los 20 SKUs direccionables ya
  tienen su nombre dentro de PickD. El backfill gratis (F1) pasa delante del paso de AS400.
- **2 sep 2026** — Rafael: el peso del AS400 se trae pero **nunca se marca verificado**; el hueco
  del peso es `weight_verified = false`, no `weight_lbs IS NULL`, porque el trigger escribe 45 en
  toda bici y ese 45 no es el dato de nadie.
- **2 sep 2026** — Rafael: escritura **directa a `sku_metadata`, sólo sobre hueco vacío**. Sin
  tabla intermedia — mismo pacto que `customers.phone` y el sellado de `as400_account`.
- **2 sep 2026** — Rafael: **órdenes y SKUs intercalados**, y `F6·F6·F7` **hasta 3 intentos** antes
  de rendirse ante un atascamiento. Lo segundo cambia `unstick_to_menu`, que hoy lo intenta una vez
  y es código compartido con la captura de órdenes.
- **2 sep 2026** — Rafael: **bicis primero hasta vaciar la cola, después los clientes**. La cola de
  bicis es finita; la de clientes son 628 filas que no se acaban.

---

## 15) Respuestas de Rafael (2 sep, tarde) — Q1 y Q2, y lo que mueven

**Q1: `Stock Inquiry` da el nombre completo, sin recortes.** Rafael lo confirma sobre el terminal.
La pantalla del AS400 **no comparte** el corte a 30 caracteres de la página de ítems (§3.3), así que
es la única fuente de un nombre entero para todo lo que llegó mutilado.

**Q2: si el SKU no tiene color, se pulsa TAB directo** sin llenar el campo. Los **126 SKUs de bici
sin sufijo** (`01-0169`) **entran a la cola**; el default que decía «si lo rechaza, quedan fuera»
queda anulado.

### 15.1 Lo que esto le cuesta a F1, y por qué es una buena noticia

Una descripción de **exactamente 30 caracteres es indistinguible de una completa que mida 30**.
`EXPLORER A2 19 2025 GLOSS BLAC` parte en `EXPLORER A2` / `19` / **`GLOSS BLAC`** — modelo y talla
salen bien (el corte es por el final, y la llave del export es `model` + `size`), pero el color
entra mutilado. **Un color truncado es peor que un color NULL**, así que:

> **R1 se refuerza:** el backfill **no escribe** cuando la descripción de origen mide exactamente
> 30 caracteres. Ese SKU no se descarta — **pasa a la cola de F2**, que ahora sí tiene con qué
> contestarlo.

### 15.2 Los números, rehechos

| | antes (2 sep, mañana) | ahora |
|---|---|---|
| **F1** — backfill sin AS400 | 12 + 1 | **9 con descripción completa + 1 sólo por `item_name` = 10** |
| **F2/F3** — el paso de AS400 | 7 | **7 sin fuente + 3 con el nombre cortado = 10** |
| …de esos, **en el piso** | 2 | **3** |

Y el trabajo que Q1 destapa, que no estaba dimensionado en el cuerpo del PRD:

| | |
|---|---|
| SKUs de bici cuyo nombre en PickD viene de una descripción **cortada a 30** | **182** (de 566 con descripción) |
| …con stock | **103** |
| …ya medidos, o sea **ya dentro del export de FedEx** | **73** |
| …de los 182, con `size` en NULL (la forma legacy sin partir) | 78 |

**Y aquí la parte tranquilizadora: esos 73 ya están limpios.** La muestra dice `03-3731GY` →
`DURANGO A2` / `17` / **`Grey`**, no `THUNDER GRE`; `03-3606BL` → `HUDSON E2 S/T` / `27.5X14` /
**`DEEP BLUE`**, no `DEEP BLU`. Alguien los arregló a mano o entraron por el alta estructurada.
**El corte nunca llegó al export**, y la llave `model` + `size` está intacta en las filas que hoy
viajan a FedEx.

O sea: los 182 **no son un incendio**, son una oportunidad. Re-leerlos por Stock Inquiry es una
tanda posterior (su propio PRD), no una urgencia — y ya no es «un tercio del catálogo a ciegas»
sino 182 filas con nombre y apellido, de las que 109 ni siquiera están medidas todavía.

### 15.3 Una segunda forma de la pantalla

Rafael pega, además, un encabezado distinto del mismo Stock Inquiry:

```
                            S T O C K   I N Q U I R Y
                                                                   (Cmd7-Exit)
  Stock Number: 03 3933 BK      CODA S2 L16 2026 GLOSS BLACK
```

La descripción va **en la línea del `Stock Number`, sin la etiqueta `Description:`**, y la única
tecla es `(Cmd7-Exit)` arriba a la derecha en vez de la legenda de dos teclas al pie.

~~❓ Q9 — ¿qué pantalla es?~~ **CONTESTADA el 2 sep: es `Cmd10 NOTES`.** Rafael la pulsó y llegó
ahí. Entra al mapa como §2.12b, y con ella queda mapeada la única tecla que le faltaba a §2.12.

> ⚠️ **Y trae un requisito que no estaba.** Las dos pantallas llevan **el mismo título**, así que
> un marcador `STOCKINQUIRY` las clasificaría igual — pero §2.12 tiene todos los campos por los que
> entramos y **NOTES no tiene ninguno**: ni `Weight`, ni `B-Bike/P-Part`, ni `On Hand`. Un paso que
> aterrice en NOTES creyéndose en el detalle leería `Weight` como *ausente* en vez de como *no estoy
> donde creo*, y escribiría el peso de una bici desde una pantalla que nunca lo tuvo.
>
> **R10 (nuevo) — el discriminador es un campo, no el título.** Antes de leer nada se exige la
> etiqueta `Description:` en su propia línea **y** `Weight:`. Sin las dos, el paso aborta sin
> escribir y sale por `Cmd7`. Ya está fijado en `tests/test_as400_capture.py`
> (`test_the_notes_screen_cannot_be_told_apart_by_the_title`) con las dos capturas reales.

Lo bueno: NOTES repite **la descripción completa**, así que es una segunda lectura del nombre si
alguna vez hiciera falta confirmarlo. Lo abierto: ❓ **Q10** — qué muestra el cuerpo cuando el SKU
sí tiene notas. *Default:* no se mira; no se pulsa `Cmd10` en el flujo automático, sólo se sabe
reconocer la pantalla para salir de ella.

### 15.4 Qué queda reemplazado

- **R1** gana la regla de los 30 caracteres (§15.1).
- **R6** ya no excluye a los SKUs sin sufijo de color: **entran**, con TAB en blanco.
- **R10 nuevo** (§15.3): el discriminador de pantalla es un campo, no el título.
- Los conteos de **§3.2, §8 (F1/F3) y §13-1** pasan a los de §15.2.
- El default de **❓Q2** queda anulado; **❓Q1** queda cerrada.
- **Criterio de aceptación nuevo (12):** el backfill F1 no escribe nada sobre un SKU cuya
  descripción de origen mida exactamente 30 caracteres — `03-3803BL`
  (`VENTURA A2 L48 2026 BLUE VAPOR`) se queda para F2 en vez de entrar con el color a medias.
- **Criterio de aceptación nuevo (13):** `01-0169`, sin sufijo de color, se teclea `010169` + TAB +
  TAB + `X` y la cola lo acepta.
- **Criterio de aceptación nuevo (14):** con la pantalla de `Cmd10 NOTES` delante, el paso **no
  escribe nada** —aunque el `Stock Number` sea el que pidió— y sale por `Cmd7`.

---

## 16) Decisiones de Rafael (2 sep, noche) — dónde vive cada cosa

Cuatro respuestas, y **una corrección medida que cambia el plan de fases**.

### 16.1 La corrección: F1 son 5 filas, no 10, y el ensanche no le sirve

Bajo la regla «sólo si `model` está NULL» (§6), sobre los 54 sin modelo:

| | |
|---|---|
| Sin **ninguna** fuente de nombre | **41** |
| Con la descripción cortada a 30 | 3 |
| **Parten hoy → F1** | **5** |
| No parten (compuestas, sin año, cm) | 5 |

**44 de 54 sólo los puede contestar el AS400.** Y el ensanche de talla `L` **no añade una sola
fila a F1**: los 25 SKUs con `L14`/`L16` **ya tienen `model`** — son la forma legacy con la talla
pegada dentro (`CODA S2 L16`, `size` NULL), y tocarlos es ❓Q4, cerrada en «no».

> **F1 deja de ser una fase.** Cinco filas no justifican una acción de mantenimiento con
> Preview/Apply. Se resuelven en la sesión de patrones (§16.2), a mano o con un script de veinte
> líneas, y **el esfuerzo se va entero a F3**. Esto reemplaza **F1 en §8** y los conteos de §15.2.

### 16.2 La sesión de patrones: sirve para F3, no para F1

Se ensancha `parseBikeName` **sólo para la talla con prefijo `L`** (`^L?\d{1,2}$`), con sus casos
en `parseBikeName.test.ts`. No es inventar regla: `renderSize` ya formatea `L16`, así que hoy dos
funciones del mismo repo se contradicen. **Su beneficiario es F3**, que va a recibir nombres como
`CODA S2 L16 2026 GLOSS BLACK` del Stock Inquiry y sin esto no los parte.

**Las compuestas NO se parten** (decisión de Rafael): `DIVIDE 13X27` se lee cuadro × rueda mientras
las 6 filas guardadas hoy son rueda × cuadro (`27.5X19`, `700CX16`) —y `TAXI 10X20` ya está al
revés—, así que una talla invertida entraría en la llave de agrupación del export. **R1/R3 se
niegan a partir un nombre con talla compuesta** y lo dejan para la mano. Las 13 de criterio (9
compuestas + 3 sin año + 1 en cm) se teclean en la misma sesión.

### 16.3 Dónde corre cada cosa

**El backfill vive en pickd, en TypeScript**, donde ya están `parseBikeName` y `renderSize` — un
hecho, una fuente. No se porta la regla a Python: sería un segundo espejo con su tabla de casos
duplicada, el problema que `canonical_sku` ya tiene en tres archivos.

**El watchdog se queda con F2, F3 y F4** — lo que de verdad necesita el terminal. Esto reemplaza
**R1** (que lo ponía en `maintenance.ACTIONS`) y saca F1 del alcance de este PRD.

### 16.4 La pasada nocturna se decide con un número, no ahora

Los **195** SKUs que sólo contesta el AS400 (78 cortados + 117 sin fuente) tardarían semanas a un
SKU por hueco. La ventana nocturna **no se decide todavía**: primero F3 con la cadencia normal.

> **R11 (nuevo) — F3 mide su propio drenaje.** Una línea de log al día con cuántos SKUs se
> resolvieron, cuántos huecos hubo y cuántos se perdieron por actividad del operador. Sin ese
> número la ventana nocturna se decidiría a ojo, que es justo lo que este PRD viene evitando.

❓ **Q11 — ¿ventana nocturna para los 195?** *Default:* se responde cuando F3 lleve una semana y
R11 haya dado la cifra. Si drena a menos de 20 SKUs/día, se abre la discusión; si no, no hace falta.

### 16.5 Qué queda reemplazado

- **F1 deja de existir como fase** del PRD del watchdog (§16.1); pasa a pickd y a la sesión.
- **R1** queda anulado aquí y renace en pickd.
- **R3** y el futuro parser: no parten tallas compuestas (§16.2).
- **R11 nuevo**: F3 mide su drenaje.
- Los conteos de **§15.2** quedan reemplazados por los de §16.1.
- **Criterio de aceptación nuevo (15):** un nombre con talla compuesta (`DIVIDE 13X27`) no se parte
  nunca automáticamente — ni en el backfill ni en el paso del AS400.

---

## 17) La sesión de patrones, hecha (8 sep 2026)

### 17.1 El ensanche

`parseBikeName` acepta la talla con prefijo `L` (`/^L?\d{1,2}$/i`, pickd `5da5b0d`). Sobre prod
mueve **8 nombres** de «no parte» a «parte». Quedan como *fallback* deliberado, cada uno con su
test: la compuesta `13X27`, la letra suelta (`KROMO S`), los centímetros (`54CM`) y la bici **sin
talla de cuadro** (`TAXI TRIKE`) — esta última no es un fallo, es la respuesta correcta.

### 17.2 Las 16 filas a mano, y las 2 que no se tocaron

Escritas en prod el 8 sep: 14 cambiaron, 2 ya estaban bien. Tres eran huecos (`model` NULL) y once
eran Q4 — `model` sucio con el año y el color dentro de la llave de agrupación.

**Rafael, 8 sep: las tallas compuestas se guardan en el orden del AS400 (cuadro × rueda,
`19X29`).** Consecuencia cosmética: `renderSize` pone la marca de pulgadas en el número del cuadro
(`19''X29`). FedEx usa esa cadena como descripción y no la interpreta, pero el catálogo queda con
**dos convenciones conviviendo** (`HUDSON E2 S/T` sigue en `27.5X14`, rueda primero). ❓ **Q12** —
normalizarlas es una tanda aparte.

**Efecto medido en el export, simulado antes de escribir y verificado después:**

| | antes | después |
|---|---|---|
| Registros | 201 | **199** |
| Excepciones | 17 | **16** |

Once registros basura (`DIVIDE 19X29 2025 OXBLOOD`, `TAXI 16 BAMBOO BEACH TEAL '`, `JUV MISS DAISY
HOT P`) se volvieron nueve limpios, con **tres fusiones reales** — `DIVIDE 19''X29-21''X29`,
`JUV CAPRI 2.4`, `KROMO L/S` — que es exactamente para lo que existe el export. `01-0529` entró al
archivo. **Ningún SKU salió.**

> **Y esto es lo que hay que quedarse de la sesión:** la primera propuesta **sí** expulsaba dos
> filas sanas. Dar a `01-0539` el `RENEGADE A1 LTD` talla `54` que ya tenía `03-4270BK`, y a
> `07-3606GP` el `JUV MISS DAISY` de `07-3664PK`, metía cada par en un mismo bucket con cajas que
> difieren 2″ y 19″ → `dimension_conflict` echa del archivo a **los dos** miembros del bucket, no
> sólo al nuevo. Se detectó **simulando `buildFedexDimensions` sobre las 276 filas medidas antes de
> escribir nada**. Sin esa simulación, dos filas con stock habrían dejado de cotizar en silencio.
>
> **R12 (nuevo) — toda escritura sobre `model` o `size` se simula contra el export antes de
> aplicarse**, y no se aplica si algún SKU pasa a excepción. Vale para F3 y para cualquier tanda
> futura de limpieza.

### 17.3 Dos medidas sospechosas, para la cinta métrica

Las dos filas que se dejaron sin `model` a propósito (ambas con stock 0) no son un problema de
nombres sino de **medición**, y alguien debería verlas en el piso:

| Nombre | Dos SKUs, dos cajas |
|---|---|
| `JUV MISS DAISY` 2025 | `07-3606GP` **56 × 9.75 × 37.5** vs `07-3664PK` **37 × 8 × 18** |
| `RENEGADE A1 LTD` talla 54 | `01-0539` **54 × 8 × 30** vs `03-4270BK` **55.75 × 8 × 30.4** |

El primero son 19 pulgadas de diferencia entre dos bicis del mismo nombre y año: una de las dos
medidas está mal. Mientras no se aclare, ponerles el mismo `model` es lo que las echaba del archivo.

### 17.4 Lo que queda de la misma forma: 47 filas, 1.884 unidades

La limpieza tocó las 16 que salían de la cola de nombres sin partir. Pero el mismo defecto vive en
**47 filas medidas más**, con **1.884 unidades** detrás — `model` de cuatro palabras o más con la
talla y el color dentro:

```
CITIZEN 2 17 MONTEREY      250 u      EXPLORER A2 17 GLOSS BLACK   166 u
CITIZEN 2 21 STORM         227 u      Divide 13 x 27.5 Smokey Green 77 u
CITIZEN 2 21 MONTEREY      209 u      HUDSON 19 GLOSS BLACK         45 u
CITIZEN 2 17 STORM         182 u      CODA S2 L18 VANILLA           40 u
```

Sólo la familia CITIZEN son nueve SKUs y más de 1.100 unidades, cada color como registro propio en
el archivo de FedEx en vez de fusionarse por `model` + `size`.

❓ **Q13 — ¿se limpian las 47?** *Default:* sí, pero **no a mano**: son demasiadas para teclearlas y
demasiado valiosas para adivinarlas. La forma es una acción con Preview/Apply que proponga el split,
**simule el export (R12)** y muestre qué se fusiona y qué se caería, para aprobarla en bloque. Es su
propio PRD, en pickd, y no bloquea nada de F2/F3.

---

## 18) F2 construido (8 sep 2026) — lee, no escribe

Lo que hay en el repo, sin desplegar todavía.

| | |
|---|---|
| `as400_capture.py` | `STATE_STOCK_INQUIRY`, `is_stock_detail_screen` (**R10**), `sku_screen_fields`, `capture_stock_inquiry`, `return_to_order_search` (**R3**), y los 3 intentos de `F6·F6·F7` |
| `parser.py` | `parse_stock_number`, `parse_stock_inquiry` |
| `sku_enrichment.py` (nuevo) | la cola (**R5/R6**), `plan_write` (**§6/R4**), `run_sku_step`, la lista de no-conocidos (**R7**) |
| `auto_scanner.py` | `_run_sku_gap()` dentro de la rama `not_found` |
| `tests/test_sku_enrichment.py` | 31 tests; la suite entera queda en **443** |

**Nace apagado.** `SKU_ENRICH=0`. Es una desviación deliberada de la tabla de §10, que lo daba en
`1`: el mismo deploy lleva el cambio de `unstick_to_menu` de **un** intento a **tres**, y lo primero
que hace un deploy no debe ser ponerse a manejar el terminal solo. Se enciende editando `.env` y
reiniciando el LaunchAgent — que es exactamente la propiedad que §10 buscaba.

**Lo que el paso hace hoy:** navega, lee, parsea, **comprueba que el `Stock Number` en pantalla es
el que pidió**, calcula el plan de escritura con `plan_write` y lo **loguea**. Cero escrituras.
Vuelve a la búsqueda de órdenes en un `finally`, así que la vuelta ocurre tanto si el lookup salió
bien como si reventó, y si no llega la cola se pausa sola.

### 18.1 Lo que los tests fijan antes de que pueda hacer daño

`plan_write` es la función a la que F3 le va a dar una conexión `service_role` sin RLS debajo, así
que sus reglas están pinchadas ahora: el hueco del peso es `weight_verified = false` y no un NULL;
una lectura de báscula **nunca** se toca; el plan **jamás** pone `weight_verified` en true; el
nombre sólo se planea si `model` está vacío. Y aparte: una guía de FedEx no gasta ni una tecla
(**R6** por forma), la pantalla de NOTES se **rechaza** en vez de leerse (**R10**), un `Stock
Number` que no es el nuestro corta el paso, `F6·F6·F7` se rinde a los 3, y una excepción del paso
de SKU **nunca** se lleva por delante a las órdenes.

### 18.2 La contradicción que F2 destapa, y que F3 no puede esquivar

> ~~❓ Q14 — ¿dónde parte F3 el nombre?~~ **CONTESTADA el 8 sep — §19.** `plan_write` sabe planear el **peso** —ese no necesita
> partir nada— pero para `model`/`size`/`color` hace falta la regla de `parseBikeName`, que vive en
> **TypeScript** y que el 2 sep se decidió **no portar a Python** (§16.3, «sería un segundo espejo»).
> F3 escribe desde el watchdog, en Python. Las dos decisiones no caben juntas.
>
> Hoy `plan_write` devuelve la descripción cruda bajo la llave `_description` y F2 la loguea, que es
> honesto para una fase que no escribe. *Default propuesto:* el watchdog escribe la descripción
> cruda del AS400 en una columna nueva (`sku_metadata.as400_description`) y **pickd la parte**, donde
> la regla ya vive — no es una tabla intermedia de aprobación, es poner cada mitad donde su regla
> está. **Esto se decide antes de F3, no durante.**

### 18.3 Lo que hay que ver en Bay 2 antes de encender F3

Con `SKU_ENRICH=1` y un día de log, F2 contesta lo que sólo el terminal sabe:

1. **¿Hace falta el ENTER final?** El recorrido de Rafael acaba en la `X`. El código pulsa ENTER
   después; si la `X` ya disparaba, ese ENTER cae sobre la pantalla de resultado, donde sólo
   redibuja (§2.9). Inofensivo en los dos casos, pero conviene saberlo.
2. **¿Qué pantalla sale cuando el SKU no existe?** Nadie la ha visto. F2 la trata como
   `StockSkuNotFound` y la loguea; con el texto delante entra al mapa.
3. **¿Cuánto cuesta un paso?** La línea de log lleva los segundos. Es el número que decide si «un
   SKU por hueco» se queda o si `SKU_ENRICH_MAX_PER_GAP` sube.
4. **¿`B-Bike/P-Part` discrepa con `is_bike`?** Se loguea cada desacuerdo y no se cambia nada (Q6).

---

## 19) Q14 contestada: **pickd la parte** (8 sep 2026)

Rafael, 8 sep: *«pickd la parte»*. Las dos mitades se cortan por la misma línea que la regla — el
watchdog escribe **lo que leyó**, y pickd lo parte donde `parseBikeName` ya vive. `parseBikeName` no
se porta a Python; §16.3 se mantiene entero.

### 19.1 La columna

`sku_metadata.as400_description` (texto crudo, sin partir, **sin el corte a 30** de la página de
ítems) y `sku_metadata.as400_read_at`. Migración `20260908120000_as400_description.sql` en pickd,
que es la fuente de verdad; `migrations.py` del watchdog repite los `ADD COLUMN IF NOT EXISTS` por
si el watcher se actualiza antes — **PostgREST descarta en silencio una columna que no existe**, y
aquí eso no sería sólo perder el dato: el SKU volvería a la cola en cada hueco, para siempre.

**Esto no es una tabla intermedia de aprobación** — Rafael cerró eso el 2 sep. Es que
`as400_description` **es un hueco vacío** y `model` no lo es: `model` es la llave de agrupación del
export. Y encaja con **R12**: una descripción cruda no necesita simulación del export, `model` sí —
y la simulación vive del lado que tiene `buildFedexDimensions` para correrla.

### 19.2 `as400_read_at` cierra el bucle de la cola

Lo destapó un test. Como `model` **sigue vacío** hasta que pickd parta, una cola que filtre sólo por
«sin modelo» pediría el mismo SKU en cada hueco eternamente. Ahora una fila ya leída **sale entera**
de la cola —también de la rama del peso, porque el peso vino en la misma pantalla— que es el ❓Q7 de
siempre («una vez leído, no se vuelve») convertido en código.

### 19.3 Lo que queda construido

| Mitad | Dónde | Estado |
|---|---|---|
| Leer AS400 y escribir `as400_description` + `weight_lbs` | watchdog, `sku_enrichment.apply_write` | **hecho**, tras `SKU_ENRICH_WRITE=1` |
| Partir en `model`/`size`/`color` | **pickd**, con `parseBikeName` | **falta** — ❓Q15 |

`apply_write` es un `update` por SKU, **nunca un upsert**: un upsert sobre un SKU que no estuviera
en el catálogo crearía una fila de metadata sin inventario detrás, que es la forma huérfana que
pickd limpió con una migración entera.

### 19.4 ~~❓Q15~~ — ¿dónde vive el partidor en pickd? **CONTESTADA — §20**

*Default propuesto:* **la misma acción Preview/Apply que pide ❓Q13** para las 47 filas sucias. Son
el mismo trabajo — proponer un split, **simular el export (R12)**, enseñar qué se fusiona y qué se
caería, aplicar en bloque — sobre dos orígenes: la descripción que trajo el AS400 y el `model` sucio
que ya estaba. Dos superficies para una misma decisión serían dos sitios donde equivocarse con la
llave del export.

**No automático, y esa es la razón:** R12 nació porque una propuesta mía echaba dos filas sanas del
archivo de FedEx sin avisar. Un partidor que escriba `model` solo, de noche, no puede enseñar eso a
nadie.

---

## 20) Exploración de largo plazo, y el plan que sale de ella (8 sep 2026)

Rafael pidió explorar alternativas antes de definir la superficie. La exploración movió el diseño,
así que va escrita: la propuesta de §19.4 —«la misma acción Preview/Apply que pide Q13»— era
**mi error**, y aquí está por qué.

### 20.1 Lo que la exploración encontró

**El backlog es cuatro veces lo que dijimos.** Las 47 de §17.4 eran sólo las medidas. Sobre las 836
bicis del catálogo:

| | |
|---|---|
| `model` de **4 palabras o más** | **227** |
| `model` **sin `size`** | **263** |
| con el **color** dentro del `model` | 52 |
| con el **año** dentro | 34 |
| con una **comilla doble literal** | **16** ← y el formato del export la prohíbe |

**`model` no es sólo la llave del export.** Lo consumen ~20 sitios (labels, `ShipScreen`,
`DoubleCheckView`, `warehouse-map`, Scratch & Dent) y **`ItemDetailView` lo edita a mano**. Eso
descarta derivarlo al vuelo desde `as400_description`: quedarían dos nombres para una misma cosa y
el piso vería el sucio.

**La casa ya tiene el patrón, y no es un diálogo por lotes.** Es `MeasureCartonsScreen`: «la misma
lista, ordenada por demanda, con el formulario para arreglarlo en la tarjeta; la misma forma que
Double Check, porque es el mismo trabajo».

**Y Q13 y Q15 NO eran el mismo trabajo.** Partir `CODA S2 L16 2026 GLOSS BLACK` es mecánico.
Decidir si `JUV CAPRI 2.4` lleva talla, si la `L` de `KROMO L` es *large* o parte del nombre, o si
un `TAXI TRIKE` tiene talla de cuadro, es criterio de negocio. Juntarlos en una superficie era
confundir dos cosas.

### 20.2 La decisión: que el AS400 quite el criterio de en medio

**El paso deja de filtrar por «`model` vacío» y lee TODAS las bicis consultables.** Son **745** de
las 836 (las otras 91 son guías y seriales que R6 excluye por forma), de las cuales **465 con
stock**. A un SKU por hueco son ~30 días hábiles; con la ventana nocturna de ❓Q11, días.

**No pisa nada.** `as400_description` está vacía en las 836, así que llenarla es exactamente el
pacto de siempre —sólo sobre hueco vacío— y `model` sigue sin tocarse desde el watchdog.

**Lo que compra:** las 227 sucias dejan de necesitar criterio. Con el nombre del fabricante delante,
`parseBikeName` decide; sobre los nombres no truncados que ya tenemos parte el **77%**, así que la
gente vería ~190 tarjetas en vez de 745.

**Consecuencia que obligó a un cambio:** ahora cada hueco aterriza en una bici cualquiera, y su peso
casi siempre es el 45 del trigger. Sin separarlo, la fase del nombre y la del peso se desplegarían
como una sola. `plan_write` recibe `with_weight` y el paso se lo pasa desde `SKU_ENRICH_WEIGHT`,
que es lo que §10 prometía.

### 20.3 La superficie: **una pantalla, dos carriles, UNA simulación**

Y esto ya no es cuestión de gusto: **los dos carriles escriben en la misma llave y se rompen entre
sí.** Si el carril mecánico funde `03-3777RD` y `03-3778BK` en `model = DIVIDE` y el carril humano,
en otra pantalla, le da `DIVIDE` a un tercero cuya caja mide 10″ menos, **los tres** se caen del
archivo por `dimension_conflict`. Es literalmente lo que casi pasó el 8 sep con `RENEGADE A1 LTD` y
`JUV MISS DAISY` (§17.2): la simulación no cazó una fila mala, cazó una **interacción**.

Dos superficies = dos simulaciones que no se ven entre sí. Así que:

- **Carril de arriba — «se parte solo»:** `parseBikeName` sobre `as400_description`, aplicable en
  bloque.
- **Carril de abajo — «necesita criterio»:** tarjeta a tarjeta, con la forma de `MeasureCartonsScreen`.
- **Una sola simulación de `buildFedexDimensions` sobre el resultado combinado** (**R12**), y no se
  escribe si algún SKU pasa a excepción.

### 20.4 Y una regla en la escritura, DESPUÉS de limpiar

Las 227 se regeneran solas: `model` lo escriben catorce caminos y una persona lo edita a mano. El
playbook de la casa ya lo resolvió una vez, en `canonical_sku` (`20260826220000`): *«cada intento
anterior añadió una lectura tolerante; esto añade una regla en la escritura»*.

Un CHECK que rechace un `model` con **año de 4 dígitos** o **comilla doble**. **Va después de la
limpieza**: hoy 34 y 16 filas lo violan y la migración fallaría al aplicarse. ❓ **Q16** — si además
debe rechazar el color dentro del modelo (52 filas) o eso es demasiado frágil para un CHECK.

### 20.5 El orden

1. **Ensanchar la cola** — hecho aquí. El watchdog lee las 745 hacia `as400_description`.
2. **La pantalla de pickd**, dos carriles y una simulación (❓Q15 contestada).
3. **El CHECK**, cuando 1 y 2 hayan dejado el catálogo limpio.

---

## 21) La ráfaga en el hueco (8 sep 2026) — el «5 minutos» del pedido original, de vuelta

Rafael: *«tenemos mucho tiempo durante el día cuando no hay órdenes, ¿hay posibilidad de aprovechar
un poco más el tiempo de día?»*

Sí, y el número es vergonzoso: **el hueco dura 20 minutos y usábamos ~6 segundos de él.** Con 745
bicis en cola, un SKU por hueco son **28 días hábiles**.

**Esto revierte una decisión mía, no una suya.** El 2 sep Rafael pidió «que dedique unos 5 minutos»
y yo lo llevé a un SKU por hueco (§9) — con razón entonces: la cola eran **7 SKUs** y un
presupuesto por minutos sólo habría añadido estado que persistir. La cola creció **cien veces**
(§20.2), y con ella el argumento se dio la vuelta. Su número original era el correcto.

### 21.1 Por qué subir el tope, tal cual estaba, era inseguro

La regla del 10 jun protege **el teclado del operador**, y el código sólo la cumplía por accidente:
**el gate de inactividad se comprobaba una vez, antes del hueco**. Diez consultas seguidas son ~60 s
agarrando el teclado, y una ráfaga podía haber pasado por encima de alguien sentándose.

Así que la ráfaga entra **con el gate movido a donde debía estar**:

- Se comprueba `system_idle_seconds()` **antes de cada consulta**, no una vez por hueco.
- Un *«get orders now»* manual (`_kick`) también corta: pidieron órdenes, no catálogo.
- La consulta que ya está corriendo **siempre devuelve el terminal a la búsqueda de órdenes** antes
  de parar — el `finally` de §18 no cambia.
- Dos límites, manda el primero: **presupuesto de reloj** (`SKU_ENRICH_GAP_BUDGET_SEC`, 300 s) y
  **tope de cuenta** (`SKU_ENRICH_MAX_PER_GAP`, 40). El reloj es lo que importa —una consulta lenta
  acorta la ráfaga, no la alarga— y la cuenta es el cinturón por si alguna volviera instantánea.

**El resultado neto es que el operador está mejor protegido que antes**, no peor: antes una ráfaga
de una consulta no podía interrumpirse; ahora una de cuarenta sí.

**Y las órdenes no pierden nada:** la búsqueda de la orden siguiente ya corrió; esto sólo llena el
sueño que venía después.

### 21.2 Lo que hace con los 28 días

Con 27 huecos al día y 300 s de presupuesto, la cola de 745 se vacía en **días, no en un mes** —
cuánto exactamente depende de lo que cueste una consulta, que es el número que F2 va a medir en Bay
2 y que nadie tiene todavía.

**Esto reordena ❓Q11 (la ventana nocturna).** Con el día bien aprovechado puede que sobre; se
decide con la cifra de F2, no antes. `SKU_ENRICH_MAX_PER_GAP=1` devuelve la cadencia vieja con una
línea de `.env`, sin deploy, si algo en el piso no cuadra.

**§9 queda reemplazado por esta sección** en lo que toca a la cadencia. Lo que NO cambia, y no se
negocia: el `capture_lock`, el gate de inactividad, y que la orden siempre va primero.

---

## 22) La pantalla de nombres: diseñada, y **en stand by** (8 sep 2026)

Rafael, 8 sep: *«dejemos esta pantalla en stand by, cuando recibamos los datos volvemos a
considerarla con la data en la mano»*. Correcto, y por una razón concreta: el reparto entre los dos
carriles —«se parte solo» contra «necesita criterio»— está estimado en **77 % / 23 %** a partir de
los nombres **no truncados que ya tenemos**, que son un proxy. Con las 745 descripciones reales del
AS400 puede salir muy distinto, y de ese número depende si la pantalla es un lote con una cola
pequeña al lado o al revés.

**Lo que NO está en stand by:** el watchdog sigue leyendo hacia `as400_description`. Los datos son
justamente lo que falta.

### 22.1 Lo decidido, para no volver a discutirlo

- **La entrada natural ya existe.** `/export` monta `FedexDimensionsExportCard`, que bajo el botón
  de exportar tiene un botón-fila con contador hacia `/export/measure`. Y `fedexCartonGap` ya
  etiqueta `no_model: 'No model on the record'`: la app **ya sabe** que una caja que FedEx no puede
  cotizar tiene dos causas —sin medir y sin nombre— y sólo construimos la mitad. La de nombres es la
  hermana exacta de la de medir.
- **El contador NO puede ser «excepciones `no_model`»** — hoy son 4. El daño real son las 227 con
  nombre sucio, que **no fallan**: producen registros con nombre basura, y por eso son invisibles.
  El contador es *bicis donde el nombre del AS400, partido, no coincide con lo guardado*.
- **Colisiones (Rafael):** la fila que tiraría a otra fuera del archivo **se desmarca sola**, se
  explica en ámbar con los números (`esto tiraría a 03-4270BK: cajas de 55.75″ y 54″`) y pasa al
  carril de criterio. El resto del lote se aplica. Avanza lo que se puede, no esconde nada.
- **Aplicar (Rafael):** el carril mecánico llega **todo marcado**; se revisa, se desmarca lo que
  no guste, se simula, se aplica en bloque. Con ~190 tarjetas es la única forma de que se acabe.
- **La simulación es la puerta:** sin correr `buildFedexDimensions` sobre el resultado **combinado**
  de los dos carriles, no hay botón de aplicar (**R12**, §20.3).

### 22.2 ❓Q17 — «también la estación» choca con dónde vive la pantalla

Rafael quiere que **la estación también la trabaje**, no sólo admin. Pero `/export` y
`/export/measure` son **admin-only** en `App.tsx` (hoy: 6 admins, 2 staff). Colgar de ahí una
pantalla que staff debe usar no funciona, y hay dos salidas y ninguna es gratis:

1. **Sacar `/export` de admin-only** — arrastra el CSV de FedEx, el resto del ExportScreen y
   cualquier cosa que viva ahí. Es la más simple y la que más reparte.
2. **Ruta propia fuera de `/export`** (p. ej. `/catalog/names`) con su propia entrada en el menú, y
   el botón-fila de `/export` como atajo para admin. Más código, permisos exactos.

*Default propuesto:* la **2**. La llave de agrupación del export merece una puerta propia, y abrir
`/export` entero para llegar a una pantalla es abrir de más. **Se decide con la data, junto con el
resto.**
