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
