# Los huecos del escáner se usan para las fichas de cliente

> Estado: **E1 + E2 escritos y apagados, 29 sep 2026** (`customer_enrichment.py`). Se encienden desde
> PickD con la fila `app_flags.as400_customer_enrich`, no desde el `.env` de Bay 2 — ver §9.
> Propuesta original: 1 sep 2026.
> Pedido de Rafael: *«cuando no hay órdenes para tomar, se comienza a analizar los detalles de los
> clientes de las órdenes que se fueron a PickD ese día y se envían, y luego de terminar se deja en
> la pantalla de búsqueda de órdenes»*.

## 1. Por qué encaja

El escáner **está parado casi todo el día**. Su ritmo real, del log de Bay 2:

```
11:26:59  auto-scan: not_found on #881337 (waiting 1200s)
11:29:47  auto-scan: not_found on #881337 (waiting 1200s)
...
12:52:48  auto-scan: not_found on #881337 (waiting 1200s)
```

Hora y media en el mismo número. Con ~10 órdenes al día y capturas de 6 s, el trabajo real del
terminal es **un minuto de cada jornada**. Todo lo demás es esperar a que exista la orden siguiente.

Y hay un dato que falta en PickD y que está a cuatro teclas de distancia: **`customers.phone` y
`customers.email` están vacías en las 628 filas**, y viven en `CUSTOMER DISPLAY`
(`docs/as400-screen-map.md` §2.11).

## 2. La forma: un cliente por hueco, no una ráfaga

Misma cadencia que las órdenes, por la misma razón (decisión del 10 jun 2026): **un paso, un
cliente**. Cuando `run_scan_step` devuelve `not_found` —no hay orden nueva que capturar— en vez de
dormir 20 minutos enteros, se hace **una** ficha y se vuelve a dormir.

Nunca compite con el operador: entra por el mismo `capture_lock` y respeta el mismo gate de
inactividad de 60 s. Si el operador toca el teclado a mitad, se abandona **después de devolver el
terminal a la búsqueda de órdenes**, no en medio.

## 3. El recorrido, tecla a tecla

Todo confirmado por Rafael el 1 sep 2026 (`docs/as400-screen-map.md` §2.4 y §2.11):

```
búsqueda de orden ──Cmd7 EXIT──▶ menú SALESN ──1 + ENTER──▶ Customer Inquiry
   ──cuenta + TAB + 00 + ENTER──▶ CUSTOMER DISPLAY   (leer aquí)
   ──Cmd7 EXIT──▶ menú ──3 + ENTER──▶ búsqueda de orden
```

**La vuelta es parte de la operación, no una limpieza opcional.** El paso no se da por terminado
hasta que una lectura confirma que la pantalla es la de búsqueda de órdenes; si no lo es, se aplica
la salida del operador (`F6·F6·F7` → menú → `3`) y, si aun así no vuelve, el escáner se declara
`unavailable` y deja de tocar el terminal hasta el próximo `bootstrap_session`.

Con la fusión de teclas (F8) el recorrido entero son **tres** llamadas a System Events, no diez.

## 4. Qué se lee y qué se escribe

| En pantalla | A dónde va | Regla |
|---|---|---|
| `Phone No` | `customers.phone` | **sólo si está NULL** |
| `EMAIL Address` | `customers.email` | **sólo si está NULL** |
| `Account Number` | — | se usa para **verificar** que la ficha es la que pedimos |

**Nunca se pisa un valor que ya esté.** Misma regla que el sellado de `as400_account`: rellenar un
hueco es seguro, sobrescribir lo que escribió una persona no. Y **si la cuenta de la pantalla no es
la que pedimos, no se escribe nada**: es la única defensa contra haber aterrizado en otra ficha.

`Salesman ID`, `Terms Code` y los `Cr Limit` se leen y se descartan por ahora (❓1).

## 5. A quién, y en qué orden

1. Los clientes de las órdenes **enviadas a PickD hoy** que no tengan teléfono.
2. Cuando no queden, los que más órdenes acumulan en los últimos 90 días.

Hace falta la cuenta AS400 para navegar, así que sólo entran clientes con `customers.as400_account`
sellada. Los `ship_to_varies` (consumidor final, eBay, garantía) **se excluyen**: su destinatario
cambia en cada orden y su ficha no significa lo mismo.

## 6. Lo que puede salir mal, y qué lo frena

| Riesgo | Freno |
|---|---|
| Aterrizar en la ficha equivocada | Se compara `Account Number` con la pedida antes de escribir |
| Quedarse fuera de la búsqueda de órdenes | El paso no termina hasta comprobarlo por lectura; luego `F6·F6·F7` |
| Robarle el teclado al operador | Mismo `capture_lock` y mismo gate de 60 s que la captura |
| Entrar en una pantalla nueva sin querer | `classify_screen` antes de teclear, como en toda captura |
| Perder capturas de órdenes por estar en el maestro | Sólo se entra tras un `not_found`, y **una ficha por hueco** |

## 7. Fases

- **E1 — Leer y no escribir.** Navegar, capturar `CUSTOMER DISPLAY`, parsearlo, **loguear** lo que
  habría escrito, y volver a la búsqueda. Cero escrituras. Un día así dice si el recorrido es fiable
  antes de que toque la base de datos.
- **E2 — Escribir.** `phone` y `email` sólo sobre NULL, con la comprobación de cuenta.
- **E3 — La cola.** De los clientes del día a los 628, en los huecos.

## 8. Preguntas abiertas (❓ con su default)

1. ❓ **¿Sólo teléfono y email?** *Default:* sí. `Terms Code` y `Cr Limit` son información de crédito
   —no es asunto del almacén— y `Salesman ID` es nuestro vendedor, no el contacto del dealer.
2. ❓ **¿Se refresca un cliente ya leído?** *Default:* no. Una vez leído, no se vuelve; si un teléfono
   cambia, se corrige a mano. Volver a leer 628 fichas cada mes es tráfico sin motivo.
3. ❓ **¿`Bike Buyer` como contacto?** *Default:* no se escribe. En el único cliente que hemos visto
   contenía códigos (`ACT# 2385 ROUT# 0353`), no una persona. E1 lo loguea; si en veinte fichas
   aparecen nombres, entonces sí.
4. ❓ **¿Cuántas fichas por hueco?** *Default:* **una**, y volver a dormir lo que tocaba. El escáner
   existe para las órdenes; esto es lo que hace mientras no hay ninguna.

## 9. 29 sep 2026 — escrito, y el CONTACT del papel

Rafael, con la foto del pack slip de **881753**: *«empieza con la fase de los datos alcanzables,
luego tienes que mandar al watcher a explorar cada opción para descubrir el mapa completo. Lo que
queremos es lo que dice contact en esta orden»*. El papel impreso trae
`TELEPHONE (201) 891-5500` y `CONTACT MICHAEL PORRARO-OWNER`; la captura de ORDER INQUIRY de esa
misma orden (`as400_captures.raw_text`) no trae ninguno de los dos — ni ninguna de las 328 que hay.

**Lo escrito:**

- `capture_customer_display` (`as400_capture.py`): menú verificado → `1` + ENTER → comprueba que la
  pantalla diga CUSTOMER → cuenta, TAB, `00`, ENTER → exige `CUSTOMER DISPLAY`. `enter_menu_option`
  **se niega** a abrir 07 y 09.
- `parse_customer_display` (`parser.py`): teléfono con la grafía del papel (`(732) 741-2799`), email,
  vendedor y los tres `Buyer`.
- `customer_enrichment.run_customer_step`: una cuenta, guarda **cada** pantalla en `as400_screens`
  (`sku = acct:<cuenta>-<sufijo>`, `classified = customer_entry | customer_display`), compara la cuenta
  de la pantalla con la pedida y aplica `plan_write` — sólo `phone` y `email`, sólo sobre NULL (la
  regla se repite en el `UPDATE … is null`). Lo leído se anota en `.customer_seen.json` para no
  preguntar dos veces en la fase de sólo lectura.
- En el escáner, `_run_customer_gap` corre en el hueco `not_found` **sólo si el de SKU no tuvo nada
  que hacer**, con las mismas guardas antes de cada cuenta (operario, «get orders now», deploy
  pendiente) y **una** vuelta verificada a la búsqueda de órdenes al terminar.

**El interruptor vive en PickD** (`app_flags`, clave `as400_customer_enrich`), porque Bay 2 está
detrás del NAT y editar su `.env` exige ir al Mac. `enabled` enciende el paso; `config.write` es E2;
`config.explore` + `config.explore_rev` mandan la expedición (una vez por rev: subir el número la
repite sin desplegar); `config.explore_account`, `explore_keys`, `explore_menu`, `gap_budget_sec` y
`max_per_gap` la ajustan. Un env var puesto gana siempre — `CUSTOMER_ENRICH=0` es el freno en el Mac.

**La expedición** (`run_expedition`), para encontrar el CONTACT: sobre la cuenta **6034** (WYCKOFF
CYCLE, la de 881753 — así una pantalla que diga PORRARO se reconoce), abre cada tecla que CUSTOMER
DISPLAY anuncia — `Cmd1 Product`, `Cmd2 Comp`, `Cmd3 Closest Dlr`, `Cmd4 POP Info`, `Cmd5 CallBack`,
`Cmd6 Prior`, `Cmd10 Top10`, `Cmd12 PreSeas` — y las opciones del menú nunca abiertas `04`, `06` y
`10`. **Una tecla, una lectura, y a casa** por el camino verificado; si no puede volver, aborta.
Todo queda en `as400_screens` con `classified = explore:customer:f<n>` / `explore:menu:<nn>`.

- ❓ **`Cmd11 Commit` no se pulsa.** *Default:* fuera de la lista, porque es la única leyenda que se
  lee como una acción. Se añade con `config.explore_keys` si Rafael confirma que sólo consulta.
- ❓ **La cuenta, ¿se teclea `6034` o `0006034`?** *Default:* como la guarda PickD (`6034`); la
  pantalla se compara con la pedida, así que una grafía equivocada es un `mismatch`, nunca otra ficha.
  `CUSTOMER_ACCOUNT_PAD=1` prueba la otra.
- `contact_name` **no se escribe todavía**: primero hay que ver en qué pantalla está.


### Lo que dijo Bay 2 el mismo día (`as400_screens` 298–313)

- **La opción 01 abre primero `CUSTOMER DETAIL DISPLAY`**: `Customer: _______ __`, `Phone:`,
  `Search:` y una lista. Estado nuevo `STATE_CUSTOMER_INQUIRY` (sale con `Cmd7`).
- **La cuenta se teclea con ceros a la izquierda, siete dígitos** (`0006034`): el campo se llena
  desde la izquierda y `6034` quedó `6034000` → «Customer ID NOT on File». Ahora es el default
  (`CUSTOMER_ACCOUNT_PAD=0` teclea la cuenta tal cual).
- **`04`** = `ACCOUNTS RECEIVABLE INQUIRY`: `Customer:`, `Phone:`, `Search:`, `Roll Keys SCROLL`,
  `Cmd7 EXIT`. Sin teclear nada dentro todavía.
- **`06`** = `SPOOL FILE STATUS` de IBM (`SPOOLJOB — Control printing`), con opciones que **cancelan,
  retienen y cambian** entradas de impresión. **Fuera de `READ_ONLY_MENU_OPTIONS`.** Ojo: es donde
  viven las impresiones — el pack slip con el CONTACT sale de ahí (`6. Copy or display entries`) —,
  así que es la pista más fuerte, y **no se automatiza sin que Rafael lo decida**.
- **`10`** = `PICK SLIP UPDATE` (`Order:`). Un «update»: **fuera de la lista**.
