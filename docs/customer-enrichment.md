# Los huecos del escáner se usan para las fichas de cliente

> Estado: **PROPUESTA**, 2 sep 2026. Nada escrito todavía.
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
