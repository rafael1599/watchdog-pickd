"""
as400_explore.py — Una expedición corta por las pantallas de STOCK INQUIRY,
para que la navegación se diseñe sobre lo que hay y no sobre lo que supongo.

Rafael, 12 sep 2026: «si nos enfocamos en un análisis de las pantallas primero,
podemos definir el modo más rápido… una persona saca un SKU cada cinco o seis
segundos porque se mantiene en la misma página». Y después: «lo vas a tener que
automatizar y capturar tú mismo, no estaré ahí hasta el lunes».

Tres preguntas, y ninguna se puede contestar leyendo el código:

  1. La pantalla a la que vuelve el Cmd7 tiene un campo `Search:` y una lista de
     quince filas con Stock #, precio y disponible por almacén. ¿Cómo se llena?
     Si lista de verdad, son quince SKUs por lectura en vez de uno.
  2. ¿La pantalla de DETALLE deja teclear otro número encima y enviar? Si deja,
     el Cmd7 entre consulta y consulta sobra — y ese paso es casi toda la
     diferencia entre los 5 s de una persona y nuestros 13.
  3. ¿Qué hace `F11=Toggle Warehouses`?

**El truco para no adivinar dónde está el cursor**: teclear un MARCADOR y mirar
en qué columna aparece. El mapa de campos sale de la pantalla, no de mi cabeza.
Es exactamente lo que no hice en los tres intentos de acelerar que tuve que
revertir hoy.

SEGURIDAD, y es lo que decide qué entra aquí:
  - Sólo la opción **02**, que es de consulta. La 09 (Order Entry) escribe y la
    07 cambia el terminal: no se tocan ni por error.
  - Sólo teclas que la propia pantalla anuncia (`F11`, `Cmd7`) más TAB y texto.
    Ninguna tecla de función a ver qué pasa.
  - Cada experimento termina volviendo por el camino VERIFICADO, y si no puede
    volver, la expedición entera se aborta. El escáner recupera con su
    bootstrap: un experimento no puede costarle las órdenes del lunes.
  - Corre UNA vez por proceso y guarda un puñado de pantallas. No es un modo.
"""

from __future__ import annotations

import logging
import time

log = logging.getLogger("pickd-as400-explore")

MARKER = "ZZZZ"


def _cap(driver, keep, name, note):
    try:
        keep(f"explore:{name}", note, driver.copy_screen())
    except Exception as e:  # noqa: BLE001 — mirar no puede tumbar nada
        log.warning("explore: no se pudo guardar %s (%s)", name, e)


def run_expedition(driver, keep, enter_stock_fn, home_fn, page_wait=1.0, step_wait=0.6) -> int:
    """Corre los experimentos y devuelve cuántas pantallas guardó.

    `enter_stock_fn(driver)` deja el terminal dentro de la opción 02.
    `home_fn(driver)` es el camino verificado de vuelta a las órdenes.
    `keep(classified, after, raw)` guarda una pantalla.
    """
    saved = 0

    def go_home():
        try:
            home_fn(driver)
            return True
        except Exception as e:  # noqa: BLE001
            log.warning("explore: no se pudo volver a casa (%s) — se aborta", e)
            return False

    experiments = [
        # Dónde cae el cursor al entrar, y dónde caen los TABs sucesivos. El
        # marcador aparece en el campo que tenga el foco, así que estas cuatro
        # capturas SON el mapa de campos.
        ("cursor_0tab", 0),
        ("cursor_1tab", 1),
        ("cursor_2tab", 2),
        ("cursor_3tab", 3),
    ]

    for name, tabs in experiments:
        try:
            enter_stock_fn(driver)
            time.sleep(page_wait)
            if name == "cursor_0tab":
                _cap(driver, keep, "browse_fresh", "entrar a 02")
                saved += 1
            for _ in range(tabs):
                driver.key("tab")
                time.sleep(step_wait)
            driver.type_text(MARKER)
            time.sleep(step_wait)
            _cap(driver, keep, name, f"{tabs}×TAB + {MARKER}")
            saved += 1
        except Exception as e:  # noqa: BLE001
            log.warning("explore: %s falló (%s)", name, e)
        if not go_home():
            return saved

    # `F11=Toggle Warehouses`, que la propia pantalla anuncia.
    try:
        enter_stock_fn(driver)
        time.sleep(page_wait)
        driver.key("f11")
        time.sleep(page_wait)
        _cap(driver, keep, "f11_toggle", "F11 en la pantalla de lista")
        saved += 1
    except Exception as e:  # noqa: BLE001
        log.warning("explore: f11 falló (%s)", e)
    if not go_home():
        return saved

    log.info("explore: %d pantallas guardadas", saved)
    return saved


def probe_retype_on_detail(driver, keep, lookup_fn, home_fn, sku_a, sku_b, page_wait=1.0) -> int:
    """¿La pantalla de DETALLE deja teclear otro número encima y enviar?

    Es la pregunta cara: si deja, el Cmd7 entre consulta y consulta sobra, y con
    él casi toda la diferencia entre los 5 s de una persona y nuestros 13.

    Se hace con dos SKUs distintos a propósito. Si tras teclear el segundo la
    pantalla enseña el segundo, la respuesta es sí; si sigue enseñando el
    primero, o se va a otro sitio, es no — y la captura dice a dónde.
    """
    saved = 0
    try:
        lookup_fn(sku_a, driver)  # deja el terminal en el detalle de A
        _cap(driver, keep, "detail_before_retype", f"detalle de {sku_a}")
        saved += 1

        from as400_capture import sku_screen_fields

        fields = sku_screen_fields(sku_b)
        if fields:
            digits, colour = fields
            driver.type_text(digits)
            driver.key("tab")
            if colour:
                driver.type_text(colour)
            driver.key("tab")
            driver.type_text("X")
            time.sleep(page_wait)
            _cap(driver, keep, "detail_after_retype", f"{sku_b} tecleado sobre el detalle")
            saved += 1
    except Exception as e:  # noqa: BLE001
        log.warning("explore: retype falló (%s)", e)
    try:
        home_fn(driver)
    except Exception as e:  # noqa: BLE001
        log.warning("explore: no se pudo volver tras el retype (%s)", e)
    return saved
