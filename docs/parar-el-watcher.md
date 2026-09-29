# Cómo parar al watcher desde el Mac de Bay 2

Rafael, 29 sep 2026: *«nadie puede parar al watcher, no escucha cuando presionamos una tecla o
movemos el mouse»*. Desde `operator_sentinel.py`:

| Qué haces | Qué hace el watcher |
|---|---|
| **Mover el ratón**, hacer clic o scroll | Suelta el teclado en la siguiente tecla que iba a mandar y no vuelve hasta que el ratón lleve **60 s** quieto (`SCAN_IDLE_THRESHOLD_SEC`). |
| **Delete dos veces** (en menos de 2 s) | **Parada de emergencia: 30 minutos** sin tocar el AS400 (`OPERATOR_EMERGENCY_STOP_SEC`). El latido dice `stopped by the operator (Delete ×2)`. |
| El botón «get orders now» de la UI | Siempre funciona: es una persona pidiéndolo. |

**Por qué antes no escuchaba.** El watcher reconocía a una persona comparando el reloj de inactividad
de macOS con el de su propia última tecla. Mientras trabaja teclea y copia la pantalla varias veces
por segundo, así que su reloj nunca salía de cero y los eventos del operario se perdían entre los
suyos. Y el paso de clientes sólo miraba entre una ficha y otra.

**Por qué el ratón y el Delete.** Son las dos cosas que el watcher **nunca** produce: no mueve el
ratón y no pulsa Delete. Los lee un proceso aparte (`scripts/operator_sentinel.js`, JavaScript for
Automation, ~3 % de CPU) con los contadores de eventos de macOS, sin depender de ningún reloj.

El freno está en la única puerta por la que el watcher teclea (`MochaDriver._osascript`) y sólo
aplica al hilo automático (`mark_automated_thread`): una captura que pides desde la UI no se frena.
`OPERATOR_SENTINEL=0` en el `.env` apaga la vigilancia.
