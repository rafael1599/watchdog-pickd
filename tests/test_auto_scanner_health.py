"""El faro de salud del AS400, ahora visible desde fuera del Mac."""

import auto_scanner


def _reset():
    auto_scanner._as400_health.update({"at": 0.0, "ok": None, "parked": None})


def test_a_quiet_weekend_and_a_jammed_terminal_no_longer_look_the_same():
    # 12 sep 2026: Bay 2 perdio el terminal tras un mismatch y desde fuera se
    # veia igual que un sabado sin ordenes — el heartbeat late, `last_gap_at`
    # solo se mueve cuando corre un hueco, y un escaner reintentando el
    # bootstrap no corre ninguno.
    _reset()
    assert auto_scanner.as400_health()["state"] == "unknown"

    auto_scanner.note_as400(False, "stock_inquiry")
    h = auto_scanner.as400_health()
    assert h["state"] == "err" and h["parked"] == "stock_inquiry"

    # Y en cuanto vuelve a contestar, el sitio donde quedo aparcado deja de
    # ser noticia: describia un atasco que ya no existe.
    auto_scanner.note_as400(True)
    h = auto_scanner.as400_health()
    assert h["state"] == "ok" and h["parked"] is None
    _reset()


def test_a_signal_nobody_refreshed_degrades_to_unknown(monkeypatch):
    _reset()
    auto_scanner.note_as400(False, "stock_inquiry")
    monkeypatch.setattr(auto_scanner, "AS400_HEALTH_MAX_AGE_SEC", -1.0)
    assert auto_scanner.as400_health()["state"] == "unknown"
    _reset()
