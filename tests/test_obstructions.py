"""What covers Mocha is taken away — any window, not one kind (Rafael, 6 nov 2026)."""

import pytest

import obstructions
from as400_capture import NO_COPY_MESSAGE, CaptureError, MochaDriver

APPLE_ID = "512\tstoreuid\t\tEnter the Apple ID password for “someone”.\n"


def test_parse_reads_pid_process_title_and_text():
    out = APPLE_ID + "77\tSafari\tPickD\t\n" + "garbage line\n" + "x\ty\n"
    ws = obstructions.parse_scan(out)
    assert [(w.pid, w.process, w.title) for w in ws] == [
        (512, "storeuid", ""),
        (77, "Safari", "PickD"),
    ]
    assert ws[0].describe() == "storeuid «Enter the Apple ID password for “someone”.»"


def test_scan_skips_mocha_the_shell_and_the_watcher():
    script = obstructions.build_scan_script(
        obstructions.SHELL | obstructions.OURS | {"Mocha TN5250"}
    )
    for name in ("Mocha TN5250", "Dock", "Finder", "Python", "osascript"):
        assert f'"{name}"' in script


class Screen:
    """A fake macOS: windows by pid; pressing or killing makes them go."""

    def __init__(self, windows, sticky=(), deaf=()):
        self.windows = dict(windows)  # pid -> scan line
        self.sticky = set(sticky)  # ignore buttons and Escape
        self.deaf = set(deaf)  # pressing raises
        self.scripts = []
        self.killed = []

    def run(self, script):
        self.scripts.append(script)
        if "every process)" in script:
            return "".join(self.windows.values())
        pid = int(script.split("unix id is ")[1].split("\n")[0])
        if pid in self.deaf:
            raise RuntimeError("no such button")
        if pid not in self.sticky:
            self.windows.pop(pid, None)
            return "button:Cancel"
        return "escape"

    def kill(self, pid):
        self.killed.append(pid)
        self.windows.pop(pid, None)


def test_a_dialog_is_declined_with_its_own_button():
    screen = Screen({512: APPLE_ID})
    removed, left = obstructions.clear(screen.run, "Mocha TN5250", screen.kill)
    assert removed == ["storeuid «Enter the Apple ID password for “someone”.» (button:Cancel)"]
    assert left == []
    assert screen.killed == []


def test_a_window_that_stays_gets_its_process_quit():
    screen = Screen({77: "77\tSafari\tPickD\t\n"}, sticky={77})
    removed, left = obstructions.clear(screen.run, "Mocha TN5250", screen.kill)
    assert screen.killed == [77]
    assert removed == ["Safari «PickD» (quit)"]
    assert left == []


def test_a_window_that_refuses_the_keys_is_still_quit():
    screen = Screen({77: "77\tSafari\tPickD\t\n"}, deaf={77})
    removed, _ = obstructions.clear(screen.run, "Mocha TN5250", screen.kill)
    assert screen.killed == [77] and removed == ["Safari «PickD» (quit)"]


def test_protected_windows_are_reported_and_never_touched():
    line = "88\tSecurityAgent\t\tEnter an administrator name and password\n"
    screen = Screen({88: line})
    removed, left = obstructions.clear(screen.run, "Mocha TN5250", screen.kill)
    assert removed == []
    assert left == ["SecurityAgent «Enter an administrator name and password»"]
    assert screen.killed == []
    assert not any("unix id is 88" in s for s in screen.scripts)


def test_the_decline_script_never_presses_a_yes():
    script = obstructions.build_decline_script(512)
    for yes in ('"OK"', '"Allow"', '"Sign In"', '"Install"', '"Continue"'):
        assert yes not in script
    assert "key code 53" in script  # Escape is the last word


class _Driver(MochaDriver):
    def __init__(self, reads, screen):
        super().__init__(app_name="Mocha TN5250")
        self.reads = list(reads)
        self.screen = screen

    def _copy_screen_once(self, steps=()):
        r = self.reads.pop(0)
        if r is None:
            raise CaptureError(NO_COPY_MESSAGE)
        return r

    def _osascript(self, script, capture=False):
        return self.screen.run(script)


def test_a_covered_mocha_is_uncovered_and_read_again(monkeypatch):
    monkeypatch.setattr(obstructions.subprocess, "run", lambda *a, **k: None)
    screen = Screen({512: APPLE_ID})
    driver = _Driver([None, "SIGN ON"], screen)
    assert driver.copy_screen() == "SIGN ON"
    assert screen.windows == {}


def test_nothing_to_remove_says_what_is_on_screen(monkeypatch):
    screen = Screen({88: "88\tSecurityAgent\t\tadmin password\n"})
    driver = _Driver([None], screen)
    with pytest.raises(CaptureError) as e:
        driver.copy_screen()
    assert "SecurityAgent «admin password»" in str(e.value)


def test_a_clean_screen_keeps_the_old_message():
    driver = _Driver([None], Screen({}))
    with pytest.raises(CaptureError) as e:
        driver.copy_screen()
    assert str(e.value) == NO_COPY_MESSAGE


def test_retry_happens_only_once(monkeypatch):
    monkeypatch.setattr(obstructions.subprocess, "run", lambda *a, **k: None)
    driver = _Driver([None, None], Screen({512: APPLE_ID}))
    with pytest.raises(CaptureError) as e:
        driver.copy_screen()
    assert "Removed first: storeuid" in str(e.value)
    assert driver.reads == []


def test_the_guard_can_be_switched_off(monkeypatch):
    monkeypatch.setenv("AS400_OBSTRUCTION_GUARD", "0")
    screen = Screen({512: APPLE_ID})
    driver = _Driver([None], screen)
    with pytest.raises(CaptureError):
        driver.copy_screen()
    assert screen.scripts == []


def test_a_good_read_costs_no_scan():
    screen = Screen({512: APPLE_ID})
    assert _Driver(["SCREEN"], screen).copy_screen() == "SCREEN"
    assert screen.scripts == []


def test_a_person_at_the_mac_stops_the_guard():
    class OperatorTookOver(Exception):
        pass

    screen = Screen({512: APPLE_ID})

    def run(script):
        if "unix id is" in script:
            raise OperatorTookOver()
        return screen.run(script)

    with pytest.raises(OperatorTookOver):
        obstructions.clear(run, "Mocha TN5250", screen.kill)
    assert screen.killed == []
