// operator_sentinel.js — the operator's signals, watched from outside the
// watcher's own keystrokes (operator_sentinel.py runs it; Rafael, 29 sep 2026:
// «nadie puede parar al watcher… usar la tecla delete dos veces»).
//
// The watcher never moves the mouse and never presses Delete, so either one is
// a person, whatever the idle clock says. Prints MOUSE on any mouse move, click
// or scroll, and DELETE on each Delete / Forward-Delete press. `argv[0]` = run
// for that many seconds (tests); none = for ever.
ObjC.import('AppKit');
ObjC.import('CoreGraphics');
ObjC.import('Foundation');
function say(s) {
  $.NSFileHandle.fileHandleWithStandardOutput.writeData($(s + "\n").dataUsingEncoding($.NSUTF8StringEncoding));
}
function run(argv) {
  var pollMs = 30, seconds = argv.length ? parseFloat(argv[0]) : 0;
  var HID = 1, DELETE = 51, FWD_DELETE = 117;
  var moves = $.CGEventSourceCounterForEventType(HID, 5);
  var clicks = $.CGEventSourceCounterForEventType(HID, 1) + $.CGEventSourceCounterForEventType(HID, 3);
  var scrolls = $.CGEventSourceCounterForEventType(HID, 22);
  var delDown = false;
  say("READY");
  var start = Date.now();
  while (!seconds || Date.now() - start < seconds * 1000) {
    var m = $.CGEventSourceCounterForEventType(HID, 5);
    var c = $.CGEventSourceCounterForEventType(HID, 1) + $.CGEventSourceCounterForEventType(HID, 3);
    var sc = $.CGEventSourceCounterForEventType(HID, 22);
    if (m !== moves || c !== clicks || sc !== scrolls) { say("MOUSE"); moves = m; clicks = c; scrolls = sc; }
    var d = $.CGEventSourceKeyState(HID, DELETE) || $.CGEventSourceKeyState(HID, FWD_DELETE);
    if (d && !delDown) say("DELETE");
    delDown = d;
    delay(pollMs / 1000);
  }
}
