// Injected into the page before any app code. Measures, per interaction:
//
//   firstPaintMs  click -> first DOM mutation reaches a paint  (perceived response)
//   settleMs      click -> last DOM mutation settles + paints  (content latency)
//   blockingMs    total long-task time in the window           (jank / main-thread cost)
//   eventMs       the click's own handler duration (Event Timing API)
//   mutations     mutation records emitted                     (React commit churn proxy)
//   batches       distinct MutationObserver callbacks           (commit count proxy)
//
// settleMs is the headline metric. firstPaintMs is what the user *feels*.
export const PROBE_SOURCE = `(() => {
  const B = {
    t0: 0, running: false,
    firstMut: 0, lastMut: 0, mutations: 0, batches: 0,
    lastPaint: 0, paintPending: false,
    longTasks: [], events: [],
    marks: [],
  };

  const now = () => performance.now();

  const mo = new MutationObserver((records) => {
    if (!B.running) return;
    const t = now();
    B.batches += 1;
    B.mutations += records.length;
    if (!B.firstMut) B.firstMut = t;
    B.lastMut = t;
    if (!B.paintPending) {
      B.paintPending = true;
      requestAnimationFrame(() => {
        // rAF fires pre-paint; a queued task after it lands post-paint.
        setTimeout(() => { B.lastPaint = now(); B.paintPending = false; }, 0);
      });
    }
  });

  const startObserving = () => {
    mo.observe(document.documentElement, {
      subtree: true, childList: true, attributes: true, characterData: true,
    });
  };
  if (document.documentElement) startObserving();
  else document.addEventListener('DOMContentLoaded', startObserving, { once: true });

  try {
    new PerformanceObserver((list) => {
      for (const e of list.getEntries()) {
        if (!B.running) continue;
        B.longTasks.push({ start: e.startTime, dur: e.duration });
      }
    }).observe({ type: 'longtask', buffered: false });
  } catch (_) {}

  try {
    new PerformanceObserver((list) => {
      for (const e of list.getEntries()) {
        if (!B.running || e.startTime < B.t0 - 50) continue;
        B.events.push({
          name: e.name,
          dur: e.duration,
          processing: e.processingEnd - e.processingStart,
          start: e.startTime,
        });
      }
    }).observe({ type: 'event', durationThreshold: 0, buffered: false });
  } catch (_) {}

  B.begin = () => {
    B.t0 = now();
    B.firstMut = 0; B.lastMut = 0; B.mutations = 0; B.batches = 0;
    B.lastPaint = 0; B.paintPending = false;
    B.longTasks = []; B.events = [];
    B.running = true;
  };

  B.quietFor = () => (B.lastMut ? now() - B.lastMut : now() - B.t0);

  B.finish = () => {
    B.running = false;
    // Rebase on the browser's own click timestamp when Event Timing gives us
    // one, so Playwright's driver overhead never lands in the measurement.
    const clicks = B.events.filter((e) => e.name === 'click');
    const origin = clicks.length ? Math.min(...clicks.map((e) => e.start)) : B.t0;
    const base = origin > B.t0 - 5 && origin < B.t0 + 300 ? origin : B.t0;
    const rel = (t) => (t ? Math.max(0, t - base) : null);
    const blocking = B.longTasks
      .filter((l) => l.start + l.dur > base)
      .reduce((s, l) => s + Math.min(l.dur, l.start + l.dur - base), 0);
    const clickEvents = B.events.filter((e) =>
      e.name === 'click' || e.name === 'pointerdown' || e.name === 'pointerup' || e.name === 'keydown');
    return {
      firstPaintMs: rel(B.firstMut),
      settleMs: rel(B.lastPaint || B.lastMut),
      domSettleMs: rel(B.lastMut),
      blockingMs: Math.round(blocking * 10) / 10,
      eventMs: clickEvents.length ? Math.max(...clickEvents.map((e) => e.dur)) : null,
      handlerMs: clickEvents.length ? Math.max(...clickEvents.map((e) => e.processing)) : null,
      mutations: B.mutations,
      batches: B.batches,
      longTasks: B.longTasks.length,
    };
  };

  window.__bench = B;
})();`
