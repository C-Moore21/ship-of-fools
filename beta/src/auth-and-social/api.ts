// Typed fetchers for auth, per-show ratings, per-show notes.
// All calls send credentials so the Flask session cookie carries.
//
// Backend endpoints wired (see app.py):
//   GET  /api/auth/me                       -> { username: string | null }
//   POST /api/auth/login                    -> { ok, username } | { error } 401
//   POST /api/auth/register                 -> { ok, username } | { error } 4xx
//   POST /api/auth/logout                   -> { ok }
//   GET  /api/show-ratings/lookup?show_id=  -> { stars: number }  (0 = none)
//   POST /api/show-ratings {show_id,venue,stars}   stars in [0.5, 5] step 0.5
//   DELETE /api/show-ratings {show_id}
//   GET  /api/notes/<show_id>               -> { note: string }
//   POST /api/notes {show_id, note}         (empty note deletes)
//
// NOTE: There is no dedicated backend endpoint for a single show's community
// average rating (only per-user rows and full-aggregation leaderboards).
// getShowAvgRating() therefore attempts a not-yet-implemented endpoint and
// resolves to null on 404 so callers can hide the widget gracefully. Add
// /api/show-ratings/community?show_id=... server-side to light it up.

export interface Me {
  username: string | null;
}

export interface AuthResult {
  ok: boolean;
  username?: string;
  error?: string;
}

export interface ShowAvg {
  avg: number;
  count: number;
}

const JSON_HEADERS = { 'Content-Type': 'application/json' };

async function jsonOrThrow<T>(res: Response): Promise<T> {
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = (data && (data as any).error) || `HTTP ${res.status}`;
    throw new Error(msg);
  }
  return data as T;
}

// ── Auth ────────────────────────────────────────────────────────────────
export async function getMe(): Promise<Me> {
  const res = await fetch('/api/auth/me', { credentials: 'include' });
  return jsonOrThrow<Me>(res);
}

export async function login(username: string, password: string): Promise<AuthResult> {
  const res = await fetch('/api/auth/login', {
    method: 'POST',
    credentials: 'include',
    headers: JSON_HEADERS,
    body: JSON.stringify({ username, password }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) return { ok: false, error: (data as any).error || 'Login failed' };
  return { ok: true, username: (data as any).username };
}

export async function register(username: string, password: string): Promise<AuthResult> {
  const res = await fetch('/api/auth/register', {
    method: 'POST',
    credentials: 'include',
    headers: JSON_HEADERS,
    body: JSON.stringify({ username, password }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) return { ok: false, error: (data as any).error || 'Register failed' };
  return { ok: true, username: (data as any).username };
}

export async function logout(): Promise<void> {
  await fetch('/api/auth/logout', {
    method: 'POST',
    credentials: 'include',
  });
}

// ── Per-show ratings (half-star supported, 0.5–5.0 in 0.5 steps) ────────
export async function getMyRating(showDate: string): Promise<number> {
  const res = await fetch(
    `/api/show-ratings/lookup?show_id=${encodeURIComponent(showDate)}`,
    { credentials: 'include' },
  );
  if (res.status === 401) return 0;
  const data = await jsonOrThrow<{ stars: number }>(res);
  return typeof data.stars === 'number' ? data.stars : 0;
}

export async function saveRating(
  showDate: string,
  stars: number,
  venue = '',
): Promise<number> {
  if (stars <= 0) {
    await fetch('/api/show-ratings', {
      method: 'DELETE',
      credentials: 'include',
      headers: JSON_HEADERS,
      body: JSON.stringify({ show_id: showDate }),
    });
    return 0;
  }
  const res = await fetch('/api/show-ratings', {
    method: 'POST',
    credentials: 'include',
    headers: JSON_HEADERS,
    body: JSON.stringify({ show_id: showDate, venue, stars }),
  });
  const data = await jsonOrThrow<{ ok: boolean; stars: number }>(res);
  return data.stars;
}

export async function getShowAvgRating(showDate: string): Promise<ShowAvg | null> {
  // Optimistic call to a not-yet-existing endpoint; safe null on absence.
  try {
    const res = await fetch(
      `/api/show-ratings/community?show_id=${encodeURIComponent(showDate)}`,
      { credentials: 'include' },
    );
    if (!res.ok) return null;
    const data = await res.json();
    if (typeof data.avg !== 'number') return null;
    return { avg: data.avg, count: data.count ?? 0 };
  } catch {
    return null;
  }
}

// ── Per-show notes ──────────────────────────────────────────────────────
export async function getMyNote(showDate: string): Promise<string> {
  const res = await fetch(`/api/notes/${encodeURIComponent(showDate)}`, {
    credentials: 'include',
  });
  if (res.status === 401) return '';
  const data = await jsonOrThrow<{ note: string }>(res);
  return data.note || '';
}

export async function saveNote(showDate: string, text: string): Promise<void> {
  const res = await fetch('/api/notes', {
    method: 'POST',
    credentials: 'include',
    headers: JSON_HEADERS,
    body: JSON.stringify({ show_id: showDate, note: text }),
  });
  await jsonOrThrow<{ ok: boolean }>(res);
}
