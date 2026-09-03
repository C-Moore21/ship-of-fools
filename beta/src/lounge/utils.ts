// Pure helpers for the Lounge: text formatting, mention rendering, reaction diff.
import type { LoungeMessage } from './api';

export const LOUNGE_MEMBERS = ['camden', 'jmac', 'caleb', 'zack'] as const;
export type LoungeMember = typeof LOUNGE_MEMBERS[number];

// 👍 ❤️ 👎 😭 — must match server _CHAT_REACTIONS ordering
export const LOUNGE_REACTIONS = ['\u{1F44D}', '❤️', '\u{1F44E}', '\u{1F62D}'] as const;
export type LoungeEmoji = typeof LOUNGE_REACTIONS[number];

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/** Day-aware timestamp: today shows time; yesterday prefixed; older shows month/day; other years include year. */
export function fmtLoungeTs(ts?: string | null): string {
  if (!ts) return '';
  const d = new Date(ts);
  if (isNaN(d.getTime())) return '';
  const now = new Date();
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate());
  const yesterday = new Date(today.getTime() - 86400000);
  const msgDay = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  const time = d.toLocaleTimeString('en-US', { hour: 'numeric', minute: '2-digit' });
  if (msgDay.getTime() === today.getTime()) return time;
  if (msgDay.getTime() === yesterday.getTime()) return `Yesterday ${time}`;
  if (d.getFullYear() === now.getFullYear()) {
    return `${MONTHS[d.getMonth()]} ${d.getDate()} · ${time}`;
  }
  return `${MONTHS[d.getMonth()]} ${d.getDate()}, ${d.getFullYear()} · ${time}`;
}

/** Stable signature of a message's reactions map — used to detect updates. */
export function reactionSig(m: Pick<LoungeMessage, 'reactions'>): string {
  const r = m.reactions || {};
  const keys = Object.keys(r).sort();
  return keys.map(k => `${k}:${(r[k] || []).slice().sort().join(',')}`).join('|');
}

/** Text token model for rendering. React consumers can safely map these
 *  into elements without any innerHTML — no dangerouslySetInnerHTML. */
export type LoungeTextToken =
  | { kind: 'text'; value: string }
  | { kind: 'mention'; name: string; mine: boolean }
  | { kind: 'url'; href: string; text: string }
  | { kind: 'show'; date: string; text: string };

// Trailing punctuation that shouldn't be swallowed into a URL match.
const URL_TRAIL = /[.,;:!?)\]}'"]+$/;

/** Normalize a two- or four-digit year from a `gdYY-MM-DD` / `gdYYYY-MM-DD`
 *  identifier. Two-digit years assume 19xx (the Dead toured 1965–1995). */
function normalizeGdDate(y: string, m: string, d: string): string {
  const year = y.length === 2 ? `19${y}` : y;
  return `${year}-${m}-${d}`;
}

/** Tokenize a message body into plain text + @mention chips + auto-detected
 *  URL and show-date tokens. Order of matches is preserved by scanning left
 *  to right and taking the earliest hit at each step. */
export function tokenizeText(text: string, currentUser: string | null | undefined): LoungeTextToken[] {
  const out: LoungeTextToken[] = [];
  if (!text) return out;

  // Single master regex with named-ish alternation groups.
  // Order in the pattern matters only inside the same position; we always
  // consume the earliest match, so URL vs. show-date can't collide at the
  // same index (URL requires scheme/www; a bare 1977-05-08 has neither).
  const mentionPart = '@(' + LOUNGE_MEMBERS.join('|') + ')\\b';
  const urlPart     = '(https?://[^\\s<>]+|www\\.[^\\s<>]+)';
  const gdPart      = '\\bgd(\\d{4}|\\d{2})-(\\d{2})-(\\d{2})\\b';
  const isoPart     = '\\b(\\d{4})-(\\d{2})-(\\d{2})\\b';
  const re = new RegExp(
    `${mentionPart}|${urlPart}|${gdPart}|${isoPart}`,
    'gi',
  );

  let last = 0;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) out.push({ kind: 'text', value: text.slice(last, m.index) });
    const whole = m[0];
    let consumed = whole.length;

    if (m[1]) {
      // mention: group 1 is the name (member list capture)
      const name = m[1];
      const mine = !!currentUser && currentUser.toLowerCase() === name.toLowerCase();
      out.push({ kind: 'mention', name, mine });
    } else if (m[2]) {
      // url: strip trailing sentence punctuation so "see https://x.com/y." works
      let raw = m[2];
      const trail = raw.match(URL_TRAIL);
      if (trail) {
        raw = raw.slice(0, raw.length - trail[0].length);
        consumed = whole.length - trail[0].length;
      }
      const href = /^https?:\/\//i.test(raw) ? raw : `https://${raw}`;
      out.push({ kind: 'url', href, text: raw });
    } else if (m[3]) {
      // gd date: groups 3=year, 4=month, 5=day
      const date = normalizeGdDate(m[3], m[4], m[5]);
      out.push({ kind: 'show', date, text: whole });
    } else if (m[6]) {
      // iso date: groups 6=year, 7=month, 8=day. Only 19xx/20xx look real.
      const y = parseInt(m[6], 10);
      if (y >= 1900 && y <= 2099) {
        out.push({ kind: 'show', date: `${m[6]}-${m[7]}-${m[8]}`, text: whole });
      } else {
        out.push({ kind: 'text', value: whole });
      }
    }
    last = m.index + consumed;
    // If we trimmed URL punctuation, re-anchor lastIndex so the trailing
    // char isn't skipped for subsequent matches.
    re.lastIndex = last;
  }
  if (last < text.length) out.push({ kind: 'text', value: text.slice(last) });
  return out;
}

/** Given the textarea value and caret position, return the active @mention
 *  fragment (or null). Frag "" means "@" was just typed. */
export function mentionQuery(value: string, caret: number): { start: number; frag: string } | null {
  const upto = value.slice(0, caret);
  const m = upto.match(/(?:^|\s)@([a-zA-Z]*)$/);
  if (!m) return null;
  return { start: caret - m[1].length - 1, frag: m[1].toLowerCase() };
}

/** Filter members that match the given fragment. Returns empty when there's an
 *  exact single match (so Enter sends instead of picking). */
export function mentionMatches(frag: string): string[] {
  const hits = LOUNGE_MEMBERS.filter(n => n.startsWith(frag));
  if (hits.length === 1 && hits[0] === frag) return [];
  return hits;
}

/** Splice an @name insertion into a textarea value, returns the new value
 *  and where the caret should land. */
export function applyMention(
  value: string,
  caret: number,
  name: string,
): { value: string; caret: number } {
  const q = mentionQuery(value, caret);
  const start = q ? q.start : caret;
  const before = value.slice(0, start);
  const after = value.slice(caret);
  const insertion = '@' + name + ' ';
  return { value: before + insertion + after, caret: (before + insertion).length };
}

/** LocalStorage key for the resize persistence. */
export const LOUNGE_SIZE_KEY = 'sof_lounge_size';

export interface LoungeSize { w: number; h: number }

export function loadLoungeSize(): LoungeSize | null {
  try {
    const raw = localStorage.getItem(LOUNGE_SIZE_KEY);
    if (!raw) return null;
    const s = JSON.parse(raw);
    if (!s || typeof s.w !== 'number' || typeof s.h !== 'number') return null;
    return s;
  } catch { return null; }
}

export function saveLoungeSize(s: LoungeSize): void {
  try { localStorage.setItem(LOUNGE_SIZE_KEY, JSON.stringify(s)); } catch {}
}
