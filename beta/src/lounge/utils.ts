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
  | { kind: 'mention'; name: string; mine: boolean };

/** Tokenize a message body into plain text + @mention chips. Case-insensitive
 *  match against LOUNGE_MEMBERS, bounded on the right so "@camdens" won't match
 *  but "@camden!" will. */
export function tokenizeText(text: string, currentUser: string | null | undefined): LoungeTextToken[] {
  const out: LoungeTextToken[] = [];
  if (!text) return out;
  const re = new RegExp('@(' + LOUNGE_MEMBERS.join('|') + ')\\b', 'gi');
  let last = 0;
  let match: RegExpExecArray | null;
  while ((match = re.exec(text)) !== null) {
    if (match.index > last) out.push({ kind: 'text', value: text.slice(last, match.index) });
    const name = match[1];
    const mine = !!currentUser && currentUser.toLowerCase() === name.toLowerCase();
    out.push({ kind: 'mention', name, mine });
    last = match.index + match[0].length;
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
