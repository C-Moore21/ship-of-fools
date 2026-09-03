// The Lounge state hook. Owns polling, message merging, unread badge, and
// optimistic reactions. Panel-agnostic — call `toggle()` from wherever you
// wire the launcher button.
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { loungeApi, type LoungeMessage, type LoungeRef } from './api';
import { reactionSig } from './utils';

const POLL_OPEN_MS = 4000;   // fast poll while panel is open
const POLL_UNREAD_MS = 30000; // slow badge poll when closed

export interface PendingReply {
  id: string;
  user: string;
  text: string;
}

export interface UseLoungeOptions {
  /** Currently logged-in username, lowercased. Pass null when logged out. */
  currentUser: string | null;
  /** Optional: called after the panel opens (e.g. focus the textarea). */
  onOpen?: () => void;
}

export interface UseLoungeReturn {
  open: boolean;
  toggle: () => void;
  close: () => void;

  member: boolean;
  loading: boolean;
  error: string | null;

  messages: LoungeMessage[];
  unread: number;

  send: (text: string, opts?: { ref?: LoungeRef | null }) => Promise<void>;
  react: (messageId: string, emoji: string) => Promise<void>;

  pendingReply: PendingReply | null;
  startReply: (messageId: string) => void;
  cancelReply: () => void;

  /** Force a poll now (used e.g. after send). */
  pollNow: () => Promise<void>;
}

export function useLounge({ currentUser, onOpen }: UseLoungeOptions): UseLoungeReturn {
  const [open, setOpen] = useState(false);
  const [member, setMember] = useState(false);
  const [messages, setMessages] = useState<LoungeMessage[]>([]);
  const [unread, setUnread] = useState(0);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [pendingReply, setPendingReply] = useState<PendingReply | null>(null);

  const lastTsRef = useRef<string | null>(null);
  const msgsRef = useRef<Map<string, LoungeMessage>>(new Map());
  const pollTimerRef = useRef<number | null>(null);
  const unreadTimerRef = useRef<number | null>(null);

  // ── Access check + unread poll ─────────────────────────────────────────
  useEffect(() => {
    let cancelled = false;
    if (!currentUser) { setMember(false); return; }
    (async () => {
      try {
        const a = await loungeApi.access();
        if (cancelled) return;
        setMember(!!a.member);
      } catch { /* not logged in / not member */ }
    })();
    return () => { cancelled = true; };
  }, [currentUser]);

  const pollUnread = useCallback(async () => {
    if (!member) return;
    try {
      const u = await loungeApi.unread();
      setUnread(u.member ? u.unread : 0);
    } catch {}
  }, [member]);

  useEffect(() => {
    if (!member) return;
    pollUnread();
    unreadTimerRef.current = window.setInterval(pollUnread, POLL_UNREAD_MS);
    return () => {
      if (unreadTimerRef.current) window.clearInterval(unreadTimerRef.current);
      unreadTimerRef.current = null;
    };
  }, [member, pollUnread]);

  // ── Load initial + merge helpers ───────────────────────────────────────
  const commitMessages = useCallback((next: LoungeMessage[]) => {
    msgsRef.current = new Map(next.map(m => [m.id, m]));
    setMessages(next);
    let latest: string | null = null;
    for (const m of next) {
      const s = m.bumped_at || m.ts;
      if (s && (!latest || s > latest)) latest = s;
    }
    lastTsRef.current = latest;
  }, []);

  const loadInitial = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const msgs = await loungeApi.messages(null, 100);
      commitMessages(msgs);
      try { await loungeApi.markRead(); } catch {}
      setUnread(0);
    } catch (e: any) {
      setError(e?.message || 'Unable to load');
    } finally {
      setLoading(false);
    }
  }, [commitMessages]);

  const pollNow = useCallback(async () => {
    if (!member) return;
    if (!lastTsRef.current) { await loadInitial(); return; }
    try {
      const incoming = await loungeApi.messages(lastTsRef.current, 100);
      if (!incoming.length) return;
      const current = msgsRef.current;
      const appended: LoungeMessage[] = [];
      let latest = lastTsRef.current;
      let mutated = false;
      for (const m of incoming) {
        if (!m.id) continue;
        const prev = current.get(m.id);
        if (!prev) {
          appended.push(m);
          current.set(m.id, m);
          mutated = true;
        } else if (reactionSig(prev) !== reactionSig(m)) {
          current.set(m.id, m);
          mutated = true;
        }
        const stamp = m.bumped_at || m.ts;
        if (stamp && (!latest || stamp > latest)) latest = stamp;
      }
      lastTsRef.current = latest;
      if (mutated) {
        // Preserve insertion order: existing followed by newly-appended.
        const merged = Array.from(current.values()).sort((a, b) => {
          const ta = a.ts || '';
          const tb = b.ts || '';
          return ta < tb ? -1 : ta > tb ? 1 : 0;
        });
        setMessages(merged);
        if (appended.length) {
          try { await loungeApi.markRead(); } catch {}
          setUnread(0);
        }
      }
    } catch {}
  }, [member, loadInitial]);

  // ── Open/close orchestration ───────────────────────────────────────────
  useEffect(() => {
    if (!open || !member) {
      if (pollTimerRef.current) window.clearInterval(pollTimerRef.current);
      pollTimerRef.current = null;
      return;
    }
    loadInitial();
    pollTimerRef.current = window.setInterval(pollNow, POLL_OPEN_MS);
    onOpen?.();
    return () => {
      if (pollTimerRef.current) window.clearInterval(pollTimerRef.current);
      pollTimerRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open, member]);

  const toggle = useCallback(() => {
    setOpen(o => {
      const next = !o;
      if (!next) {
        // closing — clear pending reply, fire mark-read for good measure
        setPendingReply(null);
        loungeApi.markRead().catch(() => {});
      }
      return next;
    });
  }, []);

  const close = useCallback(() => {
    setOpen(false);
    setPendingReply(null);
    loungeApi.markRead().catch(() => {});
  }, []);

  // ── Send / react / reply ───────────────────────────────────────────────
  const send = useCallback(async (text: string, opts?: { ref?: LoungeRef | null }) => {
    const trimmed = text.trim();
    const ref = opts?.ref ?? null;
    if (!trimmed && !ref) return;
    const reply_to = pendingReply?.id ?? null;
    await loungeApi.send(trimmed, { ref, reply_to });
    setPendingReply(null);
    await pollNow();
  }, [pendingReply, pollNow]);

  const react = useCallback(async (messageId: string, emoji: string) => {
    if (!currentUser || !member) return;
    // Optimistic toggle
    const current = msgsRef.current;
    const m = current.get(messageId);
    if (m) {
      const reactions = { ...(m.reactions || {}) };
      const arr = [...(reactions[emoji] || [])];
      const i = arr.indexOf(currentUser);
      if (i >= 0) arr.splice(i, 1); else arr.push(currentUser);
      if (arr.length) reactions[emoji] = arr;
      else delete reactions[emoji];
      const next: LoungeMessage = { ...m, reactions };
      current.set(messageId, next);
      setMessages(Array.from(current.values()));
    }
    try {
      const resp = await loungeApi.react(messageId, emoji);
      const m2 = current.get(messageId);
      if (m2) {
        const next = { ...m2, reactions: resp.reactions || {} };
        current.set(messageId, next);
        setMessages(Array.from(current.values()));
      }
    } catch {}
  }, [currentUser, member]);

  const startReply = useCallback((messageId: string) => {
    const m = msgsRef.current.get(messageId);
    if (!m) return;
    let preview = (m.text || '').trim();
    if (!preview && m.ref) preview = '▶ ' + (m.ref.show_date || 'shared show');
    setPendingReply({ id: messageId, user: m.user || '', text: preview.slice(0, 140) });
  }, []);

  const cancelReply = useCallback(() => setPendingReply(null), []);

  return useMemo<UseLoungeReturn>(() => ({
    open, toggle, close,
    member, loading, error,
    messages, unread,
    send, react,
    pendingReply, startReply, cancelReply,
    pollNow,
  }), [open, toggle, close, member, loading, error, messages, unread,
      send, react, pendingReply, startReply, cancelReply, pollNow]);
}
