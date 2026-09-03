// The floating Lounge panel — self-contained. Drop it anywhere in the beta
// tree. It renders nothing when closed (aside from an optional launcher via
// props). Integration wires `open`/`toggle` from useLounge to whatever
// launcher (AppHeader button, etc.) the host chooses.
//
// Design carries over the classic Lounge look: royal-accent top border,
// surface2 pills, red-for-mine mention highlighting, drag-to-resize corner.
import React, {
  useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState,
} from 'react';
import type { LoungeMessage } from './api';
import { useLounge, type UseLoungeReturn } from './useLounge';
import {
  LOUNGE_REACTIONS,
  applyMention,
  fmtLoungeTs,
  loadLoungeSize,
  mentionMatches,
  mentionQuery,
  saveLoungeSize,
  tokenizeText,
} from './utils';

export interface LoungePanelProps {
  /** Logged-in username (lowercased). Panel hides itself if member=false. */
  currentUser: string | null;
  /** If provided, we use this controller instead of creating one. Useful
   *  when the host also wants to read `unread` for its own launcher badge. */
  controller?: UseLoungeReturn;
  /** Called when the user clicks a shared-show pill. Host wires navigation. */
  onOpenShow?: (showDate: string) => void;
}

export function LoungePanel({ currentUser, controller, onOpenShow }: LoungePanelProps) {
  const local = useLounge({ currentUser });
  const c = controller ?? local;

  const {
    open, member, messages, loading, error,
    send, react, pendingReply, startReply, cancelReply,
  } = c;

  const bodyRef = useRef<HTMLDivElement | null>(null);
  const inputRef = useRef<HTMLTextAreaElement | null>(null);
  const panelRef = useRef<HTMLDivElement | null>(null);

  // ── Draft + mention autocomplete ───────────────────────────────────────
  const [draft, setDraft] = useState('');
  const [mention, setMention] = useState<{ matches: string[]; active: number } | null>(null);
  const [flashId, setFlashId] = useState<string | null>(null);
  const [pickerFor, setPickerFor] = useState<string | null>(null);

  const updateMention = useCallback((el: HTMLTextAreaElement) => {
    const q = mentionQuery(el.value, el.selectionStart || 0);
    if (!q) { setMention(null); return; }
    const matches = mentionMatches(q.frag);
    if (!matches.length) { setMention(null); return; }
    setMention({ matches, active: 0 });
  }, []);

  const pickMention = useCallback((name: string) => {
    const el = inputRef.current;
    if (!el) return;
    const { value, caret } = applyMention(el.value, el.selectionStart || 0, name);
    setDraft(value);
    setMention(null);
    // restore caret after React re-render
    requestAnimationFrame(() => {
      if (!inputRef.current) return;
      inputRef.current.focus();
      inputRef.current.setSelectionRange(caret, caret);
    });
  }, []);

  // ── Focus + auto-scroll on open / new messages ─────────────────────────
  useEffect(() => {
    if (open) inputRef.current?.focus();
  }, [open]);

  const prevCountRef = useRef(0);
  useLayoutEffect(() => {
    const body = bodyRef.current;
    if (!body) return;
    const wasAtBottom = (body.scrollHeight - body.scrollTop - body.clientHeight) < 60;
    const grew = messages.length > prevCountRef.current;
    prevCountRef.current = messages.length;
    if (grew && wasAtBottom) body.scrollTop = body.scrollHeight;
    if (grew && messages.length && prevCountRef.current === messages.length && body.scrollTop === 0) {
      // initial load — pin to bottom
      body.scrollTop = body.scrollHeight;
    }
  }, [messages]);

  // On first-open, snap to bottom
  useLayoutEffect(() => {
    if (open && bodyRef.current) bodyRef.current.scrollTop = bodyRef.current.scrollHeight;
  }, [open, loading]);

  // ── Textarea autoresize ────────────────────────────────────────────────
  useLayoutEffect(() => {
    const el = inputRef.current;
    if (!el) return;
    el.style.height = 'auto';
    el.style.height = Math.min(el.scrollHeight, 120) + 'px';
  }, [draft]);

  // ── Send handler ───────────────────────────────────────────────────────
  const [sending, setSending] = useState(false);
  const onSubmit = useCallback(async () => {
    if (sending) return;
    const text = draft.trim();
    if (!text) return;
    setSending(true);
    try {
      await send(text);
      setDraft('');
    } catch (e: any) {
      alert('Send failed: ' + (e?.message || e));
    } finally {
      setSending(false);
    }
  }, [draft, send, sending]);

  const onKeyDown = useCallback((e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (mention && mention.matches.length) {
      if (e.key === 'ArrowDown') { e.preventDefault(); setMention(m => m && ({ ...m, active: (m.active + 1) % m.matches.length })); return; }
      if (e.key === 'ArrowUp')   { e.preventDefault(); setMention(m => m && ({ ...m, active: (m.active - 1 + m.matches.length) % m.matches.length })); return; }
      if (e.key === 'Escape')    { e.preventDefault(); setMention(null); return; }
      if (e.key === 'Enter' || e.key === 'Tab') {
        e.preventDefault();
        pickMention(mention.matches[mention.active]);
        return;
      }
    }
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      onSubmit();
    }
  }, [mention, pickMention, onSubmit]);

  // ── Scroll-to-parent (reply flash) ─────────────────────────────────────
  const scrollToMsg = useCallback((id: string) => {
    const el = bodyRef.current?.querySelector<HTMLElement>(`[data-msg-id="${cssEscape(id)}"]`);
    if (!el) return;
    el.scrollIntoView({ behavior: 'smooth', block: 'center' });
    setFlashId(null);
    // reflow so animation can restart
    requestAnimationFrame(() => {
      setFlashId(id);
      setTimeout(() => setFlashId(f => (f === id ? null : f)), 1500);
    });
  }, []);

  // ── Resize (desktop) ───────────────────────────────────────────────────
  const [size, setSize] = useState<{ w: number; h: number } | null>(null);
  useEffect(() => {
    if (!open) return;
    if (typeof window !== 'undefined' && window.innerWidth <= 700) return;
    const s = loadLoungeSize();
    if (s) setSize(s);
  }, [open]);

  const resizeStart = useCallback((e: React.PointerEvent) => {
    if (window.innerWidth <= 700) return;
    e.preventDefault();
    const panel = panelRef.current;
    if (!panel) return;
    const rect = panel.getBoundingClientRect();
    const start = { x: e.clientX, y: e.clientY, w: rect.width, h: rect.height };
    const onMove = (ev: PointerEvent) => {
      const w = Math.max(280, Math.min(window.innerWidth - 28, start.w + (start.x - ev.clientX)));
      const h = Math.max(320, Math.min(window.innerHeight - 120, start.h + (start.y - ev.clientY)));
      setSize({ w, h });
    };
    const onUp = () => {
      document.removeEventListener('pointermove', onMove);
      document.body.style.userSelect = '';
      const p = panelRef.current;
      if (p) {
        const r = p.getBoundingClientRect();
        saveLoungeSize({ w: Math.round(r.width), h: Math.round(r.height) });
      }
    };
    document.addEventListener('pointermove', onMove);
    document.addEventListener('pointerup', onUp, { once: true });
    document.body.style.userSelect = 'none';
  }, []);

  // ── Dismiss reaction picker on outside click ───────────────────────────
  useEffect(() => {
    if (!pickerFor) return;
    const off = (ev: MouseEvent) => {
      const t = ev.target as HTMLElement | null;
      if (t && t.closest('[data-react-picker]')) return;
      setPickerFor(null);
    };
    setTimeout(() => document.addEventListener('click', off), 0);
    return () => document.removeEventListener('click', off);
  }, [pickerFor]);

  const style = useMemo<React.CSSProperties>(() => (
    size ? { width: size.w, height: size.h } : {}
  ), [size]);

  if (!open || !member) return null;

  return (
    <>
      <FlashKeyframe />
    <div
      ref={panelRef}
      className={[
        // Desktop: floating bottom-right, royal top-border. Mobile: fullscreen.
        'fixed z-[500] flex flex-col font-mono',
        'bottom-[90px] right-[14px] w-[380px] h-[520px]',
        'max-w-[calc(100vw-28px)] max-h-[calc(100vh-120px)]',
        'bg-surface border border-line rounded-[4px]',
        'shadow-[0_8px_30px_rgba(0,0,0,0.8)]',
        'border-t-[3px] border-t-royal',
        // Mobile fullscreen override
        'max-[700px]:inset-0 max-[700px]:w-full max-[700px]:h-[100dvh]',
        'max-[700px]:max-w-none max-[700px]:max-h-none max-[700px]:rounded-none',
        'max-[700px]:border-0 max-[700px]:border-t-[3px] max-[700px]:z-[9999]',
      ].join(' ')}
      style={style}
    >
      {/* Resize handle */}
      <div
        onPointerDown={resizeStart}
        title="Drag to resize"
        className="absolute top-0 left-0 w-4 h-4 z-[2] cursor-nwse-resize opacity-60 hover:opacity-100 transition-opacity max-[700px]:hidden"
        style={{
          background: 'linear-gradient(135deg, var(--accent2) 0%, var(--accent2) 40%, transparent 42%)',
          borderTopLeftRadius: 3,
        }}
      />

      {/* Header */}
      <div className="flex items-center justify-between px-3 py-2 border-b border-line">
        <div className="text-ink text-xs uppercase tracking-[2px]">The Lounge</div>
        <button
          onClick={c.close}
          className="text-muted hover:text-ink px-2 py-1 text-sm"
          title="Close"
        >×</button>
      </div>

      {/* Reply banner */}
      {pendingReply && (
        <div className="flex items-center justify-between gap-2 px-3 py-1.5 bg-surface2 border-b border-line border-l-[3px] border-l-royal text-[11px]">
          <span className="text-muted overflow-hidden text-ellipsis whitespace-nowrap">
            ↪ Replying to <span className="text-royal-bright font-semibold mr-1">{pendingReply.user}</span>
            {pendingReply.text}
          </span>
          <button
            onClick={cancelReply}
            className="text-muted hover:text-ink text-sm px-1"
            title="Cancel reply"
          >×</button>
        </div>
      )}

      {/* Body */}
      <div
        ref={bodyRef}
        className="flex-1 overflow-y-auto px-3 py-2 flex flex-col gap-[3px] max-[700px]:px-3.5 max-[700px]:py-3.5 max-[700px]:gap-3"
      >
        {loading && <div className="text-muted text-[11px] text-center py-8">Loading…</div>}
        {error && <div className="text-muted text-[11px] text-center py-8">{error}</div>}
        {!loading && !error && messages.length === 0 && (
          <div className="text-muted text-[11px] text-center py-8">The lounge is empty. Say hi.</div>
        )}
        {messages.map(m => (
          <MessageRow
            key={m.id}
            m={m}
            currentUser={currentUser}
            flashing={flashId === m.id}
            onReplyStart={() => startReply(m.id)}
            onReact={emoji => react(m.id, emoji)}
            onOpenPicker={() => setPickerFor(pickerFor === m.id ? null : m.id)}
            pickerOpen={pickerFor === m.id}
            onClosePicker={() => setPickerFor(null)}
            onReplyClick={() => m.reply_to && scrollToMsg(m.reply_to)}
            onOpenShow={onOpenShow}
          />
        ))}
      </div>

      {/* Input */}
      <div className="relative flex gap-1.5 px-3 py-2.5 border-t border-line
                      max-[700px]:pb-[calc(0.625rem+env(safe-area-inset-bottom,0px))]">
        {mention && mention.matches.length > 0 && (
          <div className="absolute bottom-[calc(100%+4px)] left-3 min-w-[140px] max-h-[180px] overflow-y-auto
                          bg-surface2 border border-line rounded-md py-1
                          shadow-[0_4px_14px_rgba(0,0,0,0.5)] z-[10001]">
            {mention.matches.map((name, i) => (
              <div
                key={name}
                data-name={name}
                onMouseDown={(e) => { e.preventDefault(); pickMention(name); }}
                className={[
                  'px-3 py-1.5 text-xs font-mono cursor-pointer',
                  i === mention.active ? 'bg-royal text-chalk' : 'text-ink hover:bg-royal hover:text-chalk',
                ].join(' ')}
              >@{name}</div>
            ))}
          </div>
        )}
        <textarea
          ref={inputRef}
          value={draft}
          onChange={(e) => { setDraft(e.target.value); updateMention(e.currentTarget); }}
          onKeyDown={onKeyDown}
          onBlur={() => setTimeout(() => setMention(null), 150)}
          placeholder="Type a message…"
          maxLength={1000}
          rows={1}
          className="flex-1 bg-bg border border-line text-ink px-2.5 py-1.5
                     font-mono text-xs outline-none rounded-[2px] resize-none
                     focus:border-royal max-[700px]:text-base max-[700px]:py-2.5
                     max-[700px]:px-3 max-[700px]:min-h-[42px]"
          style={{ maxHeight: 120 }}
        />
        <button
          onClick={onSubmit}
          disabled={sending || !draft.trim()}
          className="bg-royal text-chalk px-3 py-1 rounded-[2px] text-sm font-mono
                     hover:bg-royal-light disabled:opacity-40 disabled:cursor-not-allowed"
          title="Send"
        >▶</button>
      </div>
    </div>
    </>
  );
}

// Global once-per-mount keyframe declaration (self-contained: no CSS file edit).
function FlashKeyframe() {
  return (
    <style>{`
      @keyframes loungeFlash {
        0%   { background: rgba(26,75,191,0.30); }
        100% { background: transparent; }
      }
    `}</style>
  );
}

// ── Per-message row ─────────────────────────────────────────────────────
interface MessageRowProps {
  m: LoungeMessage;
  currentUser: string | null;
  flashing: boolean;
  onReplyStart: () => void;
  onReact: (emoji: string) => void;
  onOpenPicker: () => void;
  pickerOpen: boolean;
  onClosePicker: () => void;
  onReplyClick: () => void;
  onOpenShow?: (showDate: string) => void;
}

function MessageRow({
  m, currentUser, flashing,
  onReplyStart, onReact, onOpenPicker, pickerOpen, onClosePicker,
  onReplyClick, onOpenShow,
}: MessageRowProps) {
  const mine = m.user === currentUser;
  const tokens = tokenizeText(m.text || '', currentUser);
  const rp = m.reply_preview;

  return (
    <div
      data-msg-id={m.id}
      className={[
        'group flex flex-col gap-[1px] py-0.5 relative',
        flashing && 'animate-[loungeFlash_1.4s_ease-out]',
      ].filter(Boolean).join(' ')}
    >
      <div className="flex items-baseline gap-2 text-[11px] max-[700px]:text-xs">
        <span className={[
          'font-bold',
          mine ? 'text-royal-bright' : 'text-accent',
        ].join(' ')}>{m.user}</span>
        <span className="text-muted text-[9px] max-[700px]:text-[10px]">{fmtLoungeTs(m.ts)}</span>
      </div>

      {m.reply_to && rp && (
        <div
          onClick={onReplyClick}
          title="Jump to replied message"
          className="border-l-[3px] border-l-royal bg-royal/10 hover:bg-royal/20
                     px-2 py-0.5 mb-[1px] rounded-r-md cursor-pointer text-[11px]
                     overflow-hidden max-[700px]:text-xs"
        >
          <span className="text-royal-bright font-semibold mr-1.5">↪ {rp.user}</span>
          <span className="text-muted overflow-hidden text-ellipsis whitespace-nowrap
                          inline-block max-w-full align-bottom">{rp.text}</span>
        </div>
      )}

      {m.text && (
        <div className="text-ink text-xs leading-[1.4] break-words whitespace-pre-wrap max-[700px]:text-sm max-[700px]:leading-[1.45]">
          {tokens.map((t, i) => t.kind === 'text'
            ? <React.Fragment key={i}>{t.value}</React.Fragment>
            : (
              <span
                key={i}
                className={[
                  'rounded-[3px] px-[3px] font-semibold',
                  t.mine
                    ? 'bg-accent/20 text-accent'
                    : 'bg-royal/20 text-royal-bright',
                ].join(' ')}
              >@{t.name}</span>
            ))}
        </div>
      )}

      {m.ref && (
        <div
          onClick={() => m.ref && onOpenShow?.(m.ref.show_date)}
          className="bg-surface2 border border-line border-l-2 border-l-accent
                     px-2 py-1 rounded-[2px] text-[11px] cursor-pointer
                     hover:border-l-royal flex flex-col gap-[1px]
                     max-[700px]:text-xs"
        >
          <span className="text-[8px] text-muted uppercase tracking-[1px]">▶ Shared show</span>
          <span className="text-ink font-bold max-[700px]:text-[13px]">
            {m.ref.show_date}
            {m.ref.venue ? ` · ${m.ref.venue}` : ''}
          </span>
        </div>
      )}

      <ReactionsRow
        m={m}
        currentUser={currentUser}
        onReact={onReact}
        onOpenPicker={onOpenPicker}
        pickerOpen={pickerOpen}
        onClosePicker={onClosePicker}
        onReplyStart={onReplyStart}
      />
    </div>
  );
}

interface ReactionsRowProps {
  m: LoungeMessage;
  currentUser: string | null;
  onReact: (emoji: string) => void;
  onOpenPicker: () => void;
  pickerOpen: boolean;
  onClosePicker: () => void;
  onReplyStart: () => void;
}

function ReactionsRow({
  m, currentUser, onReact, onOpenPicker, pickerOpen, onClosePicker, onReplyStart,
}: ReactionsRowProps) {
  const reactions = m.reactions || {};
  const chips: { emoji: string; users: string[]; mine: boolean }[] = [];
  for (const emoji of LOUNGE_REACTIONS) {
    const users = reactions[emoji] || [];
    if (!users.length) continue;
    chips.push({ emoji, users, mine: !!currentUser && users.includes(currentUser) });
  }

  return (
    <div className="flex flex-wrap gap-[3px] mt-0.5 items-center">
      {chips.map(c => (
        <span
          key={c.emoji}
          title={c.users.join(', ')}
          onClick={() => onReact(c.emoji)}
          className={[
            'select-none cursor-pointer inline-flex items-center gap-1',
            'px-2 py-[1px] rounded-full text-[11px] border transition-colors',
            c.mine
              ? 'bg-royal/20 border-royal'
              : 'bg-surface2 border-line hover:border-ink',
            'max-[700px]:text-[13px] max-[700px]:px-2.5 max-[700px]:py-0.5',
          ].join(' ')}
        >
          {c.emoji}
          <span className={[
            'font-mono text-[10px]',
            c.mine ? 'text-royal-bright' : 'text-muted',
          ].join(' ')}>{c.users.length}</span>
        </span>
      ))}

      <div className="relative" data-react-picker>
        <button
          onClick={(e) => { e.stopPropagation(); onOpenPicker(); }}
          title="React"
          className="bg-transparent border border-dashed border-line rounded-full
                     px-[7px] py-[1px] text-[11px] text-muted
                     opacity-0 group-hover:opacity-90 hover:text-ink hover:border-ink
                     hover:opacity-100 transition-opacity
                     max-[700px]:opacity-70 max-[700px]:text-[13px] max-[700px]:px-2.5"
        >+</button>
        {pickerOpen && (
          <div className="absolute bottom-[calc(100%+6px)] left-1/2 -translate-x-1/2
                          bg-surface2 border border-line rounded-2xl p-1 flex gap-0.5
                          shadow-[0_4px_14px_rgba(0,0,0,0.5)] z-[10001]">
            {LOUNGE_REACTIONS.map(em => (
              <button
                key={em}
                onClick={(e) => { e.stopPropagation(); onReact(em); onClosePicker(); }}
                className="bg-transparent border-0 text-lg px-1.5 py-1 leading-none
                           rounded-full hover:bg-surface transition-colors
                           max-[700px]:text-2xl max-[700px]:px-2.5 max-[700px]:py-1.5"
              >{em}</button>
            ))}
          </div>
        )}
      </div>

      <button
        onClick={onReplyStart}
        title="Reply"
        className="bg-transparent border border-dashed border-line rounded-full
                   px-2 py-[1px] text-[11px] text-muted
                   opacity-0 group-hover:opacity-90 hover:text-ink hover:border-ink
                   hover:opacity-100 transition-opacity
                   max-[700px]:opacity-70 max-[700px]:text-[13px] max-[700px]:px-2.5"
      >↪ Reply</button>
    </div>
  );
}

// CSS.escape isn't available in older jsdom test envs — inline a tiny fallback.
function cssEscape(s: string): string {
  if (typeof CSS !== 'undefined' && CSS.escape) return CSS.escape(s);
  return s.replace(/["\\]/g, '\\$&');
}
