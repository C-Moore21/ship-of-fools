import { useEffect, useRef } from 'react';
import type { UseShowNote } from './useShowNote';

interface Props {
  hook: UseShowNote;
  loggedIn: boolean;
  onRequestLogin?: () => void;
  placeholder?: string;
  label?: string;
  minRows?: number;
  maxRows?: number;
}

/**
 * Expandable textarea for the ShowDetail sidebar's "Your note" slot.
 * Autosaves via the useShowNote hook (debounced) and shows a subtle
 * saved/error indicator. Grows with content up to maxRows.
 */
export function NoteEditor({
  hook,
  loggedIn,
  onRequestLogin,
  placeholder = 'Your thoughts on this show…',
  label = 'Your Note',
  minRows = 3,
  maxRows = 14,
}: Props) {
  const ref = useRef<HTMLTextAreaElement | null>(null);

  // Auto-grow
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    el.style.height = 'auto';
    const lineHeight = 20; // approx for Space Mono at text-sm
    const min = minRows * lineHeight + 16;
    const max = maxRows * lineHeight + 16;
    el.style.height = Math.min(Math.max(el.scrollHeight, min), max) + 'px';
  }, [hook.note, minRows, maxRows]);

  if (!loggedIn) {
    return (
      <div className="rounded border border-border bg-surface p-3 font-mono text-sm text-muted">
        <div className="mb-2 text-xs uppercase tracking-wider">{label}</div>
        <button
          type="button"
          className="text-accent2 underline"
          onClick={onRequestLogin}
        >
          Log in
        </button>{' '}
        to save your notes on this show.
      </div>
    );
  }

  const statusText =
    hook.status === 'saving' ? 'Saving…'
    : hook.status === 'saved' ? 'Saved'
    : hook.status === 'error' ? (hook.error || 'Save failed')
    : hook.loading ? 'Loading…'
    : '';

  const statusColor =
    hook.status === 'error' ? 'text-accent'
    : hook.status === 'saved' ? 'text-accent2'
    : 'text-muted';

  return (
    <div className="rounded border border-border bg-surface p-3 font-mono">
      <div className="mb-2 flex items-baseline justify-between">
        <div className="text-xs uppercase tracking-wider text-muted">{label}</div>
        <div className={`text-[10px] ${statusColor}`}>{statusText}</div>
      </div>
      <textarea
        ref={ref}
        value={hook.note}
        onChange={(e) => hook.setNote(e.target.value)}
        onBlur={() => void hook.flush()}
        placeholder={placeholder}
        rows={minRows}
        className="w-full resize-none rounded border border-border bg-surface2 px-3 py-2 text-sm text-text outline-none focus:border-accent2"
        style={{ fontFamily: '"Space Mono", monospace', minHeight: `${minRows * 20 + 16}px` }}
      />
    </div>
  );
}

export default NoteEditor;
