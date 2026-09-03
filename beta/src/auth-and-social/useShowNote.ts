import { useCallback, useEffect, useRef, useState } from 'react';
import { getMyNote, saveNote } from './api';

export type NoteStatus = 'idle' | 'saving' | 'saved' | 'error';

export interface UseShowNote {
  note: string;
  loading: boolean;
  status: NoteStatus;
  error: string | null;
  setNote: (text: string) => void;    // debounces a save
  flush: () => Promise<void>;         // force-save immediately (e.g. on blur)
}

const DEBOUNCE_MS = 1000;

export function useShowNote(
  showDate: string | null | undefined,
  loggedIn: boolean,
): UseShowNote {
  const [note, setNoteState] = useState('');
  const [loading, setLoading] = useState(false);
  const [status, setStatus] = useState<NoteStatus>('idle');
  const [error, setError] = useState<string | null>(null);
  const timerRef = useRef<number | null>(null);
  const pendingRef = useRef<string>('');
  const showRef = useRef<string | null | undefined>(showDate);

  useEffect(() => {
    showRef.current = showDate;
    setNoteState('');
    setStatus('idle');
    setError(null);
    if (!showDate || !loggedIn) return;
    let cancelled = false;
    setLoading(true);
    getMyNote(showDate)
      .then((v) => {
        if (!cancelled) {
          setNoteState(v);
          pendingRef.current = v;
        }
      })
      .catch((e) => {
        if (!cancelled) setError(String(e?.message || e));
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [showDate, loggedIn]);

  const doSave = useCallback(async () => {
    const d = showRef.current;
    if (!d || !loggedIn) return;
    const text = pendingRef.current;
    setStatus('saving');
    setError(null);
    try {
      await saveNote(d, text);
      setStatus('saved');
      window.setTimeout(() => {
        setStatus((s) => (s === 'saved' ? 'idle' : s));
      }, 1500);
    } catch (e: any) {
      setStatus('error');
      setError(String(e?.message || e));
    }
  }, [loggedIn]);

  const setNote = useCallback(
    (text: string) => {
      setNoteState(text);
      pendingRef.current = text;
      if (!loggedIn) return;
      if (timerRef.current) window.clearTimeout(timerRef.current);
      setStatus('idle');
      timerRef.current = window.setTimeout(() => {
        timerRef.current = null;
        void doSave();
      }, DEBOUNCE_MS);
    },
    [loggedIn, doSave],
  );

  const flush = useCallback(async () => {
    if (timerRef.current) {
      window.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
    await doSave();
  }, [doSave]);

  // Cleanup on unmount: fire any pending save.
  useEffect(() => {
    return () => {
      if (timerRef.current) {
        window.clearTimeout(timerRef.current);
        timerRef.current = null;
        void doSave();
      }
    };
  }, [doSave]);

  return { note, loading, status, error, setNote, flush };
}
