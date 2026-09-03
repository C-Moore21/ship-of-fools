import { useEffect, useState } from 'react';
import type { UseAuth } from './useAuth';

interface Props {
  open: boolean;
  onClose: () => void;
  auth: UseAuth;
  initialMode?: 'login' | 'register';
}

export function LoginModal({ open, onClose, auth, initialMode = 'login' }: Props) {
  const [mode, setMode] = useState<'login' | 'register'>(initialMode);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [err, setErr] = useState('');
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (open) {
      setMode(initialMode);
      setUsername('');
      setPassword('');
      setErr('');
    }
  }, [open, initialMode]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, onClose]);

  if (!open) return null;

  const submit = async (e?: React.FormEvent) => {
    e?.preventDefault();
    setErr('');
    setBusy(true);
    const fn = mode === 'login' ? auth.login : auth.register;
    const errMsg = await fn(username.trim(), password);
    setBusy(false);
    if (errMsg) {
      setErr(errMsg);
      return;
    }
    onClose();
  };

  const isLogin = mode === 'login';

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm"
      onClick={onClose}
      role="dialog"
      aria-modal="true"
    >
      <form
        onClick={(e) => e.stopPropagation()}
        onSubmit={submit}
        className="w-full max-w-sm rounded-lg border border-accent2 bg-surface2 p-6 font-mono shadow-2xl"
        style={{ fontFamily: '"Space Mono", monospace' }}
      >
        <h2
          className="mb-1 text-2xl text-text"
          style={{ fontFamily: '"Playfair Display", serif' }}
        >
          {isLogin ? 'Welcome Back' : 'Create Account'}
        </h2>
        <p className="mb-4 text-sm text-muted">
          {isLogin ? 'Sign in to rate shows and save notes' : 'Join to save your ratings'}
        </p>

        <label className="mb-3 block">
          <span className="mb-1 block text-xs uppercase tracking-wider text-muted">
            Username
          </span>
          <input
            autoFocus
            type="text"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            className="w-full rounded border border-border bg-surface px-3 py-2 text-text outline-none focus:border-accent2"
            autoComplete="username"
          />
        </label>

        <label className="mb-4 block">
          <span className="mb-1 block text-xs uppercase tracking-wider text-muted">
            Password
          </span>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            className="w-full rounded border border-border bg-surface px-3 py-2 text-text outline-none focus:border-accent2"
            autoComplete={isLogin ? 'current-password' : 'new-password'}
          />
        </label>

        {err && (
          <div className="mb-3 rounded border border-accent/50 bg-accent/10 px-3 py-2 text-sm text-accent">
            {err}
          </div>
        )}

        <button
          type="submit"
          disabled={busy || !username || !password}
          className="w-full rounded bg-accent2 px-4 py-2 font-bold uppercase tracking-wider text-white transition hover:brightness-110 disabled:opacity-50"
        >
          {busy ? '…' : isLogin ? 'Log In' : 'Register'}
        </button>

        <div className="mt-4 text-center text-sm text-muted">
          {isLogin ? "Don't have an account? " : 'Already have an account? '}
          <button
            type="button"
            className="text-accent2 underline"
            onClick={() => {
              setMode(isLogin ? 'register' : 'login');
              setErr('');
            }}
          >
            {isLogin ? 'Register' : 'Log In'}
          </button>
        </div>
      </form>
    </div>
  );
}

export default LoginModal;
