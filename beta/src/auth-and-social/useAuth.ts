import { useCallback, useEffect, useState } from 'react';
import { getMe, login as apiLogin, logout as apiLogout, register as apiRegister } from './api';

export interface UseAuth {
  user: string | null;
  loading: boolean;
  login: (username: string, password: string) => Promise<string | null>; // null = ok, string = error
  register: (username: string, password: string) => Promise<string | null>;
  logout: () => Promise<void>;
  refresh: () => Promise<void>;
}

export function useAuth(): UseAuth {
  const [user, setUser] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    try {
      const me = await getMe();
      setUser(me.username);
    } catch {
      setUser(null);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const login = useCallback(async (username: string, password: string) => {
    const r = await apiLogin(username, password);
    if (r.ok && r.username) {
      setUser(r.username);
      return null;
    }
    return r.error || 'Login failed';
  }, []);

  const register = useCallback(async (username: string, password: string) => {
    const r = await apiRegister(username, password);
    if (r.ok && r.username) {
      setUser(r.username);
      return null;
    }
    return r.error || 'Register failed';
  }, []);

  const logout = useCallback(async () => {
    await apiLogout();
    setUser(null);
  }, []);

  return { user, loading, login, register, logout, refresh };
}
