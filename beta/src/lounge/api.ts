// Typed fetch wrappers for the Lounge (private chat) endpoints.
// Endpoints live at /api/chat/lounge/* in app.py.

export interface LoungeRef {
  show_date: string;
  venue?: string;
  location?: string;
  source_id?: string;
}

export interface LoungeReplyPreview {
  user: string;
  text: string;
}

export interface LoungeMessage {
  id: string;
  user: string;
  text: string;
  ts: string;              // ISO timestamp
  bumped_at: string;       // ISO — updated on reaction changes too
  ref?: LoungeRef | null;
  reactions: Record<string, string[]>; // emoji -> [usernames]
  reply_to?: string | null;
  reply_preview?: LoungeReplyPreview | null;
}

export interface AccessResp { member: boolean }
export interface UnreadResp { member: boolean; unread: number }
export interface ReactResp {
  id: string;
  reactions: Record<string, string[]>;
  bumped_at: string;
}

async function jsonFetch<T>(url: string, init?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    credentials: 'same-origin',
    ...init,
    headers: {
      ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
      ...(init?.headers || {}),
    },
  });
  if (!res.ok) {
    let err: any = {};
    try { err = await res.json(); } catch {}
    throw new Error(err.error || `HTTP ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export const loungeApi = {
  access: () => jsonFetch<AccessResp>('/api/chat/lounge/access'),

  unread: () => jsonFetch<UnreadResp>('/api/chat/lounge/unread'),

  messages: (since?: string | null, limit = 100) => {
    const p = new URLSearchParams();
    if (since) p.set('since', since);
    p.set('limit', String(limit));
    return jsonFetch<LoungeMessage[]>(`/api/chat/lounge/messages?${p.toString()}`);
  },

  send: (text: string, opts?: { ref?: LoungeRef | null; reply_to?: string | null }) =>
    jsonFetch<LoungeMessage>('/api/chat/lounge/send', {
      method: 'POST',
      body: JSON.stringify({
        text,
        ref: opts?.ref ?? null,
        reply_to: opts?.reply_to ?? null,
      }),
    }),

  react: (message_id: string, emoji: string) =>
    jsonFetch<ReactResp>('/api/chat/lounge/react', {
      method: 'POST',
      body: JSON.stringify({ message_id, emoji }),
    }),

  markRead: () =>
    jsonFetch<{ ok: boolean }>('/api/chat/lounge/read', { method: 'POST' }),
};
