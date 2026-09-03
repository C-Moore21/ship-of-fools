import { useEffect, useRef, useState } from 'react'
import { searchAll, type SearchResult } from './api'

const DEBOUNCE_MS = 200

export interface SearchState {
  results: SearchResult[]
  loading: boolean
  error: string | null
  /** True when the query is non-empty but shorter than the min length. */
  tooShort: boolean
}

/**
 * Debounced search hook. Fires ~200ms after the query settles, aborts any
 * in-flight request when the query changes, and swallows AbortError so a
 * superseded query never overwrites the current results.
 */
export function useSearch(query: string): SearchState {
  const [state, setState] = useState<SearchState>({
    results: [],
    loading: false,
    error: null,
    tooShort: false,
  })
  const abortRef = useRef<AbortController | null>(null)

  useEffect(() => {
    const q = query.trim()

    // Cancel any prior in-flight request.
    if (abortRef.current) {
      abortRef.current.abort()
      abortRef.current = null
    }

    if (!q) {
      setState({ results: [], loading: false, error: null, tooShort: false })
      return
    }
    if (q.length < 2) {
      setState({ results: [], loading: false, error: null, tooShort: true })
      return
    }

    setState((s) => ({ ...s, loading: true, error: null, tooShort: false }))

    const timer = window.setTimeout(() => {
      const ctrl = new AbortController()
      abortRef.current = ctrl
      searchAll(q, ctrl.signal)
        .then((results) => {
          if (ctrl.signal.aborted) return
          setState({ results, loading: false, error: null, tooShort: false })
        })
        .catch((err: unknown) => {
          if (ctrl.signal.aborted) return
          if (err instanceof DOMException && err.name === 'AbortError') return
          const message = err instanceof Error ? err.message : String(err)
          setState({ results: [], loading: false, error: message, tooShort: false })
        })
    }, DEBOUNCE_MS)

    return () => {
      window.clearTimeout(timer)
    }
  }, [query])

  // Abort on unmount.
  useEffect(() => {
    return () => {
      abortRef.current?.abort()
    }
  }, [])

  return state
}
