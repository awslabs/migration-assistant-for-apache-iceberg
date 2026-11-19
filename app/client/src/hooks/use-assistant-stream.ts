// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useState, useCallback, useRef } from 'react'
import { useQueryClient } from '@tanstack/react-query'

export interface ToolCallEvent {
  id: string
  tool: string
  table: string
  status: 'calling' | 'done'
  timestamp: number
}

export interface AnalysisProgress {
  isRunning: boolean
  tablesCount: number
  currentTable: string
  currentTableIndex: number
  toolCalls: ToolCallEvent[]
  summary: string | null
  tablesAnalyzed: number
  error: string | null
  // Batch progress — large plans are analyzed in batches server-side.
  batch: number
  batchesTotal: number
}

const initialProgress: AnalysisProgress = {
  isRunning: false,
  tablesCount: 0,
  currentTable: '',
  currentTableIndex: -1,
  toolCalls: [],
  summary: null,
  tablesAnalyzed: 0,
  error: null,
  batch: 0,
  batchesTotal: 0,
}

export function useAssistantStream() {
  const [progress, setProgress] = useState<AnalysisProgress>(initialProgress)
  const abortRef = useRef<AbortController | null>(null)
  // Monotonic counter for tool-call ids. Date.now() collides when many tool
  // calls (often the same tool name) arrive within the same millisecond, which
  // produces duplicate React keys — React then fails to reconcile the sliced
  // list and stale rows linger in the DOM, making the feed appear to grow past
  // its cap. A strictly-increasing counter guarantees unique keys.
  const toolSeqRef = useRef(0)
  const queryClient = useQueryClient()

  const startAnalysis = useCallback(async (planId: string) => {
    // Reset state
    toolSeqRef.current = 0
    setProgress({ ...initialProgress, isRunning: true })

    const controller = new AbortController()
    abortRef.current = controller

    try {
      const res = await fetch('/api/assistant/stream', {
        method: 'POST',
        // Auth rides the HttpOnly session cookie (M7); include credentials so
        // the browser sends it with this streaming request.
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ plan_id: planId }),
        signal: controller.signal,
      })

      if (!res.ok) {
        const err = await res.json().catch(() => ({ detail: res.statusText }))
        setProgress(p => ({ ...p, isRunning: false, error: err.detail || 'Request failed' }))
        return
      }

      const reader = res.body?.getReader()
      if (!reader) {
        setProgress(p => ({ ...p, isRunning: false, error: 'No response stream' }))
        return
      }

      const decoder = new TextDecoder()
      let buffer = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() || ''

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          const payload = line.slice(6).trim()
          if (!payload) continue

          try {
            const event = JSON.parse(payload)

            switch (event.type) {
              case 'start':
                setProgress(p => ({ ...p, tablesCount: event.tables_count }))
                break

              case 'analyzing_table':
                setProgress(p => ({
                  ...p,
                  currentTable: event.table,
                  currentTableIndex: event.index,
                }))
                break

              case 'batch_progress':
                setProgress(p => ({
                  ...p,
                  batch: event.batch ?? p.batch,
                  batchesTotal: event.batches_total ?? p.batchesTotal,
                }))
                break

              case 'tool_call':
                setProgress(p => ({
                  ...p,
                  toolCalls: [
                    ...p.toolCalls,
                    {
                      id: `${event.tool}-${toolSeqRef.current++}`,
                      tool: event.tool,
                      table: event.table,
                      status: 'calling',
                      timestamp: Date.now(),
                    },
                  ],
                }))
                break

              case 'tool_result':
                setProgress(p => ({
                  ...p,
                  toolCalls: p.toolCalls.map(tc =>
                    tc.tool === event.tool && tc.table === event.table && tc.status === 'calling'
                      ? { ...tc, status: 'done' }
                      : tc
                  ),
                }))
                break

              case 'table_done':
                setProgress(p => ({
                  ...p,
                  toolCalls: p.toolCalls.map(tc =>
                    tc.table === event.table ? { ...tc, status: 'done' } : tc
                  ),
                }))
                break

              case 'complete':
                setProgress(p => ({
                  ...p,
                  isRunning: false,
                  summary: event.summary,
                  tablesAnalyzed: event.tables_analyzed,
                }))
                // Refresh plan data
                queryClient.invalidateQueries({ queryKey: ['plans'] })
                break

              case 'error':
                setProgress(p => ({
                  ...p,
                  isRunning: false,
                  error: event.message,
                }))
                queryClient.invalidateQueries({ queryKey: ['plans'] })
                break
            }
          } catch {
            // Skip malformed events
          }
        }
      }
    } catch (e: any) {
      if (e.name !== 'AbortError') {
        setProgress(p => ({
          ...p,
          isRunning: false,
          error: e.message || 'Connection failed',
        }))
      }
    }
  }, [queryClient])

  const cancel = useCallback(() => {
    abortRef.current?.abort()
    setProgress(p => ({ ...p, isRunning: false }))
  }, [])

  const reset = useCallback(() => {
    setProgress(initialProgress)
  }, [])

  return { progress, startAnalysis, cancel, reset }
}
