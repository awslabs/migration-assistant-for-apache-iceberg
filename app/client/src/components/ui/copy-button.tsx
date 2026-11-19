// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import { useState, useCallback, useRef, useEffect } from 'react'
import { Copy, Check } from 'lucide-react'
import { cn } from '@/lib/utils'
import { useToast } from '@/components/ui/toast'

interface CopyButtonProps {
  /** The full value to copy to the clipboard. */
  value: string
  /** Optional label used in the "Copied <label>" toast (e.g. "pipeline ID"). */
  label?: string
  /** Extra classes for the button. */
  className?: string
  /** Accessible title/tooltip. Defaults to "Copy". */
  title?: string
}

/**
 * Tiny inline copy-to-clipboard icon button. Place it immediately to the right
 * of a value (pipeline ID, EMR job run ID, ARN, etc.). Shows a brief check mark
 * on success and fires a toast.
 */
export function CopyButton({ value, label, className, title }: CopyButtonProps) {
  const [copied, setCopied] = useState(false)
  const { showToast } = useToast()
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  useEffect(() => () => { if (timerRef.current) clearTimeout(timerRef.current) }, [])

  const onCopy = useCallback(async (e: React.MouseEvent) => {
    // Don't let the click bubble to parent row handlers (e.g. open detail panel).
    e.stopPropagation()
    e.preventDefault()
    if (!value) return
    try {
      if (navigator.clipboard?.writeText) {
        await navigator.clipboard.writeText(value)
      } else {
        // Fallback for non-secure contexts / older browsers.
        const ta = document.createElement('textarea')
        ta.value = value
        ta.style.position = 'fixed'
        ta.style.opacity = '0'
        document.body.appendChild(ta)
        ta.select()
        document.execCommand('copy')
        document.body.removeChild(ta)
      }
      setCopied(true)
      showToast(label ? `Copied ${label}` : 'Copied', 'success')
      if (timerRef.current) clearTimeout(timerRef.current)
      timerRef.current = setTimeout(() => setCopied(false), 1500)
    } catch {
      showToast('Failed to copy', 'error')
    }
  }, [value, label, showToast])

  return (
    <button
      type="button"
      onClick={onCopy}
      title={title || (label ? `Copy ${label}` : 'Copy')}
      aria-label={title || (label ? `Copy ${label}` : 'Copy')}
      className={cn(
        'inline-flex items-center justify-center shrink-0 rounded p-0.5 text-muted-foreground',
        'hover:text-foreground hover:bg-accent transition-colors cursor-pointer',
        className,
      )}
    >
      {copied
        ? <Check className="h-3 w-3 text-emerald-500" />
        : <Copy className="h-3 w-3" />}
    </button>
  )
}
