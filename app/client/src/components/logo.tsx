// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

export function IceLogo({ className }: { className?: string }) {
  return (
    <svg viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg" className={className} aria-hidden="true">
      <defs>
        <linearGradient id="b-top" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="#dbeafe" /><stop offset="100%" stopColor="#93c5fd" />
        </linearGradient>
        <linearGradient id="b-left" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stopColor="#60a5fa" /><stop offset="100%" stopColor="#2563eb" />
        </linearGradient>
        <linearGradient id="b-right" x1="100%" y1="0%" x2="0%" y2="100%">
          <stop offset="0%" stopColor="#93c5fd" /><stop offset="100%" stopColor="#3b82f6" />
        </linearGradient>
      </defs>
      <polygon points="20,4 36,13 20,22 4,13" fill="url(#b-top)" />
      <polygon points="4,13 20,22 20,37 4,28" fill="url(#b-left)" />
      <polygon points="36,13 20,22 20,37 36,28" fill="url(#b-right)" />
      <line x1="10" y1="10" x2="16" y2="13" stroke="white" strokeWidth="1.5" opacity="0.5" strokeLinecap="round" />
      <line x1="8" y1="18" x2="12" y2="20" stroke="white" strokeWidth="1" opacity="0.3" strokeLinecap="round" />
      <line x1="20" y1="22" x2="20" y2="37" stroke="white" strokeWidth="0.5" opacity="0.3" />
      <line x1="4" y1="13" x2="20" y2="22" stroke="white" strokeWidth="0.5" opacity="0.2" />
    </svg>
  )
}
