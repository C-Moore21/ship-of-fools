import React from 'react'

interface StealieProps {
  className?: string
}

/**
 * Ship of Fools logo — same PNG the classic UI uses. Kept as a wrapper
 * component so callers don't need to know about the asset path.
 */
export function Stealie({ className = 'h-8 w-8' }: StealieProps) {
  return (
    <img
      src="/static/stealie.png"
      alt="Ship of Fools"
      className={className}
      draggable={false}
    />
  )
}
