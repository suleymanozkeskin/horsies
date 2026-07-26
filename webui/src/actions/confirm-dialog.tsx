import { useEffect, useId, useState } from 'react';

import { Loader2 } from 'lucide-react';

import type { ConfirmCopy } from '@/actions/copy';
import type { ActionAvailability } from '@/actions/eligibility';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import { StatusChip } from '@/components/ui/status-chip';

interface ActionConfirmDialogProps {
  open: boolean;
  copy: ConfirmCopy;
  /** Freshest server status, shown so an operator confirms against live truth. */
  currentStatus: string;
  /** Recomputed on every render; a poll can make the action ineligible mid-dialog. */
  availability: ActionAvailability;
  submitting: boolean;
  onConfirm: () => void;
  onDismiss: () => void;
}

/**
 * Confirmation for one action. The dialog re-validates eligibility on every
 * render: if a concurrent change makes the action inapplicable while the dialog
 * is open, Confirm is replaced by an explanation and a Close button rather than
 * letting the operator submit a request that is already doomed.
 */
export function ActionConfirmDialog({
  open,
  copy,
  currentStatus,
  availability,
  submitting,
  onConfirm,
  onDismiss,
}: ActionConfirmDialogProps) {
  const [acknowledged, setAcknowledged] = useState(false);
  const checkboxId = useId();

  // A fresh dialog must never inherit a previous acknowledgement.
  useEffect(() => {
    if (open) {
      setAcknowledged(false);
    }
  }, [open]);

  const stillEligible = availability.shown && availability.enabled;
  const needsAcknowledgement = copy.acknowledgement !== undefined;
  const canConfirm =
    stillEligible && !submitting && (!needsAcknowledgement || acknowledged);

  return (
    <Dialog
      open={open}
      onOpenChange={next => {
        if (!next) {
          onDismiss();
        }
      }}
    >
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{copy.title}</DialogTitle>
          <DialogDescription>{copy.body}</DialogDescription>
        </DialogHeader>

        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span>Current status</span>
          <StatusChip status={currentStatus} />
        </div>

        {availability.shown && availability.enabled ? (
          needsAcknowledgement && (
            <label
              htmlFor={checkboxId}
              className="flex items-start gap-2 text-sm"
            >
              <input
                id={checkboxId}
                type="checkbox"
                checked={acknowledged}
                onChange={event => setAcknowledged(event.target.checked)}
                className="mt-0.5 size-4 accent-primary"
              />
              <span>{copy.acknowledgement}</span>
            </label>
          )
        ) : (
          <p className="text-sm" style={{ color: 'var(--warning-dark)' }}>
            {availability.shown
              ? availability.disabledReason
              : 'This action no longer applies — the status changed while this dialog was open.'}
          </p>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={onDismiss} disabled={submitting}>
            {stillEligible ? copy.dismissLabel : 'Close'}
          </Button>
          {stillEligible && (
            <Button
              variant="destructive"
              onClick={onConfirm}
              disabled={!canConfirm}
            >
              {submitting && <Loader2 className="size-4 animate-spin" />}
              {copy.confirmLabel}
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
