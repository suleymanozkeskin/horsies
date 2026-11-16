-- Schema v30: reservation-registry maintenance index.
CREATE INDEX horsies_key_reservations_expiry_idx
        ON horsies_key_reservations (expires_at)
        WHERE disposition = 'TERMINAL';
