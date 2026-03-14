-- Add PIX columns to T_CIELO_SALES if missing (manual execution required).
-- Firebird does not support IF NOT EXISTS for columns; check before running.
ALTER TABLE T_CIELO_SALES ADD PIX_ID VARCHAR(40);
ALTER TABLE T_CIELO_SALES ADD TX_ID VARCHAR(36);
ALTER TABLE T_CIELO_SALES ADD PIX_PAYMENT_ID VARCHAR(36);
