-- Drover 003_cluster_reconciliation.sql

ALTER TABLE `k3s_clusters`
  ADD COLUMN IF NOT EXISTS `last_reconciled_at` DATETIME(6) NULL,
  ADD COLUMN IF NOT EXISTS `drift_status` JSON NULL;
