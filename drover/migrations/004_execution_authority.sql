-- Drover 004_execution_authority.sql
-- Requester-owned operation trusts and user-owned cluster resource credentials replace tenant manager users.

CREATE TABLE IF NOT EXISTS `drover_delegations` (
  `id` CHAR(36) NOT NULL,
  `project_id` VARCHAR(64) NOT NULL,
  `cluster_id` CHAR(36) NOT NULL,
  `operation_id` CHAR(36) NULL,
  `action` VARCHAR(64) NOT NULL,
  `trust_id` VARCHAR(64) NOT NULL,
  `trustor_user_id` VARCHAR(64) NOT NULL,
  `trustee_user_id` VARCHAR(64) NOT NULL,
  `role_ids` JSON NOT NULL,
  `role_names` JSON NOT NULL,
  `expires_at` DATETIME(6) NOT NULL,
  `state` VARCHAR(16) NOT NULL DEFAULT 'active',
  `state_reason` VARCHAR(255) NULL,
  `created_at` DATETIME(6) NOT NULL,
  `released_at` DATETIME(6) NULL,
  `updated_at` DATETIME(6) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_drover_delegation_trust` (`trust_id`),
  KEY `idx_drover_delegation_cluster` (`cluster_id`),
  KEY `idx_drover_delegation_operation` (`operation_id`),
  KEY `idx_drover_delegation_state_expiry` (`state`, `expires_at`),
  CONSTRAINT `fk_drover_delegation_cluster_id` FOREIGN KEY (`cluster_id`) REFERENCES `k3s_clusters` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_drover_delegation_operation_id` FOREIGN KEY (`operation_id`) REFERENCES `drover_operations` (`id`) ON DELETE SET NULL,
  CONSTRAINT `ck_drover_delegation_state` CHECK (`state` IN ('active', 'released', 'revoked', 'deleted', 'expired'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `drover_cluster_credentials` (
  `id` CHAR(36) NOT NULL,
  `cluster_id` CHAR(36) NOT NULL,
  `project_id` VARCHAR(64) NOT NULL,
  `generation` INT NOT NULL,
  `purpose` VARCHAR(16) NOT NULL,
  `owner_user_id` VARCHAR(64) NULL,
  `app_credential_id` VARCHAR(64) NOT NULL,
  `secret_encrypted` TEXT NULL,
  `role_ids` JSON NULL,
  `role_names` JSON NULL,
  `guest_plugins` JSON NULL,
  `state` VARCHAR(16) NOT NULL,
  `state_reason` VARCHAR(255) NULL,
  `operation_id` CHAR(36) NULL,
  `last_error` TEXT NULL,
  `created_at` DATETIME(6) NOT NULL,
  `activated_at` DATETIME(6) NULL,
  `retired_at` DATETIME(6) NULL,
  `deleted_at` DATETIME(6) NULL,
  `updated_at` DATETIME(6) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_drover_cluster_cred_app_cred` (`app_credential_id`),
  UNIQUE KEY `idx_drover_cluster_cred_generation` (`cluster_id`, `generation`, `purpose`),
  KEY `idx_drover_cluster_cred_state` (`cluster_id`, `state`),
  KEY `idx_drover_cluster_cred_owner` (`owner_user_id`),
  CONSTRAINT `fk_drover_cluster_cred_cluster_id` FOREIGN KEY (`cluster_id`) REFERENCES `k3s_clusters` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_drover_cluster_cred_operation_id` FOREIGN KEY (`operation_id`) REFERENCES `drover_operations` (`id`) ON DELETE SET NULL,
  CONSTRAINT `ck_drover_cluster_cred_purpose` CHECK (`purpose` IN ('control', 'guest')),
  CONSTRAINT `ck_drover_cluster_cred_state` CHECK (`state` IN ('staged', 'active', 'retiring', 'deleted'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE `drover_jobs` ADD COLUMN IF NOT EXISTS `delegation_id` CHAR(36) NULL;
ALTER TABLE `drover_jobs` ADD KEY IF NOT EXISTS `idx_drover_jobs_delegation_id` (`delegation_id`);
ALTER TABLE `drover_jobs` ADD CONSTRAINT `fk_drover_jobs_delegation_id` FOREIGN KEY IF NOT EXISTS (`delegation_id`) REFERENCES `drover_delegations` (`id`) ON DELETE SET NULL;

ALTER TABLE `drover_operations` DROP CONSTRAINT IF EXISTS `ck_drover_op_kind`;
ALTER TABLE `drover_operations` ADD CONSTRAINT IF NOT EXISTS `ck_drover_op_kind` CHECK (`kind` IN ('create', 'scale', 'nodegroup_reconcile', 'delete', 'rotate_certificates', 'reconcile', 'reauthorize'));
