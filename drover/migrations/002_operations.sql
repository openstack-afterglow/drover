-- Drover 002_operations.sql

CREATE TABLE IF NOT EXISTS `drover_operations` (
  `id` CHAR(36) NOT NULL,
  `project_id` VARCHAR(64) NOT NULL,
  `cluster_id` CHAR(36) NOT NULL,
  `kind` VARCHAR(32) NOT NULL,
  `status` VARCHAR(20) NOT NULL DEFAULT 'QUEUED',
  `request_id` VARCHAR(64) NULL,
  `idempotency_key` VARCHAR(128) NULL,
  `request_hash` VARCHAR(64) NULL,
  `error` TEXT NULL,
  `created_at` DATETIME(6) NOT NULL,
  `started_at` DATETIME(6) NULL,
  `finished_at` DATETIME(6) NULL,
  PRIMARY KEY (`id`),
  KEY `idx_drover_op_project_id` (`project_id`),
  KEY `idx_drover_op_cluster_id` (`cluster_id`),
  KEY `idx_drover_op_project_created` (`project_id`, `created_at`),
  KEY `idx_drover_op_cluster_created` (`cluster_id`, `created_at`),
  UNIQUE KEY `idx_drover_op_proj_idemp` (`project_id`, `idempotency_key`),
  CONSTRAINT `fk_drover_op_cluster_id` FOREIGN KEY (`cluster_id`) REFERENCES `k3s_clusters` (`id`) ON DELETE CASCADE,
  CONSTRAINT `ck_drover_op_kind` CHECK (`kind` IN ('create', 'scale', 'nodegroup_reconcile', 'delete', 'rotate_certificates', 'reconcile')),
  CONSTRAINT `ck_drover_op_status` CHECK (`status` IN ('QUEUED', 'RUNNING', 'WAITING_CALLBACK', 'SUCCEEDED', 'FAILED', 'CANCELLED'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `drover_operation_events` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `operation_id` CHAR(36) NOT NULL,
  `sequence` INT NOT NULL,
  `phase` VARCHAR(64) NOT NULL,
  `message` TEXT NULL,
  `payload_json` JSON NULL,
  `created_at` DATETIME(6) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_drover_op_event_operation_id` (`operation_id`),
  UNIQUE KEY `idx_drover_op_event_seq` (`operation_id`, `sequence`),
  CONSTRAINT `fk_drover_op_event_operation_id` FOREIGN KEY (`operation_id`) REFERENCES `drover_operations` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `managed_openstack_resources` (
  `id` CHAR(36) NOT NULL,
  `cluster_id` CHAR(36) NOT NULL,
  `operation_id` CHAR(36) NULL,
  `service` VARCHAR(32) NOT NULL,
  `resource_type` VARCHAR(64) NOT NULL,
  `resource_id` VARCHAR(128) NOT NULL,
  `name` VARCHAR(255) NULL,
  `state` VARCHAR(32) NULL,
  `metadata_json` JSON NULL,
  `created_at` DATETIME(6) NOT NULL,
  `last_seen_at` DATETIME(6) NOT NULL,
  `deleted_at` DATETIME(6) NULL,
  PRIMARY KEY (`id`),
  KEY `idx_managed_res_cluster_id` (`cluster_id`),
  KEY `idx_managed_res_operation_id` (`operation_id`),
  KEY `idx_managed_res_deleted_at` (`deleted_at`),
  UNIQUE KEY `idx_managed_res_identity` (`service`, `resource_type`, `resource_id`),
  CONSTRAINT `fk_managed_res_cluster_id` FOREIGN KEY (`cluster_id`) REFERENCES `k3s_clusters` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_managed_res_operation_id` FOREIGN KEY (`operation_id`) REFERENCES `drover_operations` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE `drover_jobs` ADD COLUMN `operation_id` CHAR(36) NULL;
ALTER TABLE `drover_jobs` ADD KEY `idx_drover_jobs_operation_id` (`operation_id`);
ALTER TABLE `drover_jobs` ADD CONSTRAINT `fk_drover_jobs_operation_id` FOREIGN KEY (`operation_id`) REFERENCES `drover_operations` (`id`) ON DELETE SET NULL;
