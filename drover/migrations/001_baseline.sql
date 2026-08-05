-- Drover 001_baseline.sql

CREATE TABLE IF NOT EXISTS `k3s_clusters` (
  `id` CHAR(36) NOT NULL,
  `project_id` VARCHAR(64) NOT NULL,
  `name` VARCHAR(63) NOT NULL,
  `status` VARCHAR(20) NOT NULL DEFAULT 'CREATING',
  `status_reason` TEXT NULL,
  `server_vm_id` VARCHAR(64) NULL,
  `server_flavor_id` VARCHAR(64) NULL,
  `agent_flavor_id` VARCHAR(64) NULL,
  `server_image_id` VARCHAR(128) NULL,
  `network_id` VARCHAR(64) NULL,
  `security_group_id` VARCHAR(64) NULL,
  `api_lb_id` VARCHAR(64) NULL,
  `api_lb_pool_id` VARCHAR(64) NULL,
  `api_fip_id` VARCHAR(64) NULL,
  `api_fip_address` VARCHAR(45) NULL,
  `server_ip` VARCHAR(45) NULL,
  `api_address` VARCHAR(255) NULL,
  `k3s_version` VARCHAR(32) NULL,
  `node_token` VARCHAR(512) NULL,
  `key_name` VARCHAR(255) NULL,
  `ssh_public_key` TEXT NULL,
  `kubeconfig_encrypted` TEXT NULL,
  `created_by_user_id` VARCHAR(64) NULL,
  `created_by_username` VARCHAR(255) NULL,
  `agent_count` INT NOT NULL DEFAULT 0,
  `occm_enabled` TINYINT(1) NOT NULL DEFAULT 0,
  `plugins_enabled` JSON NULL,
  `plugin_status` JSON NULL,
  `secret_cloud_config_status` VARCHAR(20) NULL,
  `os_type` VARCHAR(10) NOT NULL DEFAULT 'ubuntu',
  `app_credential_id` VARCHAR(64) NULL,
  `created_at` DATETIME(6) NOT NULL,
  `updated_at` DATETIME(6) NOT NULL,
  `deleted_at` DATETIME(6) NULL,
  `deleted_by_user_id` VARCHAR(64) NULL,
  `deleted_reason` VARCHAR(255) NULL,
  `master_count` INT NOT NULL DEFAULT 1,
  `template_id` CHAR(36) NULL,
  `template_snapshot` JSON NULL,
  `resource_policy_snapshot` JSON NULL,
  `stampede_enabled` TINYINT(1) NOT NULL DEFAULT 0,
  `last_rotation_at` DATETIME(6) NULL,
  `last_rotation_initiated_by` VARCHAR(64) NULL,
  PRIMARY KEY (`id`),
  KEY `idx_k3s_clusters_project_id` (`project_id`),
  KEY `idx_k3s_clusters_created_by_user_id` (`created_by_user_id`),
  KEY `idx_k3s_clusters_deleted_at` (`deleted_at`),
  KEY `idx_k3s_cluster_project_created` (`project_id`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `k3s_agent_vms` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `cluster_id` CHAR(36) NOT NULL,
  `vm_id` VARCHAR(64) NOT NULL,
  `name` VARCHAR(255) NULL,
  `status` VARCHAR(20) NOT NULL DEFAULT 'CREATING',
  `created_at` DATETIME(6) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_k3s_agent_vms_cluster_id` (`cluster_id`),
  KEY `idx_k3s_agent_vms_vm_id` (`vm_id`),
  CONSTRAINT `fk_k3s_agent_vms_cluster_id` FOREIGN KEY (`cluster_id`) REFERENCES `k3s_clusters` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `k3s_nodegroups` (
  `id` CHAR(36) NOT NULL,
  `cluster_id` CHAR(36) NOT NULL,
  `name` VARCHAR(63) NOT NULL,
  `role` VARCHAR(10) NOT NULL DEFAULT 'agent',
  `node_count` INT NOT NULL DEFAULT 0,
  `flavor_id` VARCHAR(64) NULL,
  `image_id` VARCHAR(64) NULL,
  `labels` JSON NULL,
  `taints` JSON NULL,
  `is_default` TINYINT(1) NOT NULL DEFAULT 0,
  `stampede_enabled` TINYINT(1) NOT NULL DEFAULT 0,
  `min_size` INT NOT NULL DEFAULT 0,
  `max_size` INT NOT NULL DEFAULT 5,
  `stampede_state` JSON NULL,
  `created_at` DATETIME(6) NOT NULL,
  `updated_at` DATETIME(6) NOT NULL,
  `deleted_at` DATETIME(6) NULL,
  PRIMARY KEY (`id`),
  KEY `idx_k3s_nodegroups_cluster_id` (`cluster_id`),
  KEY `idx_ng_cluster_role` (`cluster_id`, `role`),
  CONSTRAINT `fk_k3s_nodegroups_cluster_id` FOREIGN KEY (`cluster_id`) REFERENCES `k3s_clusters` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `k3s_nodegroup_vms` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `nodegroup_id` CHAR(36) NOT NULL,
  `cluster_id` CHAR(36) NOT NULL,
  `vm_id` VARCHAR(64) NOT NULL,
  `name` VARCHAR(255) NULL,
  `status` VARCHAR(20) NOT NULL DEFAULT 'CREATING',
  `created_at` DATETIME(6) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_k3s_nodegroup_vms_nodegroup_id` (`nodegroup_id`),
  KEY `idx_k3s_nodegroup_vms_cluster_id` (`cluster_id`),
  KEY `idx_k3s_nodegroup_vms_vm_id` (`vm_id`),
  CONSTRAINT `fk_k3s_nodegroup_vms_nodegroup_id` FOREIGN KEY (`nodegroup_id`) REFERENCES `k3s_nodegroups` (`id`) ON DELETE CASCADE,
  CONSTRAINT `fk_k3s_nodegroup_vms_cluster_id` FOREIGN KEY (`cluster_id`) REFERENCES `k3s_clusters` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `k3s_cluster_templates` (
  `id` CHAR(36) NOT NULL,
  `name` VARCHAR(63) NOT NULL,
  `description` TEXT NULL,
  `k3s_version` VARCHAR(32) NULL,
  `default_node_count` INT NOT NULL DEFAULT 1,
  `default_agent_flavor_id` VARCHAR(64) NULL,
  `default_image_id` VARCHAR(64) NULL,
  `plugins_enabled` JSON NULL,
  `os_type` VARCHAR(10) NOT NULL DEFAULT 'ubuntu',
  `public_visible` TINYINT(1) NOT NULL DEFAULT 1,
  `created_by` VARCHAR(64) NULL,
  `created_at` DATETIME(6) NOT NULL,
  `updated_at` DATETIME(6) NOT NULL,
  `deleted_at` DATETIME(6) NULL,
  PRIMARY KEY (`id`),
  KEY `idx_k3s_cluster_templates_name` (`name`),
  KEY `idx_k3s_cluster_templates_created_by` (`created_by`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `project_manager_credentials` (
  `project_id` VARCHAR(64) NOT NULL,
  `user_id` VARCHAR(64) NOT NULL,
  `username` VARCHAR(255) NOT NULL,
  `encrypted_password` TEXT NOT NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`project_id`),
  KEY `ix_project_manager_credentials_user_id` (`user_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `drover_jobs` (
  `id` CHAR(36) NOT NULL,
  `cluster_id` CHAR(36) NOT NULL,
  `project_id` VARCHAR(64) NOT NULL,
  `kind` VARCHAR(32) NOT NULL,
  `status` VARCHAR(16) NOT NULL DEFAULT 'queued',
  `payload_json` JSON NULL,
  `attempts` INT NOT NULL DEFAULT 0,
  `last_error` TEXT NULL,
  `user_id` VARCHAR(64) NULL,
  `username` VARCHAR(255) NULL,
  `claimed_at` DATETIME(6) NULL,
  `created_at` DATETIME(6) NOT NULL,
  `updated_at` DATETIME(6) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_drover_jobs_cluster_id` (`cluster_id`),
  KEY `idx_drover_jobs_project_id` (`project_id`),
  KEY `idx_drover_jobs_claim` (`status`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `resource_policies` (
  `policy_key` VARCHAR(128) NOT NULL,
  `resource_kind` VARCHAR(64) NOT NULL,
  `resource_id` VARCHAR(255) NULL,
  `resource_name` VARCHAR(255) NULL,
  `constraints` JSON NULL,
  `updated_by_user_id` VARCHAR(64) NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`policy_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `runtime_settings` (
  `setting_key` VARCHAR(128) NOT NULL,
  `value_json` JSON NOT NULL,
  `updated_by_user_id` VARCHAR(64) NULL,
  `created_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
  `updated_at` DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
  PRIMARY KEY (`setting_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `gpu_quotas` (
  `id` INT AUTO_INCREMENT NOT NULL,
  `project_id` VARCHAR(64) NOT NULL,
  `gpu_type` VARCHAR(64) NOT NULL,
  `limit` INT NOT NULL DEFAULT -1,
  `created_at` DATETIME(6) NOT NULL,
  `updated_at` DATETIME(6) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_gpu_quota_project_id` (`project_id`),
  UNIQUE KEY `idx_gpu_quota_project_type` (`project_id`, `gpu_type`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
