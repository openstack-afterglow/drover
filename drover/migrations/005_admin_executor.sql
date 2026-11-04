-- Administrator requester is distinct from the real executor trustor/credential owner.
-- NULL preserves the ordinary caller-owned authority model for existing rows.
ALTER TABLE `drover_delegations` ADD COLUMN IF NOT EXISTS `requester_user_id` VARCHAR(64) NULL;
ALTER TABLE `drover_cluster_credentials` ADD COLUMN IF NOT EXISTS `requester_user_id` VARCHAR(64) NULL;

-- The cluster record need not exist when issuance starts. No FK and no secret.
CREATE TABLE IF NOT EXISTS `drover_credential_cleanup` (
  `id` CHAR(36) NOT NULL,
  `cluster_id` CHAR(36) NOT NULL,
  `project_id` VARCHAR(64) NOT NULL,
  `owner_user_id` VARCHAR(64) NOT NULL,
  `requester_user_id` VARCHAR(64) NULL,
  `app_credential_id` VARCHAR(64) NULL,
  `credential_name` VARCHAR(255) NOT NULL,
  `generation` INT NOT NULL,
  `purpose` VARCHAR(16) NOT NULL,
  `state` VARCHAR(16) NOT NULL DEFAULT 'issuing',
  `created_at` DATETIME(6) NOT NULL,
  `updated_at` DATETIME(6) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_drover_cleanup_owner_name` (`owner_user_id`, `project_id`, `credential_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
