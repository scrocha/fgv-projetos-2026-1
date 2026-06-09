variable "allowed_cidr" {
  description = "CIDR allowed to access the public MySQL instance from the local machine."
  type        = string
  default     = ""
}

variable "aws_region" {
  description = "AWS region used by the lab resources."
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Optional pre-defined S3 bucket name. Leave empty to auto-generate."
  type        = string
  default     = ""
}

variable "create_eventbridge_role" {
  description = "Whether Terraform should create a dedicated EventBridge role allowed to start the Glue job."
  type        = bool
  default     = false
}

variable "create_glue_role" {
  description = "Whether Terraform should create a dedicated Glue IAM role."
  type        = bool
  default     = false
}

variable "db_engine_version" {
  description = "Optional MySQL engine version. Leave null to let AWS choose a supported default."
  type        = string
  default     = null
  nullable    = true
}

variable "db_identifier" {
  description = "RDS instance identifier."
  type        = string
  default     = "classicmodels-task2-db"
}

variable "db_name" {
  description = "Source database name."
  type        = string
  default     = "classicmodels"
}

variable "db_password" {
  description = "RDS admin password."
  type        = string
  sensitive   = true
}

variable "db_port" {
  description = "MySQL port."
  type        = number
  default     = 3306
}

variable "db_username" {
  description = "RDS admin username."
  type        = string
  default     = "admin"
}

variable "eventbridge_schedule_expression" {
  description = "EventBridge schedule for the incremental ETL."
  type        = string
  default     = "cron(0 12 ? * MON *)"
}

variable "enable_eventbridge_target" {
  description = "Whether to create a CloudWatch EventBridge target directly pointing to Glue. Disabled by default because Glue Job ARNs are not accepted by PutTargets in the student lab."
  type        = bool
  default     = false
}

variable "existing_eventbridge_role_arn" {
  description = "Existing IAM role ARN for EventBridge to invoke Glue when create_eventbridge_role is false."
  type        = string
  default     = ""
}

variable "existing_glue_role_arn" {
  description = "Existing IAM role ARN to be used by Glue. Leave empty to create a new role."
  type        = string
  default     = ""
}

variable "existing_glue_role_name" {
  description = "Existing IAM role name to be used by Glue when no ARN is provided."
  type        = string
  default     = "LabRole"
}

variable "glue_database_name" {
  description = "Glue Catalog database used by Athena."
  type        = string
  default     = "classicmodels_analytics"
}

variable "glue_connection_name" {
  description = "Glue connection name."
  type        = string
  default     = "classicmodels-rds-connection"
}

variable "glue_job_name" {
  description = "Glue job name."
  type        = string
  default     = "classicmodels-etl-job"
}

variable "glue_script_key" {
  description = "S3 object key used for the Glue ETL script."
  type        = string
  default     = "glue/etl_job.py"
}

variable "project_prefix" {
  description = "Prefix used to name the resources."
  type        = string
  default     = "g2-sillas-task2"
}
