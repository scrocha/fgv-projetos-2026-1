variable "aws_region" {
  description = "AWS region used by task 3."
  type        = string
  default     = "us-east-1"
}

variable "glue_database_name" {
  description = "Glue catalog database name for analytics."
  type        = string
  default     = "classicmodels_analytics"
}

variable "athena_workgroup_name" {
  description = "Athena workgroup name for task 3."
  type        = string
  default     = "classicmodels-analytics"
}

variable "data_lake_bucket" {
  description = "Bucket containing the Parquet outputs from task 2."
  type        = string
}

variable "athena_results_prefix" {
  description = "S3 prefix used by Athena query results."
  type        = string
  default     = "athena-results/"
}
