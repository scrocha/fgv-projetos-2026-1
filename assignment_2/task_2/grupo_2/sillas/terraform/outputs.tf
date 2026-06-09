output "analytics_bucket_name" {
  description = "S3 bucket used for script storage and parquet outputs."
  value       = aws_s3_bucket.analytics.bucket
}

output "eventbridge_role_arn" {
  description = "IAM role used by EventBridge to start Glue."
  value       = local.eventbridge_role_arn
}

output "eventbridge_rule_name" {
  description = "EventBridge schedule rule for the incremental ETL."
  value       = aws_cloudwatch_event_rule.weekly_incremental_etl.name
}

output "glue_connection_name" {
  description = "AWS Glue connection name."
  value       = aws_glue_connection.classicmodels.name
}

output "glue_database_name" {
  description = "Glue Catalog database used by Athena."
  value       = aws_glue_catalog_database.analytics.name
}

output "glue_job_name" {
  description = "AWS Glue job name."
  value       = aws_glue_job.classicmodels.name
}

output "glue_schedule_trigger_name" {
  description = "AWS Glue scheduled trigger used in the student lab fallback."
  value       = aws_glue_trigger.weekly_incremental_etl.name
}

output "glue_security_group_id" {
  description = "Security group attached to Glue networking."
  value       = aws_security_group.glue.id
}

output "rds_endpoint" {
  description = "RDS endpoint used by the source database."
  value       = aws_db_instance.classicmodels.address
}

output "rds_security_group_id" {
  description = "Security group attached to the RDS instance."
  value       = aws_security_group.rds.id
}
