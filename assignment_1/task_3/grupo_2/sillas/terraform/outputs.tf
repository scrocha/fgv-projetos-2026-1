output "athena_workgroup_name" {
  description = "Athena workgroup name used by task 3."
  value       = aws_athena_workgroup.analytics.name
}

output "athena_output_location" {
  description = "S3 path used for Athena query results."
  value       = aws_athena_workgroup.analytics.configuration[0].result_configuration[0].output_location
}

output "data_lake_bucket" {
  description = "Bucket reused from task 2."
  value       = var.data_lake_bucket
}

output "glue_database_name" {
  description = "Glue database name used by task 3."
  value       = var.glue_database_name
}
