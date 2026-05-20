provider "aws" {
  region = var.aws_region
}

locals {
  athena_output_location = "s3://${var.data_lake_bucket}/${trim(var.athena_results_prefix, "/")}/"
}

resource "aws_athena_workgroup" "analytics" {
  name = var.athena_workgroup_name

  configuration {
    enforce_workgroup_configuration    = false
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = local.athena_output_location
    }
  }
}
