locals {
  bucket_name  = var.bucket_name != "" ? var.bucket_name : "${var.project_prefix}-${data.aws_caller_identity.current.account_id}"
  allowed_cidr = var.allowed_cidr != "" ? var.allowed_cidr : "${trimspace(data.http.public_ip.response_body)}/32"
  glue_role_arn = var.create_glue_role ? aws_iam_role.glue[0].arn : (
    var.existing_glue_role_arn != "" ? var.existing_glue_role_arn : data.aws_iam_role.existing_glue[0].arn
  )
  eventbridge_role_arn = var.create_eventbridge_role ? aws_iam_role.eventbridge_glue[0].arn : (
    var.existing_eventbridge_role_arn != "" ? var.existing_eventbridge_role_arn : local.glue_role_arn
  )
}

data "aws_caller_identity" "current" {}

data "http" "public_ip" {
  url = "https://checkip.amazonaws.com/"
}

data "aws_iam_role" "existing_glue" {
  count = var.create_glue_role ? 0 : 1
  name  = var.existing_glue_role_name
}

data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

data "aws_route_tables" "default" {
  vpc_id = data.aws_vpc.default.id
}

data "aws_subnet" "selected" {
  id = data.aws_subnets.default.ids[0]
}

resource "aws_db_subnet_group" "classicmodels" {
  name       = "${var.project_prefix}-db-subnet-group"
  subnet_ids = data.aws_subnets.default.ids

  tags = {
    Name = "${var.project_prefix}-db-subnet-group"
  }
}

resource "aws_vpc_endpoint" "s3" {
  vpc_id          = data.aws_vpc.default.id
  service_name    = "com.amazonaws.${var.aws_region}.s3"
  route_table_ids = data.aws_route_tables.default.ids

  tags = {
    Name = "${var.project_prefix}-s3-endpoint"
  }
}

resource "aws_security_group" "rds" {
  name        = "${var.project_prefix}-rds-sg"
  description = "RDS access for local bootstrap and Glue"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_prefix}-rds-sg"
  }
}

resource "aws_security_group_rule" "rds_local_ingress" {
  type              = "ingress"
  from_port         = var.db_port
  to_port           = var.db_port
  protocol          = "tcp"
  security_group_id = aws_security_group.rds.id
  cidr_blocks       = [local.allowed_cidr]
  description       = "Local MySQL access for lab bootstrap"
}

resource "aws_security_group" "glue" {
  name        = "${var.project_prefix}-glue-sg"
  description = "AWS Glue job networking"
  vpc_id      = data.aws_vpc.default.id

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project_prefix}-glue-sg"
  }
}

resource "aws_security_group_rule" "glue_self_ingress" {
  type                     = "ingress"
  from_port                = 0
  to_port                  = 65535
  protocol                 = "tcp"
  security_group_id        = aws_security_group.glue.id
  source_security_group_id = aws_security_group.glue.id
  description              = "Glue self-referencing networking"
}

resource "aws_security_group_rule" "rds_from_glue" {
  type                     = "ingress"
  from_port                = var.db_port
  to_port                  = var.db_port
  protocol                 = "tcp"
  security_group_id        = aws_security_group.rds.id
  source_security_group_id = aws_security_group.glue.id
  description              = "Glue access to MySQL"
}

resource "aws_db_instance" "classicmodels" {
  identifier             = var.db_identifier
  allocated_storage      = 20
  max_allocated_storage  = 100
  db_name                = var.db_name
  engine                 = "mysql"
  engine_version         = var.db_engine_version
  instance_class         = "db.t3.micro"
  username               = var.db_username
  password               = var.db_password
  port                   = var.db_port
  publicly_accessible    = true
  skip_final_snapshot    = true
  deletion_protection    = false
  storage_encrypted      = false
  db_subnet_group_name   = aws_db_subnet_group.classicmodels.name
  vpc_security_group_ids = [aws_security_group.rds.id]

  tags = {
    Name = "${var.project_prefix}-mysql"
  }
}

resource "aws_s3_bucket" "analytics" {
  bucket        = local.bucket_name
  force_destroy = true

  tags = {
    Name = "${var.project_prefix}-analytics"
  }
}

resource "aws_s3_bucket_versioning" "analytics" {
  bucket = aws_s3_bucket.analytics.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "analytics" {
  bucket = aws_s3_bucket.analytics.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_object" "glue_script" {
  bucket       = aws_s3_bucket.analytics.id
  key          = var.glue_script_key
  source       = "${path.module}/../glue/etl_job.py"
  etag         = filemd5("${path.module}/../glue/etl_job.py")
  content_type = "text/x-python"
}

resource "aws_iam_role" "glue" {
  count = var.create_glue_role ? 1 : 0
  name  = "${var.project_prefix}-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  count      = var.create_glue_role ? 1 : 0
  role       = aws_iam_role.glue[0].name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  count = var.create_glue_role ? 1 : 0
  name  = "${var.project_prefix}-glue-s3-access"
  role  = aws_iam_role.glue[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.analytics.arn,
          "${aws_s3_bucket.analytics.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_glue_connection" "classicmodels" {
  name = var.glue_connection_name

  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:mysql://${aws_db_instance.classicmodels.address}:${var.db_port}/${var.db_name}"
    USERNAME            = var.db_username
    PASSWORD            = var.db_password
  }

  physical_connection_requirements {
    availability_zone      = data.aws_subnet.selected.availability_zone
    security_group_id_list = [aws_security_group.glue.id]
    subnet_id              = data.aws_subnet.selected.id
  }

  depends_on = [aws_vpc_endpoint.s3]
}

resource "aws_glue_job" "classicmodels" {
  name     = var.glue_job_name
  role_arn = local.glue_role_arn

  glue_version      = "4.0"
  max_retries       = 0
  timeout           = 30
  number_of_workers = 2
  worker_type       = "G.1X"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.analytics.bucket}/${aws_s3_object.glue_script.key}"
    python_version  = "3"
  }

  connections = [aws_glue_connection.classicmodels.name]

  default_arguments = {
    "--job-language"                     = "python"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-glue-datacatalog"          = "true"
    "--TempDir"                          = "s3://${aws_s3_bucket.analytics.bucket}/tmp/"
    "--db_host"                          = aws_db_instance.classicmodels.address
    "--db_port"                          = tostring(var.db_port)
    "--db_name"                          = var.db_name
    "--db_user"                          = var.db_username
    "--db_password"                      = var.db_password
    "--output_bucket"                    = aws_s3_bucket.analytics.bucket
    "--output_prefix"                    = "analytics"
  }

  depends_on = [
    aws_s3_object.glue_script,
    aws_iam_role_policy_attachment.glue_service,
    aws_iam_role_policy.glue_s3_access
  ]
}

resource "aws_glue_catalog_database" "analytics" {
  name        = var.glue_database_name
  description = "ClassicModels star schema analytics database"
}

resource "aws_glue_catalog_table" "fact_orders" {
  name          = "fact_orders"
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                       = "TRUE"
    "classification"               = "parquet"
    "projection.enabled"           = "true"
    "projection.order_year.type"   = "integer"
    "projection.order_year.range"  = "2000,2100"
    "projection.order_month.type"  = "integer"
    "projection.order_month.range" = "1,12"
    "storage.location.template"    = "s3://${aws_s3_bucket.analytics.bucket}/analytics/fact_orders/order_year=$${order_year}/order_month=$${order_month}/"
  }

  partition_keys {
    name = "order_year"
    type = "int"
  }

  partition_keys {
    name = "order_month"
    type = "int"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.analytics.bucket}/analytics/fact_orders/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "order_id"
      type = "int"
    }
    columns {
      name = "customer_id"
      type = "int"
    }
    columns {
      name = "product_id"
      type = "string"
    }
    columns {
      name = "order_date_key"
      type = "int"
    }
    columns {
      name = "country_key"
      type = "string"
    }
    columns {
      name = "quantity_ordered"
      type = "int"
    }
    columns {
      name = "price_each"
      type = "double"
    }
    columns {
      name = "sales_amount"
      type = "double"
    }
  }
}

resource "aws_glue_catalog_table" "dim_customers" {
  name          = "dim_customers"
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL         = "TRUE"
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.analytics.bucket}/analytics/dim_customers/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "customer_id"
      type = "int"
    }
    columns {
      name = "customer_name"
      type = "string"
    }
    columns {
      name = "contact_name"
      type = "string"
    }
    columns {
      name = "city"
      type = "string"
    }
    columns {
      name = "country"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "dim_products" {
  name          = "dim_products"
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL         = "TRUE"
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.analytics.bucket}/analytics/dim_products/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "product_id"
      type = "string"
    }
    columns {
      name = "product_name"
      type = "string"
    }
    columns {
      name = "product_line"
      type = "string"
    }
    columns {
      name = "product_vendor"
      type = "string"
    }
  }
}

resource "aws_glue_catalog_table" "dim_dates" {
  name          = "dim_dates"
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL         = "TRUE"
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.analytics.bucket}/analytics/dim_dates/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "date_key"
      type = "int"
    }
    columns {
      name = "full_date"
      type = "date"
    }
    columns {
      name = "year"
      type = "int"
    }
    columns {
      name = "quarter"
      type = "int"
    }
    columns {
      name = "month"
      type = "int"
    }
    columns {
      name = "day"
      type = "int"
    }
  }
}

resource "aws_glue_catalog_table" "dim_countries" {
  name          = "dim_countries"
  database_name = aws_glue_catalog_database.analytics.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL         = "TRUE"
    "classification" = "parquet"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.analytics.bucket}/analytics/dim_countries/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "country_key"
      type = "string"
    }
    columns {
      name = "country"
      type = "string"
    }
    columns {
      name = "territory"
      type = "string"
    }
  }
}

resource "aws_iam_role" "eventbridge_glue" {
  count = var.create_eventbridge_role ? 1 : 0
  name  = "${var.project_prefix}-eventbridge-glue-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "events.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })
}

resource "aws_iam_role_policy" "eventbridge_start_glue" {
  count = var.create_eventbridge_role ? 1 : 0
  name  = "${var.project_prefix}-start-glue"
  role  = aws_iam_role.eventbridge_glue[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = "glue:StartJobRun"
        Resource = aws_glue_job.classicmodels.arn
      }
    ]
  })
}

resource "aws_cloudwatch_event_rule" "weekly_incremental_etl" {
  name                = "${var.project_prefix}-weekly-incremental-etl"
  description         = "Weekly trigger for ClassicModels incremental Glue ETL"
  schedule_expression = var.eventbridge_schedule_expression
}

resource "aws_cloudwatch_event_target" "glue_job" {
  count = var.enable_eventbridge_target ? 1 : 0

  rule     = aws_cloudwatch_event_rule.weekly_incremental_etl.name
  arn      = aws_glue_job.classicmodels.arn
  role_arn = local.eventbridge_role_arn

  depends_on = [
    aws_iam_role_policy.eventbridge_start_glue
  ]

  lifecycle {
    precondition {
      condition     = local.eventbridge_role_arn != ""
      error_message = "EventBridge role ARN could not be resolved. Set existing_eventbridge_role_arn or existing_glue_role_name."
    }
  }
}

resource "aws_glue_trigger" "weekly_incremental_etl" {
  name     = "${var.project_prefix}-weekly-incremental-etl"
  type     = "SCHEDULED"
  schedule = var.eventbridge_schedule_expression
  enabled  = true

  actions {
    job_name = aws_glue_job.classicmodels.name
  }
}
