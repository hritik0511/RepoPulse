variable "aws_region" {
  type    = string
  default = "ap-south-1"
}

variable "raw_bucket_name" {
  type        = string
  description = "Globally unique S3 bucket name for the raw GH Archive copy"
}

variable "databricks_host" {
  type        = string
  description = "e.g. https://dbc-xxxx.cloud.databricks.com"
}

variable "databricks_token" {
  type        = string
  sensitive   = true
  description = "Databricks personal access token (stored in SSM SecureString)"
}

variable "volume_dir" {
  type        = string
  default     = "/Volumes/workspace/gh_lakehouse/raw/landing"
  description = "UC volume folder Auto Loader watches"
}
