variable "environment" {
  description = "Deployment environment (dev/stage/prod)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID for network resources"
  type        = string
}

variable "subnet_ids" {
  description = "List of subnet IDs"
  type        = list(string)
}

variable "db_name" {
  description = "PostgreSQL database name"
  type        = string
  default     = "airflow"
}

variable "db_username" {
  description = "PostgreSQL master username"
  type        = string
  default     = "airflow"
}

variable "db_password" {
  description = "PostgreSQL master password"
  type        = string
  sensitive   = true
}

variable "region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}