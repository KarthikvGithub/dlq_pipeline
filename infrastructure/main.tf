terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

resource "aws_s3_bucket" "dlq_storage" {
  bucket = "nyc-taxi-dlq-${var.environment}"
  tags = {
    Environment = var.environment
  }
}

resource "aws_rds_cluster" "airflow_db" {
  cluster_identifier  = "airflow-postgres-${var.environment}"
  engine              = "aurora-postgresql"
  database_name       = var.db_name
  master_username     = var.db_username
  master_password     = var.db_password
  skip_final_snapshot = true
  vpc_security_group_ids = [aws_security_group.rds_sg.id]
  db_subnet_group_name = aws_db_subnet_group.airflow.name
}

resource "aws_ecs_cluster" "data_pipeline" {
  name = "data-pipeline-cluster-${var.environment}"
}

resource "aws_security_group" "rds_sg" {
  name        = "rds-sg-${var.environment}"
  description = "Allow ECS to RDS traffic"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_db_subnet_group" "airflow" {
  name       = "airflow-subnet-group-${var.environment}"
  subnet_ids = var.subnet_ids
}

resource "aws_elasticsearch_domain" "logging" {
  domain_name           = "dlq-logs-${var.environment}"
  elasticsearch_version = "7.10"

  cluster_config {
    instance_type = "t3.small.elasticsearch"
  }

  vpc_options {
    subnet_ids = slice(var.subnet_ids, 0, 1)
    security_group_ids = [aws_security_group.elasticsearch_sg.id]
  }

  access_policies = <<POLICY
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "AWS": "*"
            },
            "Action": "es:*",
            "Resource": "arn:aws:es:${var.region}:*:domain/dlq-logs-${var.environment}/*"
        }
    ]
}
POLICY
}

resource "aws_security_group" "elasticsearch_sg" {
  name        = "elasticsearch-sg-${var.environment}"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
}