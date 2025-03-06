output "s3_bucket_name" {
  value = aws_s3_bucket.dlq_storage.id
}

output "rds_endpoint" {
  value = aws_rds_cluster.airflow_db.endpoint
}

output "ecs_cluster_name" {
  value = aws_ecs_cluster.data_pipeline.name
}

output "elasticsearch_endpoint" {
  value = aws_elasticsearch_domain.logging.endpoint
}

output "rds_security_group_id" {
  value = aws_security_group.rds_sg.id
}