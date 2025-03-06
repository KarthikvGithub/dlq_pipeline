terraform {
  backend "s3" {
    bucket         = "nyc-taxi-dlq-tfstate"
    key            = "global/s3/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "tf-lock-table"
    encrypt        = true
  }
}