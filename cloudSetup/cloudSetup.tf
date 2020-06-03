provider "aws" {
  region = "eu-central-1"
  profile = "jakob"
}

resource "aws_dynamodb_table" "open_library_test" {
  name = "open_library_test"
  hash_key = "key"
  billing_mode = "PROVISIONED"
  write_capacity = 2
  read_capacity = 2
  tags = {
    "exa:owner": "jakob.braun@exasol.com",
    "exa:deputy": "Sebastian.Baer@exasol.com"
    "exa:project": "DDBVS"
    "exa:project.name": "Virtual Schema for DynamoDB"
    "exa:stage": "development"
    "Name": "Dynamodb Test Table for Virtual Schemas for Dynamodb"
  }

  attribute {
    name = "key"
    type = "S"
  }
}