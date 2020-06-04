provider "aws" {
  region = "eu-central-1"
  profile = "default"
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

resource "aws_s3_bucket" "s3-bucket" {
  bucket = "dynamodb-virtual-schema-test-bucket"
  acl    = "private"

  tags = {
    "exa:owner": "jakob.braun@exasol.com",
    "exa:deputy": "Sebastian.Baer@exasol.com"
    "exa:project": "DDBVS"
    "exa:project.name": "Virtual Schema for DynamoDB"
    "exa:stage": "development"
    "Name": "S3 bucket for storing test data for testing Dynamodb Virtual Schema"
  }
}


module "exasol" {
  #source = "exasol/exasol/aws"
  #version = "0.0.3"
  source = "../../terraform-aws-exasol"

  cluster_name = "dynamodb-vs-test-cluster"
  database_name = "exadb"
  ami_image_name = "Exasol-R6.2.6-BYOL"
  sys_user_password = "eXaSol1337DB"
  admin_user_password = "eXaSol1337OP"
  management_server_instance_type = "m5.xlarge"
  datanode_instance_type = "m5.2xlarge"
  datanode_count = "2"
  standbynode_count = "0"
  public_ip = true

  # These values can be obtained from other modules.
  key_pair_name = "exasol-key-pair"
  subnet_id = "subnet-ed85b690"
  security_group_id = "sg-07599522f13906845"

  # Variables used in tags.
  project = "DDBVS"
  project_name = "Virtual Schema for DynamoDB"
  owner = "jakob.braun@exasol.com"
  environment = "dev"
  license = "./exasolution.lic"
}