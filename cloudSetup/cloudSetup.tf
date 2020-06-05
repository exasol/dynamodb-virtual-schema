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

resource "aws_vpc" "dynamodb_test_vpc" {
  cidr_block = "10.0.0.0/16"
  tags = {
    "exa:owner": "jakob.braun@exasol.com",
    "exa:deputy": "Sebastian.Baer@exasol.com"
    "exa:project": "DDBVS"
    "exa:project.name": "Virtual Schema for DynamoDB"
    "exa:stage": "development"
    "Name": "VPC for DynamoDB Virtual Schema performance test"
  }
}

resource "aws_subnet" "dynamodb_test_subnet" {
  vpc_id     = "${aws_vpc.dynamodb_test_vpc.id}"
  cidr_block = "10.0.0.0/24"

  tags = {
    "exa:owner": "jakob.braun@exasol.com",
    "exa:deputy": "Sebastian.Baer@exasol.com"
    "exa:project": "DDBVS"
    "exa:project.name": "Virtual Schema for DynamoDB"
    "exa:stage": "development"
    "Name": "Subnet for DynamoDB Virtual Schema performance test"
  }
}

resource "aws_security_group" "exasol_db_security_group" {
  name        = "allow_tls"
  description = "Allow TLS inbound traffic"
  vpc_id      = "${aws_vpc.dynamodb_test_vpc.id}"

  #ingress {
  #  description = "TLS from VPC"
  #  from_port   = 443
  #  to_port     = 443
  #  protocol    = "tcp"
  #  cidr_blocks = [aws_vpc.dynamodb_test_vpc.cidr_block]
  #}

  #egress {
  #  from_port   = 0
  #  to_port     = 0
  #  protocol    = "-1"
  #  cidr_blocks = ["0.0.0.0/0"]
  #}

  tags = {
    "exa:owner": "jakob.braun@exasol.com",
    "exa:deputy": "Sebastian.Baer@exasol.com"
    "exa:project": "DDBVS"
    "exa:project.name": "Virtual Schema for DynamoDB"
    "exa:stage": "development"
    "Name": "Security Group for Exasol cluster for DynamoDB Virtual Schema performance test"
  }
}

resource "aws_key_pair" "test_pc_key_pair" {
  key_name   = "test-computer-key"
  public_key = file("~/.ssh/id_rsa.pub")
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
  key_pair_name = aws_key_pair.test_pc_key_pair.id
  subnet_id = aws_subnet.dynamodb_test_subnet.id
  security_group_id = aws_security_group.exasol_db_security_group.id

  # Variables used in tags.
  project = "DDBVS"
  project_name = "Virtual Schema for DynamoDB"
  owner = "jakob.braun@exasol.com"
  environment = "dev"
  license = "./exasolution.lic"
}