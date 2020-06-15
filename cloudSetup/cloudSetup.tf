# This file use you ssh key stored in ~/.ssh/id_rsa.pub. Please make sure that you generated this file via ssh-keygen -m PEM.
# Keys generated without the -m PEM will lead to login fails.

provider "aws" {
  region = "eu-central-1"
  profile = "default"
}

resource "aws_dynamodb_table" "open_library_test" {
  name = "open_library_test"
  hash_key = "key"
  range_key = "revision"
  billing_mode = "PAY_PER_REQUEST"
  #write_capacity = 5
  #read_capacity = 5
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

  attribute {
    name = "revision"
    type = "N"
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
  vpc_id = "${aws_vpc.dynamodb_test_vpc.id}"
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


resource "aws_default_route_table" "dynamodb_test_routing_table" {
  default_route_table_id = "${aws_vpc.dynamodb_test_vpc.default_route_table_id}"
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = "${aws_internet_gateway.gw.id}"
  }
  tags = {
    "exa:owner": "jakob.braun@exasol.com",
    "exa:deputy": "Sebastian.Baer@exasol.com"
    "exa:project": "DDBVS"
    "exa:project.name": "Virtual Schema for DynamoDB"
    "exa:stage": "development"
    "Name": "Route Table for DynamoDB Virtual Schema performance test"
  }
}

resource "aws_security_group" "exasol_db_security_group" {
  name = "allow_tls"
  description = "Allow TLS inbound traffic"
  vpc_id = "${aws_vpc.dynamodb_test_vpc.id}"

  ingress {
    description = "SSH from VPC"
    from_port = 22
    to_port = 22
    protocol = "tcp"
    cidr_blocks = [
      "0.0.0.0/0"]
  }

  ingress {
    description = "HTTPS from VPC"
    from_port = 443
    to_port = 443
    protocol = "tcp"
    cidr_blocks = [
      "0.0.0.0/0"]
  }

  ingress {
    description = "SQL from VPC"
    from_port = 8563
    to_port = 8563
    protocol = "tcp"
    cidr_blocks = [
      "0.0.0.0/0"]
  }

  ingress {
    description = "BucketFS"
    from_port = 2580
    protocol = "tcp"
    to_port = 2580
    cidr_blocks = [
      "0.0.0.0/0"]
  }

  ingress {
    from_port = 0
    protocol = "-1"
    to_port = 0
    self = true
  }

  egress {
    from_port = 0
    to_port = 0
    protocol = "-1"
    cidr_blocks = [
      "0.0.0.0/0"]
  }

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
  key_name = "test-computer-key"
  public_key = file("~/.ssh/id_rsa.pub")
}

resource "aws_internet_gateway" "gw" {
  vpc_id = "${aws_vpc.dynamodb_test_vpc.id}"

  tags = {
    "exa:owner": "jakob.braun@exasol.com",
    "exa:deputy": "Sebastian.Baer@exasol.com"
    "exa:project": "DDBVS"
    "exa:project.name": "Virtual Schema for DynamoDB"
    "exa:stage": "development"
    "Name": "Gateway for Exasol cluster for DynamoDB Virtual Schema performance test"
  }
}

module "exasol" {
  #source = "exasol/exasol/aws"
  #version = "0.0.3"
  source = "../../terraform-aws-exasol"

  cluster_name = "dynamodb-vs-test-cluster"
  database_name = "exadb"
  ami_image_name = "R6.2.3-BYOL"
  sys_user_password = "eXaSol1337DB"
  admin_user_password = "eXaSol1337OP"
  management_server_instance_type = "m5.xlarge"
  datanode_instance_type = "m5.2xlarge"
  datanode_count = "2"
  standbynode_count = "0"
  public_ip = true

  # These values can be obtained from other modules.
  key_pair_name = aws_key_pair.test_pc_key_pair.key_name
  subnet_id = aws_subnet.dynamodb_test_subnet.id
  security_group_id = aws_security_group.exasol_db_security_group.id

  # Variables used in tags.
  project = "DDBVS"
  project_name = "Virtual Schema for DynamoDB"
  owner = "jakob.braun@exasol.com"
  environment = "dev"
  license = "./exasolution.lic"
}

output "exasol_ip" {
  value = module.exasol.management_server_ip
}
