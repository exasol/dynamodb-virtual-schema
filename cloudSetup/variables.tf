variable "owner" {}

variable "deputy" {
  default = ""
}

variable "exasol_nodes" {
  default = 2
}

variable "project" {
  default = "DDBVS"
}

variable "project_name" {
  default = "Virtual Schema for DynamoDB"
}

variable "stage" {}