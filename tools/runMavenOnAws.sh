#!/bin/bash
# run this file in the tools directory
TEST_RUNNER_IP=$(terraform output -state=../cloudSetup/terraform.tfstate test_runner_ip)
rsync -avzhP --delete ../ ubuntu@"$TEST_RUNNER_IP"://home/ubuntu/vs/ --exclude target/
ssh ubuntu@"$TEST_RUNNER_IP" -R 2000:127.0.0.1:22 "cd vs && mvn $@"
