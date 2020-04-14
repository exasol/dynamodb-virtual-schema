#!/bin/bash
# setup-ssh.sh: load the SSH key 

set -ev
SSH_FILE="$(mktemp -u "$HOME"/.ssh/travis_temp_ssh_key_XXXX)"
declare -r "$SSH_FILE"
# Decrypt the file containing the private key (put the real name of the variables)
# shellcheck disable=SC2154 # variables are set by travis
openssl aes-256-cbc \
  -K "$encrypted_63a949b43279_key" \
  -iv "$encrypted_63a949b43279_iv" \
  -in ".ci/travis_deploy_key.enc" \
  -out "$SSH_FILE" -d
chmod 600 "$SSH_FILE"
# Enable SSH authentication
printf "%s\n" \
       "Host github.com" \
       "  IdentityFile $SSH_FILE" \
       "  LogLevel ERROR" >> ~/.ssh/config
