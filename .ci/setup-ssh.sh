 #!/bin/bash
# setup-ssh.sh: load the SSH key 

set -ev
eval "$(ssh-agent -s)"
declare -r SSH_FILE="$(mktemp -u $HOME/.ssh/travis_temp_ssh_key_XXXX)"
# Decrypt the file containing the private key (put the real name of the variables)
openssl aes-256-cbc \
  -K $encrypted_63a949b43279_key \
  -iv $encrypted_63a949b43279_iv \
  -in ".ci/travis_deploy_key.enc" \
  -out "$SSH_FILE" -d
echo $SSH_FILE
cat $SSH_FILE
# Enable SSH authentication
chmod 600 "$SSH_FILE" \
  && printf "%s\n" \
       "Host github.com" \
       "  IdentityFile $SSH_FILE" \
       "  LogLevel ERROR" >> ~/.ssh/config
ssh-add "$SSH_FILE"
