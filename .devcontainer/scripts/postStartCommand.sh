#!/bin/bash

set -e

# Sourcing of fzf
echo "source <(fzf --bash)" >> /home/$USER/.bashrc

# Activate Completion of Task
sudo sh -c 'task --completion bash > /etc/bash_completion.d/task'

task infra:start-and-configure