#!/bin/bash
sudo mkfs -F -t ext4 /dev/nvme1n1
sudo mkdir /data
sudo -s sh -c '/usr/bin/echo "/dev/nvme1n1  /data  auto  auto,nouser,exec,rw,async,atime  0 0" >> /etc/fstab'
sudo mount /data
sudo chown ubuntu /data
