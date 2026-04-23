#!/bin/bash

if ! python3 -c "import dns.resolver" 2>/dev/null
then
  echo "Installing Python3 dnspython..."
  sudo python3 -m pip install --no-cache-dir dnspython || exit 1
fi

if ! lua -e 'require("org.conman.dns")' 2>/dev/null
then
  if ! dpkg-query -Wf'${Status}' make 2>/dev/null | grep -q '^i'
  then
    echo "Installing make..."
    sudo apt-get update
    sudo apt-get install -y --no-install-recommends make || exit 1
    sudo rm -rf /var/lib/apt/lists/*
  fi
  echo "Installing Lua org.conman.dns..."
  sudo luarocks install org.conman.dns || exit 1
fi

exit 0

