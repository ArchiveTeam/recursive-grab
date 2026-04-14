#!/bin/bash

if ! python3 -c "import dns.resolver" 2>/dev/null
then
  echo "Installing Python3 dnspython..."
  sudo python3 -m pip install --no-cache-dir dnspython || exit 1
fi

exit 0

