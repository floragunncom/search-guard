#!/bin/bash

echo -n "$PKEY" | base64 --decode > private.key

eval `gpg-agent --daemon --pinentry-program /usr/bin/pinentry-tty --no-grab` > /dev/null 2>&1
export GPG_TTY=`tty`
export GPG_AGENT_INFO
export PINENTRY_USER_DATA="tty"

echo "pinentry-program /usr/bin/pinentry-tty" >> /root/.gnupg/gpg-agent.conf
echo "allow-loopback-pinentry" >> /root/.gnupg/gpg-agent.conf
echo "default-cache-ttl 34560000" >> /root/.gnupg/gpg-agent.conf
echo "max-cache-ttl 34560000" >> /root/.gnupg/gpg-agent.conf
cat /root/.gnupg/gpg-agent.conf

killall gpg-agent || true
gpgconf --kill gpg-agent || true

eval `gpg-agent --daemon --pinentry-program /usr/bin/pinentry-tty --no-grab` > /dev/null 2>&1
export GPG_TTY=`tty`
export GPG_AGENT_INFO
export PINENTRY_USER_DATA="tty"

echo "$PASS" | gpg --pinentry-mode loopback --passphrase-fd 0 --allow-secret-key-import --import private.key > /dev/null 2>&1 || true
expect -c "spawn gpg --edit-key 4A61C8AE trust quit; send \"5\ry\r\"; expect eof" > /dev/null 2>&1 || true

#cache key
echo "123" > cl.txt
echo "$PASS" | gpg --pinentry-mode loopback --passphrase-fd 0 --textmode --always-trust --default-key 4A61C8AE -z 0 -a --clearsign cl.txt > /dev/null 2>&1
 