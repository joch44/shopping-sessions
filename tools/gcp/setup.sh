#!/usr/bin/env bash
# One-time setup of a timing VM (e.g. the GCP n2 Cascade Lake box), run ON the VM:
#   bash setup.sh
# Same harness as the Azure mirror: nolimit-run (scorer-identical docker flags), output and
# scratch on their OWN loop-mounted ext4 (root-fs output costs ~0.45 s per 20M run), both
# benchmark locks, KEEP, warm-cache bench.sh / ab.sh. Docker must already be installed.
set -euo pipefail
sudo mkdir -p /srv/nolimit/datasets /srv/nolimit/bin /srv/nolimit/run
sudo chown "$USER": /srv/nolimit/datasets /srv/nolimit/bin
if ! findmnt -n /srv/nolimit/run >/dev/null; then
  [ -f /srv/nolimit/run.img ] || { sudo fallocate -l 50G /srv/nolimit/run.img; sudo mkfs.ext4 -q -F /srv/nolimit/run.img; }
  sudo mount -o loop /srv/nolimit/run.img /srv/nolimit/run
fi
findmnt -T /srv/nolimit/run
sudo install -m 0755 nolimit-run /usr/local/bin/nolimit-run
install -m 0755 bench.sh ab.sh "$HOME/"
sudo touch /srv/nolimit/run.lock && sudo chmod 666 /srv/nolimit/run.lock
touch /tmp/feas-bench.lock
docker version --format 'docker {{.Server.Version}}' 2>/dev/null || sudo docker version --format 'docker {{.Server.Version}}'
cat /sys/kernel/mm/transparent_hugepage/enabled /sys/kernel/mm/transparent_hugepage/defrag
lscpu | grep -E "Model name|^CPU\(s\)"
