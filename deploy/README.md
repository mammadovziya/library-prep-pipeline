# Deployment layouts

`deploy/mscoc6` and the `mscoc6` Ansible role are the current deployment.
They implement one website host and one queued worker identity on the same
approved machine.

`deploy/control`, `deploy/worker`, `deploy/storage`, and the older control,
worker, storage, and common Ansible roles are retained only as historical
reference for the superseded seven-host design. Do not apply their Compose
projects or systemd units to `mscoc6`.
