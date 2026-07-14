#include <tunables/global>

profile library-prep-chemistry flags=(attach_disconnected,mediate_deleted) {
  #include <abstractions/base>
  network deny,
  deny capability,
  mount deny,
  umount deny,
  pivot_root deny,
  ptrace deny,
  signal (receive) peer=unconfined,
  / r,
  /** r,
  /work/output/** rwk,
  /work/scratch/** rwk,
  /tmp/** rwk,
  /dev/nvidia* rw,
  /proc/** r,
  /sys/devices/** r,
}
