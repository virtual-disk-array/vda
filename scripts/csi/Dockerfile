FROM ubuntu:20.04
COPY _out/linux_amd64/vda_csi /
RUN DEBIAN_FRONTEND=noninteractive apt-get update && apt-get install -y -q nvme-cli open-iscsi e2fsprogs xfsprogs
ENTRYPOINT ["/vda_csi"]