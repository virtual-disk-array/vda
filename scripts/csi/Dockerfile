FROM ubuntu:20.04
COPY _out/linux_amd64/vda_csi /
RUN apt-get update
RUN apt-get install -y nvme-cli open-iscsi e2fsprogs xfsprogs
ENTRYPOINT ["/vda_csi"]