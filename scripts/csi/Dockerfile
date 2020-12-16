FROM ubuntu:20.04
COPY vdacsi /
RUN apt-get update
RUN apt-get install -y nvme-cli open-iscsi e2fsprogs xfsprogs
ENTRYPOINT ["/vdacsi"]