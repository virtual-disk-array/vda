#!/bin/bash

set -e

work_dir=$VDA_SANDBOX_WORK_DIR
if [ "$work_dir" == "" ]; then
    work_dir=/tmp/vda_sandbox
fi

spdk_dir=${work_dir}/spdk
etcd_version="3.5.0"
vda_version="0.1.0"
vda_dir=${work_dir}/vda


function wait_for_socket() {
    file_path=$1
    max_retry=10
    retry_cnt=0
    while true; do
	if [ -S $file_path ]; then
	    break
	fi
	if [ $retry_cnt -ge $max_retry ]; then
            echo "wait $file_path timeout"
            exit 1
        fi
	sleep 1
	((retry_cnt=retry_cnt+1))
    done
}

function start() {
    mkdir -p $work_dir

    cd $work_dir
    if [ ! -d spdk ]; then
       git clone https://github.com/spdk/spdk
    fi
    cd spdk
    git submodule update --init
    sudo ./scripts/pkgdep.sh
    ./configure
    make
    sudo HUGEMEM=8192 scripts/setup.sh

    cd $work_dir
    if [ ! -f etcd-v${etcd_version}-linux-amd64.tar.gz ]; then
	curl -L -O https://github.com/etcd-io/etcd/releases/download/v${etcd_version}/etcd-v${etcd_version}-linux-amd64.tar.gz
    fi
    if [ ! -d etcd-v${etcd_version}-linux-amd64 ]; then
	tar xvf etcd-v${etcd_version}-linux-amd64.tar.gz
    fi
    cd etcd-v${etcd_version}-linux-amd64
    ./etcd --listen-client-urls http://localhost:2389 \
    	   --advertise-client-urls http://localhost:2389 \
    	   --listen-peer-urls http://localhost:2390 \
    	   --name etcd0 --data-dir $work_dir/etcd0.data \
    	   > $work_dir/etcd0.log 2>&1 &

    cd $work_dir
    if [ ! -f vda_linux_amd64_v${vda_version}.zip ]; then
	curl -L -O https://github.com/virtual-disk-array/vda/releases/download/v${vda_version}/vda_linux_amd64_v${vda_version}.tar.gz
    fi
    if [ ! -d $vda_dir ]; then
	tar xvf vda_linux_amd64_v${vda_version}.tar.gz
	mv vda_linux_amd64_v${vda_version} $vda_dir
    fi

    cd $spdk_dir
    sudo build/bin/spdk_tgt --rpc-socket $work_dir/dn.sock --wait-for-rpc > $work_dir/dn.log 2>&1 &
    wait_for_socket $work_dir/dn.sock
    sudo scripts/rpc.py -s $work_dir/dn.sock bdev_set_options -d
    sudo scripts/rpc.py -s $work_dir/dn.sock nvmf_set_crdt -t1 100 -t2 100 -t3 100
    sudo scripts/rpc.py -s $work_dir/dn.sock framework_start_init
    sudo scripts/rpc.py -s $work_dir/dn.sock framework_wait_init
    sudo chmod 777 $work_dir/dn.sock

    cd $vda_dir
    ./vda_dn_agent --network tcp --address '127.0.0.1:9720' \
    		   --sock-path $work_dir/dn.sock --sock-timeout 10 \
    		   --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4420"}' \
    		   --tr-conf '{"trtype":"TCP"}' \
    		   > $work_dir/dn_agent.log 2>&1 &

    cd $spdk_dir
    sudo build/bin/spdk_tgt --rpc-socket $work_dir/cn.sock --wait-for-rpc > $work_dir/cn.log 2>&1 &
    wait_for_socket $work_dir/cn.sock
    sudo scripts/rpc.py -s $work_dir/cn.sock bdev_set_options -d
    sudo scripts/rpc.py -s $work_dir/cn.sock nvmf_set_crdt -t1 100 -t2 100 -t3 100
    sudo scripts/rpc.py -s $work_dir/cn.sock framework_start_init
    sudo scripts/rpc.py -s $work_dir/cn.sock framework_wait_init
    sudo chmod 777 $work_dir/cn.sock

    cd $vda_dir
    ./vda_cn_agent --network tcp --address '127.0.0.1:9820' \
    		   --sock-path $work_dir/cn.sock --sock-timeout 10 \
    		   --lis-conf '{"trtype":"tcp","traddr":"127.0.0.1","adrfam":"ipv4","trsvcid":"4430"}' \
    		   --tr-conf '{"trtype":"TCP"}' \
    		   > $work_dir/cn_agent.log 2>&1 &

    cd $vda_dir
    ./vda_portal --portal-address '127.0.0.1:9520' --portal-network tcp \
    		 --etcd-endpoints localhost:2389 \
    		 > $work_dir/portal.log 2>&1 &
    ./vda_monitor --etcd-endpoints localhost:2389 \
    		  > $work_dir/monitor.log 2>&1 &

    ./vda_cli dn create --sock-addr localhost:9720 \
    	      --tr-type tcp --tr-addr 127.0.0.1 --adr-fam ipv4 --tr-svc-id 4420
    dd if=/dev/zero of=$work_dir/pd0.img bs=1M count=512
    ./vda_cli pd create --sock-addr localhost:9720 --pd-name pd0 \
	      --bdev-type-key aio --bdev-type-value $work_dir/pd0.img
    ./vda_cli cn create --sock-addr localhost:9820 \
    	      --tr-type tcp --tr-addr 127.0.0.1 --adr-fam ipv4 --tr-svc-id 4430


    cd $work_dir

    cat <<EOF > controller-rbac.yaml
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: vdacsi-controller-sa

# external-provisioner sidecar required roles
---
kind: ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: vdacsi-provisioner-role
rules:
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list"]
- apiGroups: [""]
  resources: ["persistentvolumes"]
  verbs: ["get", "list", "watch", "create", "delete"]
- apiGroups: [""]
  resources: ["persistentvolumeclaims"]
  verbs: ["get", "list", "watch", "update"]
- apiGroups: ["storage.k8s.io"]
  resources: ["storageclasses"]
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
  resources: ["events"]
  verbs: ["list", "watch", "create", "update", "patch"]
- apiGroups: ["snapshot.storage.k8s.io"]
  resources: ["volumesnapshots"]
  verbs: ["get", "list"]
- apiGroups: ["snapshot.storage.k8s.io"]
  resources: ["volumesnapshotcontents"]
  verbs: ["get", "list"]
- apiGroups: ["storage.k8s.io"]
  resources: ["csinodes"]
  verbs: ["get", "list", "watch"]
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get", "list", "watch"]

---
kind: ClusterRoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: vdacsi-provisioner-binding
subjects:
- kind: ServiceAccount
  name: vdacsi-controller-sa
  namespace: default
roleRef:
  kind: ClusterRole
  name: vdacsi-provisioner-role
  apiGroup: rbac.authorization.k8s.io

# external-attacher sidecar required roles
---
kind: ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: vdacsi-attacher-role
rules:
- apiGroups: [""]
  resources: ["persistentvolumes"]
  verbs: ["get", "list", "watch", "update", "patch"]
- apiGroups: ["storage.k8s.io"]
  resources: ["csinodes"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["storage.k8s.io"]
  resources: ["volumeattachments"]
  verbs: ["get", "list", "watch", "update", "patch"]
- apiGroups: [""]
  resources: ["secrets"]
  verbs: ["get", "list"]

---
kind: ClusterRoleBinding
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: vdacsi-attacher-binding
subjects:
- kind: ServiceAccount
  name: vdacsi-controller-sa
  namespace: default
roleRef:
  kind: ClusterRole
  name: vdacsi-attacher-role
  apiGroup: rbac.authorization.k8s.io

EOF

    cat <<EOF > controller.yaml
---
kind: StatefulSet
apiVersion: apps/v1
metadata:
  name: vdacsi-controller
spec:
  serviceName: vdacsi-controller
  replicas: 1
  selector:
    matchLabels:
      app: vdacsi-controller
  template:
    metadata:
      labels:
        app: vdacsi-controller
    spec:
      serviceAccount: vdacsi-controller-sa
      hostNetwork: true
      containers:
      - name: vdacsi-provisioner
        image: quay.io/k8scsi/csi-provisioner:v1.4.0
        imagePullPolicy: "IfNotPresent"
        args:
        - "--v=5"
        - "--csi-address=unix:///csi/csi.sock"
        - "--timeout=30s"
        - "--retry-interval-start=500ms"
        - "--enable-leader-election=false"
        volumeMounts:
        - name: socket-dir
          mountPath: /csi
      - name: vdacsi-attacher
        image: quay.io/k8scsi/csi-attacher:v2.1.1
        imagePullPolicy: "IfNotPresent"
        args:
        - "--v=5"
        - "--csi-address=unix:///csi/csi.sock"
        - "--leader-election=false"
        volumeMounts:
        - name: socket-dir
          mountPath: /csi
      - name: vdacsi-controller
        image: virtualdiskarray/vdacsi:dev
        imagePullPolicy: "IfNotPresent"
        args:
        - "--endpoint=unix:///csi/csi.sock"
        - "--enable-cs"
        - "--node-id=\$(NODE_ID)"
        - "--vda-endpoint=127.0.0.1:9520"
        env:
        - name: NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        volumeMounts:
        - name: socket-dir
          mountPath: /csi
      volumes:
      - name: socket-dir
        emptyDir:
          medium: "Memory"

EOF

    cat <<EOF > node-rbac.yaml
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: vdacsi-node-sa

EOF

    cat <<EOF > node.yaml
---
kind: DaemonSet
apiVersion: apps/v1
metadata:
  name: vdacsi-node
spec:
  selector:
    matchLabels:
      app: vdacsi-node
  template:
    metadata:
      labels:
        app: vdacsi-node
    spec:
      serviceAccount: vdacsi-node-sa
      hostNetwork: true
      containers:
      - name: vdacsi-registrar
        securityContext:
          privileged: true
        image: quay.io/k8scsi/csi-node-driver-registrar:v1.2.0
        imagePullPolicy: "IfNotPresent"
        args:
        - "--v=5"
        - "--csi-address=unix:///csi/csi.sock"
        - "--kubelet-registration-path=/var/lib/kubelet/plugins/csi.vda.io/csi.sock"
        volumeMounts:
        - name: socket-dir
          mountPath: /csi
        - name: registration-dir
          mountPath: /registration
      - name: vdacsi-node
        securityContext:
          privileged: true
          capabilities:
            add: ["SYS_ADMIN"]
          allowPrivilegeEscalation: true
        image: virtualdiskarray/vdacsi:dev
        imagePullPolicy: "IfNotPresent"
        args:
        - "--endpoint=unix:///csi/csi.sock"
        - "--enable-ns"
        - "--node-id=\$(NODE_ID)"
        - "--vda-endpoint=127.0.0.1:9520"
        env:
        - name: NODE_ID
          valueFrom:
            fieldRef:
              fieldPath: spec.nodeName
        lifecycle:
          postStart:
            exec:
              command: ["/bin/sh", "-c",
                        "/usr/sbin/iscsid || echo failed to start iscsid"]
        volumeMounts:
        - name: socket-dir
          mountPath: /csi
        - name: plugin-dir
          mountPath: /var/lib/kubelet/plugins
          mountPropagation: "Bidirectional"
        - name: pod-dir
          mountPath: /var/lib/kubelet/pods
          mountPropagation: "Bidirectional"
        - name: host-dev
          mountPath: /dev
        - name: host-sys
          mountPath: /sys
      volumes:
      - name: socket-dir
        hostPath:
          path: /var/lib/kubelet/plugins/csi.vda.io
          type: DirectoryOrCreate
      - name: registration-dir
        hostPath:
          path: /var/lib/kubelet/plugins_registry/
          type: Directory
      - name: plugin-dir
        hostPath:
          path: /var/lib/kubelet/plugins
          type: Directory
      - name: pod-dir
        hostPath:
          path: /var/lib/kubelet/pods
          type: Directory
      - name: host-dev
        hostPath:
          path: /dev
      - name: host-sys
        hostPath:
          path: /sys

EOF

    cat <<EOF > storageclass.yaml
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: vdacsi-sc
provisioner: csi.vda.io
reclaimPolicy: Delete
volumeBindingMode: Immediate

EOF

    cat <<EOF > testpvc.yaml
---
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: vdacsi-pvc
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 128Mi
  storageClassName: vdacsi-sc

EOF

    cat <<EOF > testpod.yaml
---
kind: Pod
apiVersion: v1
metadata:
  name: vdacsi-test
spec:
  containers:
  - name: alpine
    image: alpine:3
    imagePullPolicy: "IfNotPresent"
    command: ["sleep", "365d"]
    volumeMounts:
    - mountPath: "/vdavol"
      name: vda-volume
  volumes:
  - name: vda-volume
    persistentVolumeClaim:
      claimName: vdacsi-pvc

EOF

    # $work_dir/minikube start --driver=none
    # $work_dir/minikube kubectl -- apply -f $work_dir/controller-rbac.yaml
    # $work_dir/minikube kubectl -- apply -f $work_dir/controller.yaml
    # $work_dir/minikube kubectl -- apply -f $work_dir/node-rbac.yaml
    # $work_dir/minikube kubectl -- apply -f $work_dir/node.yaml
    # $work_dir/minikube kubectl -- apply -f $work_dir/storageclass.yaml
}


function cleanup() {
    set +e
    echo "nvme disconnect-all"
    sudo nvme disconnect-all
    echo "stop minikube"
    minikube status > /dev/null && minikube stop
    echo "delete minikube"
    minikube delete --all
    echo "stop vda_portal"
    ps -f -C vda_portal > /dev/null && killall vda_portal
    echo "stop vda_monitor"
    ps -f -C vda_monitor > /dev/null && killall vda_monitor
    echo "stop vda_dn_agent"
    ps -f -C vda_dn_agent > /dev/null && killall vda_dn_agent
    echo "stop vda_cn_agent"
    ps -f -C vda_cn_agent > /dev/null && killall vda_cn_agent
    echo "stop etcd"
    ps -f -C etcd > /dev/null && killall etcd
    echo "stop spdk"
    ps -f -C reactor_0 > /dev/null && sudo killall reactor_0
    set -e
}

function force_cleanup() {
    sudo nvme disconnect-all
    ps -f -C vda_portal > /dev/null && killall -9 vda_portal
    ps -f -C vda_monitor > /dev/null && killall -9 vda_monitor
    ps -f -C vda_dn_agent > /dev/null && killall -9 vda_dn_agent
    ps -f -C vda_cn_agent > /dev/null && killall -9 vda_cn_agent
    ps -f -C etcd > /dev/null && killall -9 etcd
    ps -f -C reactor_0 > /dev/null && sudo killall -9 reactor_0
}


function stop() {
    cleanup
}

if [ "$1" == "start" ]; then
    start
elif [ "$1" == "stop" ]; then
    stop
else
    echo "unknow action"
    exit 1
fi
