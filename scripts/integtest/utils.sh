#!/bin/bash

function cleanup() {
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
    
}
