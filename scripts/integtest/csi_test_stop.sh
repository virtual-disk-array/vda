#!/bin/bash

minikube stop
minikube delete --all
killall vda_portal
killall vda_monitor
killall vda_dn_agent
killall vda_cn_agent
killall etcd
sudo killall reactor_0
