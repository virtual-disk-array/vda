package csidriver

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"k8s.io/klog"
	"k8s.io/utils/exec"
	"k8s.io/utils/mount"
)

type CnListenerConf struct {
	TrType  string `json:trtype`
	TrAddr  string `json:traddr`
	AdrFam  string `json:adrfam`
	TrSvcId string `json:trsvcid`
}

const (
	initiatorNqnPrefix = "nqn.2016-06.io.spdk:"
)

func getInitiatorNqn(nodeId string) string {
	return fmt.Sprintf("%s%s", initiatorNqnPrefix, nodeId)
}

func getNvmfAddrPort(confStr string) (string, string, error) {
	var conf CnListenerConf
	err := json.Unmarshal([]byte(confStr), &conf)
	if err != nil {
		return "", "", err
	}
	return conf.TrAddr, conf.TrSvcId, nil
}

type NodeOperatorInterface interface {
	ExecWithTimeout(cmdLine []string, timeout int, ctx context.Context) error
	CheckDeviceReady(devPath string) (bool, error)
	WaitForDeviceReady(devPath string, seconds int) error
	WaitForDeviceGone(devPath string, seconds int) error
	MountAndFormat(devPath, stagingPath, fsType string, mntFlags []string) error
	Mount(stagingPath, targetPath, fsType string, mntFlags []string) error
	DeleteMountPoint(path string) error
}

type nodeOperator struct {
	execer  exec.Interface
	mounter mount.Interface
}

func (no *nodeOperator) ExecWithTimeout(
	cmdLine []string, timeout int,
	ctx context.Context) error {
	klog.Infof("running command: %v", cmdLine)
	cmd := no.execer.CommandContext(ctx, cmdLine[0], cmdLine[1:]...)
	output, err := cmd.CombinedOutput()

	if ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("timed out")
	}
	if output != nil {
		klog.Infof("command returned: %s", output)
	}
	return err
}

func (no *nodeOperator) CheckDeviceReady(deviceGlob string) (bool, error) {
	matches, err := filepath.Glob(deviceGlob)
	if err != nil {
		return false, err
	}
	if len(matches) > 0 {
		return true, nil
	} else {
		return false, nil
	}
}

func (no *nodeOperator) WaitForDeviceReady(deviceGlob string, seconds int) error {
	for i := 0; i <= seconds; i++ {
		time.Sleep(time.Second)
		matches, err := filepath.Glob(deviceGlob)
		if err != nil {
			return err
		}
		// two symbol links under /dev/disk/by-id/ to same device
		if len(matches) > 0 {
			return nil
		}
	}
	return fmt.Errorf("timed out waiting device ready: %s", deviceGlob)
}

func (no *nodeOperator) WaitForDeviceGone(deviceGlob string, seconds int) error {
	for i := 0; i <= seconds; i++ {
		time.Sleep(time.Second)
		matches, err := filepath.Glob(deviceGlob)
		if err != nil {
			return err
		}
		if len(matches) == 0 {
			return nil
		}
	}
	return fmt.Errorf("timed out waiting device gone: %s", deviceGlob)
}

func (no *nodeOperator) MountAndFormat(
	devPath, stagingPath, fsType string, mntFlags []string) error {
	notMount, err := mount.IsNotMountPoint(no.mounter, stagingPath)
	if os.IsNotExist(err) {
		notMount = true
		err = os.MkdirAll(stagingPath, 0755)
	}
	if err != nil {
		return err
	}

	if notMount {
		mounter := mount.SafeFormatAndMount{no.mounter, no.execer}
		err = mounter.FormatAndMount(devPath, stagingPath, fsType, mntFlags)
		return err
	}
	return nil
}

func (no nodeOperator) Mount(
	stagingPath, targetPath, fsType string, mntFlags []string) error {
	notMount, err := mount.IsNotMountPoint(no.mounter, targetPath)
	if os.IsNotExist(err) {
		notMount = true
		err = os.MkdirAll(targetPath, 0755)
	}
	if err != nil {
		return err
	}

	if notMount {
		return no.mounter.Mount(stagingPath, targetPath, fsType, mntFlags)
	} else {
		return nil
	}
}

func (no *nodeOperator) DeleteMountPoint(stagingPath string) error {
	unmounted, err := mount.IsNotMountPoint(no.mounter, stagingPath)
	if os.IsNotExist(err) {
		klog.Infof("%s already deleted", stagingPath)
		return nil
	}
	if err != nil {
		return err
	}
	if !unmounted {
		err = no.mounter.Unmount(stagingPath)
		if err != nil {
			return err
		}
	}
	return os.RemoveAll(stagingPath)
}

func newNodeOperator() *nodeOperator {
	return &nodeOperator{
		mounter: mount.New(""),
		execer:  exec.New(),
	}
}
