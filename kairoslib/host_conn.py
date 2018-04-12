#!/usr/bin/env python2

import logging
from sys import exit
from paramiko import SSHClient, AutoAddPolicy
from select import select


class HostConn:
    def __init__(self, ipaddr=None, port=None, username=None, ssh_keyfile=None, timeout=None):
        self.ipaddr = ipaddr
        self.username = None
        self.ssh_keyfile = None
        self.ssh_conn = None
        self.ssh_transport = None
        self.ssh_channel = None
        self.timeout = timeout
        # By default SSH will be on port 22
        if port is None:
            self.port = 22
        else:
            self.port = port

        # Creating the SSH connection and opening a channel
        self.ssh_conn = SSHClient()
        self.ssh_conn.set_missing_host_key_policy(AutoAddPolicy())
        self.ssh_conn.connect(hostname=self.ipaddr, port=self.port, username=self.username, key_filename=self.ssh_keyfile, timeout=self.timeout)

    def run_command(self, cmd=None):
        stdin, stdout, stderr = self.ssh_conn.exec_command(cmd)
        self.ssh_channel = stdout.channel

        stdin.close()
        self.ssh_channel.shutdown_write()

        stdout_chunks = list()
        stdout_chunks.append(stdout.channel.recv(len(stdout.channel.in_buffer)))
        while not self.ssh_channel.closed or self.ssh_channel.recv_ready() or self.ssh_channel.recv_stderr_ready():
            got_chunk = False
            readq, _, _ = select([stdout.channel], [], [], self.timeout)
            for chunk in readq:
                if chunk.recv_ready():
                    stdout_chunks.append(stdout.channel.recv(len(chunk.in_buffer)))
                    got_chunk = True
                if chunk.recv_stderr_ready():
                    stderr.channel.recv_stderr(len(chunk.in_stderr_buffer))
                    got_chunk = False

            if not got_chunk and stdout.channel.exit_status_ready() and not stderr.channel.recv_stderr_ready() and not stdout.channel.recv_ready():
                stdout.channel.shutdown_read()
                stdout.channel.close()
                break

        stdout.close()
        stderr.close()
        return (''.join(stdout_chunks), stdout.channel.recv_exit_status())

    def get_storage_layout(self, mongod_path=None, mountpoint=None):
        if mountpoint is None:
            # -- grabbing mongodb mount point
            result_dbpath = self.run_command('grep -i dbpath ' + mongod_path)
            if result_dbpath[1] == 0:
                mdb_dbpath = result_dbpath[0].split(':')[1].strip()
            else:
                logging.error('Could not get dbpath from host ' + self.ipaddr)
                exit(1)
        else:
            mdb_dbpath = mountpoint

        # -- grabbing mongodb filesystem device
        result_fs = self.run_command('mount | grep ' + mdb_dbpath)
        if result_fs[1] == 0:
            mdb_device = result_fs[0].split()[0]
            fs_type = result_fs[0].split()[4]
        else:
            logging.error('Could not get dbpath device from host ' + self.ipaddr)
            exit(1)

        # -- Checking if the device is a single LUN or a LVM logical volume
        #    if LVM, then Kairos has to get the list of devices that are part of the Volume Group where
        #    MongoDB's dbpath is located.
        result_lv = self.run_command('lvdisplay ' + mdb_device)
        if result_lv[1] == 0:
            mdb_vgname = mdb_device.split('/')[3].split('-')[0]
            result_pvs = self.run_command('pvs | grep ' + mdb_vgname)
            device_list = list()
            for device in result_pvs[0].split('\n'):
                if len(device.split()) > 0:
                    device_list.append(device.strip().split()[0].split('/')[3])

            svm_n_vol = list()
            for device in device_list:
                lun2svm_n_vol = dict()

                # -- grabbing volume and LUN name
                result_sanlun = self.run_command('sanlun lun show -p | grep -B4 ' + device + ' | grep ONTAP')
                if result_sanlun[1] == 0:
                    lun2svm_n_vol['svm-name'] = result_sanlun[0].strip().split(':')[1].strip()
                    lun2svm_n_vol['volume'] = result_sanlun[0].strip().split(':')[2].split('/')[2].strip()
                    lun2svm_n_vol['lun-name'] = result_sanlun[0].strip().split(':')[2].split('/')[3].strip()
                else:
                    logging.error('Could not get output from sanlun for ' + device + '.')
                    exit(1)

                # -- grabbing LUN id
                result_sanlun = self.run_command('sanlun lun show -p | grep -B4 ' + device + ' | grep LUN:')
                if result_sanlun[1] == 0:
                    lun2svm_n_vol['lun-id'] = int(result_sanlun[0].strip().split(':')[1].strip())
                else:
                    logging.error('Could not get output from sanlun for ' + device + '.')

                svm_n_vol.append(lun2svm_n_vol)

        else:
            #TODO: need to create the non-LVM use case.
            pass

        doc = dict()
        doc['volume_topology'] = list()
        for svm_vol in svm_n_vol:
            doc['volume_topology'].append(svm_vol)

        doc['mountpoint'] = mdb_dbpath
        doc['fs_type'] = fs_type
        if result_lv[1] == 0:
            doc['lvm_vgname'] = mdb_vgname
            doc['mdb_device'] = mdb_device

        return doc

    def _get_service_manager(self):
        result = self.run_command('which systemctl')
        if result[1] == 0:
            return 'systemctl'
        else:
            return 'service'

    def stop_service(self, service_name=None):
        svcmgmr = self._get_service_manager()
        if svcmgmr == 'systemctl':
            result_cmd = self.run_command('systemctl stop ' + service_name)
        else:
            result_cmd = self.run_command('service ' + service_name + ' stop')

        return result_cmd

    def start_service(self, service_name=None):
        svcmgmr = self._get_service_manager()
        if svcmgmr == 'systemctl':
            result_cmd = self.run_command('systemctl start ' + service_name)
        else:
            result_cmd = self.run_command('service ' + service_name + ' start')

        return result_cmd

    def disable_vg(self, vg_name=None):
        result_cmd = self.run_command('vgchange -an ' + vg_name)
        return result_cmd

    def enable_vg(self, vg_name=None):
        result_cmd = self.run_command('vgchange -ay ' + vg_name)
        return result_cmd

    def umount_fs(self, fs_mountpoint=None):
        result_cmd = self.run_command('umount -f ' + fs_mountpoint)
        return result_cmd

    def mount_fs(self, fs_mountpoint=None, fs_type=None, device=None):
        result_cmd = self.run_command('mount -t ' + fs_type + ' -o noatime ' + device + ' ' + fs_mountpoint)
        return result_cmd

    def get_hostname(self):
        result_cmd = self.run_command('hostname')
        return result_cmd

    def get_iscsi_iqn(self):
        result_cmd = self.run_command('cat /etc/iscsi/initiatorname.iscsi')
        return result_cmd

    def iscsi_rescan(self):
        result_cmd = self.run_command('/sbin/iscsiadm -m session --rescan')
        return result_cmd

    def iscsi_send_targets(self, iscsi_target=None):
        result_cmd = self.run_command('/sbin/iscsiadm -m discovery -t st -p ' + iscsi_target + ':3260')
        return result_cmd

    def iscsi_node_login(self):
        result_cmd = self.run_command('/sbin/iscsiadm -m node -L all')
        return result_cmd

    def get_wwpn(self):
        pass

    def remove_file(self, filename=None):
        result_cmd = self.run_command('/bin/rm -f ' + filename)
        return result_cmd

    def close(self):
        self.ssh_conn.close()
