import time
import uuid
import logging
import boto.ec2
import boto.exception

from .config import (
    INSTANCE_TYPE, DATA_VOLUME_SIZE, DATA_VOLUME_RATE, DATA_VOLUME_TYPE,
    AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_AMI_IMAGE_ID,
    AWS_KEY_NAME, AWS_SECURITY_GROUPS, AWS_SBNET_ID
)

logger = logging.getLogger('maas')

USER_SCRIPT_TEMPLATE = """#!/bin/bash -ex
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

## setup the ebs volume for data.
avail_blk=`lsblk -n -oNAME,MOUNTPOINT | grep -v '/$' | grep -v 'xvda' | awk -F' ' '{{print $1}}'`
if [ -z "$avail_blk" ]; then
    echo "Don't have a mounted data blk device."
    exit -1
fi

update_needed=`file -s /dev/$avail_blk | awk -F':' '{{print $2}}'`
setup_fs=`echo "$update_needed" | egrep -e '^[[:space:]]+data$' |wc -l`

if [ $setup_fs -eq 1 ]; then
    echo "Setting up a file system for /dev/$avail_blk"
    mkfs -t ext4 /dev/$avail_blk
fi
cp /etc/fstab /etc/fstab.orig
echo "/dev/$avail_blk /mnt/data ext4 defaults,nofail,nobootwait 0 2" >> /etc/fstab
mount -a
echo "{some_variable_to_pass}" >> /dev/null
judo service supervisor start
supervisorctl start all
"""

def create_bdm(size, type, rate, non_root_snap_id):
    """ Create a pair of block-devices to attach to the instance.
    """
    retry = 0
    while (retry < 3):
        try:
            bd_root = boto.ec2.blockdevicemapping.BlockDeviceType()
            bd_nonroot = boto.ec2.blockdevicemapping.BlockDeviceType()
            size = int(size[:-1])
            bd_nonroot.size = size
            bd_nonroot.volume_type = type
            if type == 'io1':
                bd_nonroot.iops = rate
            bd_nonroot.delete_on_termination = False
            if non_root_snap_id:
                bd_nonroot.snapshot_id = non_root_snap_id
            bdmapping = boto.ec2.blockdevicemapping.BlockDeviceMapping()
            bdmapping['/dev/sda1'] = bd_root
            bdmapping['/dev/xvdf'] = bd_nonroot
            return bdmapping
        except (boto.exception.EC2ResponseError, AssertionError) as e:
            retry += 1
            logger.exception(e)
            logger.error(e)


def try_to_create_ec2_instance():

    try:
	# some samples of required info, you can find it on your instance menu
	# AWS_REGION = 'us-west-2
	# kays can be found in instance menu as well
        conn = boto.ec2.connect_to_region(
            AWS_REGION,
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

        tenantid = str(uuid.uuid4())[:8]

	# INSTANCE_TYPE = 't2.micro'
        instance_type = INSTANCE_TYPE
	# DATA_VOLUME_SIZE = '8G'
        data_volume_size = DATA_VOLUME_SIZE
	# DATA_VOLUME_RATE = 100
        data_volume_rate = DATA_VOLUME_RATE
	# DATA_VOLUME_PROVISIONED = False
        data_volume_type = DATA_VOLUME_TYPE

        bdm = create_bdm(data_volume_size, data_volume_type,
                         data_volume_rate)

	some_variable_to_pass = "here is some data that you want to pass to server"

        cmd = USER_SCRIPT_TEMPLATE.format(
            some_variable_to_pass=some_variable_to_pass,
            tenantid=tenantid,
        )

        logger.debug(cmd)
        user_data = cmd

	# AWS_AMI_IMAGE_ID = 'ami-f53b97b6'
	# AWS_KEY_NAME = 'your-aws-key-name'
	# AWS_SECURITY_GROUPS = ['sr-b47f11z1']
	# AWS_SBNET_ID  = 'subnet-75bc7719'
        reservation = conn.run_instances(
            AWS_AMI_IMAGE_ID,
            instance_type=instance_type,
            key_name=AWS_KEY_NAME,
            security_group_ids=AWS_SECURITY_GROUPS,
            subnet_id=AWS_SBNET_ID,
            block_device_map=bdm,
            user_data=user_data,
        )

        instance = reservation.instances[0]

        while instance.update() != "running":
            time.sleep(5)

        conn.create_tags([instance.id], {"Name": tenantid})
        instance.update()

        # Check that instances got an IP and proper name
        assert instance.ip_address is not None
        assert instance.tags.get('Name') == tenantid
        assert instance.update() == "running"

        return instance

    except (boto.exception.EC2ResponseError, AssertionError) as e:
        logger.exception(e)
        logger.error(e)
        return None


def create_ec2_instance(non_root_snap_id, instance_id, max_retry=5):

    retry = 0

    while(retry < max_retry):
        instance = try_to_create_ec2_instance()
        if instance:
            return instance

    raise Exception("Can't create an instance")
