import time
import datetime
import logging
import boto.ec2
import boto.exception

from .config import (
    AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY,
)

logger = logging.getLogger('maas')


def delete_old_snapshots(conn, vol_id, exclude_snap):
    """
    We get only one volume by vol_id and delete all snapshots of it
    excluding exclude_snap and 3 last snapshots
    """
    try:
        # get volume by volume id
        volume = conn.get_all_volumes([vol_id])[0]
        snapshots = volume.snapshots()
        snapshots_sorted = sorted([(s, s.start_time) for s in snapshots],
                                  key=lambda k: k[1])[4:]
        for snapshot, start_time in snapshots_sorted:
            if snapshot.id == exclude_snap:
                continue
            info = "deleting snap {id}: {desc}".format(
                id=snapshot.id, desc=snapshot.description)
            logger.info(info)
            snapshot.delete()
    except (boto.exception.EC2ResponseError) as e:
        logger.error(e)


def manage_snapshots(conn, vol_id):
    """
    Create a snapshot, delete old ones
    """
    retry = 0
    snapshot = None
    while (retry < 3):
        try:
            # create a snapshot
            at_time = datetime.datetime.today().strftime("%d-%m-%Y %H:%M:%S")
            desc = "Vol:{vol};Date:{date}".format(vol=vol_id, date=at_time)
            snapshot = conn.create_snapshot(vol_id, desc)
            info = "snapshot created has id: {}".format(snapshot.id)
            logger.info(info)
            break
        except (boto.exception.EC2ResponseError, AssertionError) as e:
            retry += 1
            logger.error(e)

    if not snapshot:
        return

    while snapshot.status != 'completed':
        time.sleep(2)
        snapshot.update()
        if snapshot.status == 'error':
            return

    delete_old_snapshots(conn, vol_id, snapshot.id)


def extract_non_root_id(bdm):
    """
    Get id of non root volume (data volume in our case)
    """
    try:
        bd = bdm['blockDeviceMapping']['/dev/xvdf']
        return bd.volume_id
    # case when we don't have data volume is also possible
    except KeyError:
        return None


def manage_instances_snapshots():
    """
    Connect to aws, create snapshots for all instances that have data volume,
    delete old data volumes snapshots
    """
    retry = 0
    instances = []
    while (retry < 3):
        try:
            conn = boto.ec2.connect_to_region(
                AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
            instances = conn.get_only_instances()
            break
        except (boto.exception.EC2ResponseError, AssertionError) as e:
            retry += 1
            logger.error(e)

    if not instances:
        logger.error('Max retries exceeded for instance receiving')
        return

    for instance in instances:
        data_vol_id = extract_non_root_id(
            instance.get_attribute("blockDeviceMapping"))
        if data_vol_id:
            manage_snapshots(conn, data_vol_id)
