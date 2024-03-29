from spdk.rpc.client import print_json

def bdev_susres_create_func(client, base_bdev_name, name):
    """Construct a susres block device.
 
    Args:
        base_bdev_name: name of the existing bdev
        name: name of block device
 
    Returns:
        Name of created block device.
    """
    params = {
        'base_bdev_name': base_bdev_name,
        'name': name,
    }
    return client.call('bdev_susres_create', params)


def bdev_susres_delete_func(client, name):
    """Remove a susres bdev from the system.

    Args:
        name: name of the susres bdev to delete
    """
    params = {'name': name}
    return client.call('bdev_susres_delete', params)


def bdev_susres_suspend_func(client, name):
    """Suspend a susres bdev

    Args:
        name: name of the susres bdev
    """
    params = {'name': name}
    return client.call('bdev_susres_suspend', params)


def bdev_susres_resume_func(client, base_bdev_name, name):
    """Resume a susres bdev

    Args:
        base_bdev_name: name of the existing bdev
        name: name of the susres bdev
    """
    params = {
        'base_bdev_name': base_bdev_name,
        'name': name,
    }
    return client.call('bdev_susres_resume', params)


def bdev_raid1_create_func(client, raid1_name, bdev0_name, bdev1_name,
                           bit_size_kb, write_delay, clean_ratio,
                           max_delay, max_resync, meta_size,
                           synced, ignore_zero_block):
    params = {
        'raid1_name': raid1_name,
        'bdev0_name': bdev0_name,
        'bdev1_name': bdev1_name,
    }
    if bit_size_kb is not None:
        params['bit_size_kb'] = bit_size_kb
    if write_delay is not None:
        params['write_delay'] = write_delay
    if clean_ratio is not None:
        params['clean_ratio'] = clean_ratio
    if max_delay is not None:
        params['max_delay'] = max_delay
    if max_resync is not None:
        params['max_resync'] = max_resync
    if meta_size is not None:
        params['meta_size'] = meta_size
    if synced is not None:
        params['synced'] = synced
    if ignore_zero_block is not None:
        params['ignore_zero_block'] = ignore_zero_block
    return client.call('bdev_raid1_create', params)


def bdev_raid1_delete_func(client, raid1_name):
    params = {
        'raid1_name': raid1_name,
    }
    return client.call('bdev_raid1_delete', params)


def bdev_raid1_dump_func(client, bdev_name):
    params = {
        'bdev_name': bdev_name,
    }
    return client.call('bdev_raid1_dump', params)


def spdk_rpc_plugin_initialize(subparsers):

    def bdev_susres_create(args):
        print_json(bdev_susres_create_func(args.client,
                                           base_bdev_name=args.base_bdev_name,
                                           name=args.name))
 
    p = subparsers.add_parser('bdev_susres_create',
                              help='Create susres bdev')
    p.add_argument('-b', '--base-bdev-name', help="Name of the existing bdev", required=True)
    p.add_argument('-p', '--name', help="Name of the susres bdev", required=True)
    p.set_defaults(func=bdev_susres_create)

    def bdev_susres_delete(args):
        print_json(bdev_susres_delete_func(args.client,
                                           name=args.name))

    p = subparsers.add_parser('bdev_susres_delete', help='Delete a susres bdev')
    p.add_argument('-p', '--name', help='susres bdev name')
    p.set_defaults(func=bdev_susres_delete)

    def bdev_susres_suspend(args):
        print_json(bdev_susres_suspend_func(args.client,
                                            name=args.name))

    p = subparsers.add_parser('bdev_susres_suspend', help='Suspend a susres bdev')
    p.add_argument('-p', '--name', help='susres bdev name')
    p.set_defaults(func=bdev_susres_suspend)

    def bdev_susres_resume(args):
        print_json(bdev_susres_resume_func(args.client,
                                           base_bdev_name=args.base_bdev_name,
                                           name=args.name))

    p = subparsers.add_parser('bdev_susres_resume', help='Resume a susres bdev')
    p.add_argument('-b', '--base-bdev-name', help="Name of the existing bdev", required=True)
    p.add_argument('-p', '--name', help='susres bdev name')
    p.set_defaults(func=bdev_susres_resume)

    def bdev_raid1_create(args):
        print_json(bdev_raid1_create_func(args.client,
                                          raid1_name=args.raid1_name,
                                          bdev0_name=args.bdev0_name,
                                          bdev1_name=args.bdev1_name,
                                          bit_size_kb=args.bit_size_kb,
                                          write_delay=args.write_delay,
                                          clean_ratio=args.clean_ratio,
                                          max_delay=args.max_delay,
                                          max_resync=args.max_resync,
                                          meta_size=args.meta_size,
                                          synced=args.synced,
                                          ignore_zero_block=args.ignore_zero_block))

    p = subparsers.add_parser('bdev_raid1_create',
                              help='Create raid1 bdev')
    p.add_argument('-n', '--raid1-name', required=True,
                   help="Name of the raid1 bdev")
    p.add_argument('-b0', '--bdev0-name', required=True,
                   help="The first underling bdev name")
    p.add_argument('-b1', '--bdev1-name', required=True,
                   help="The second underling bdev name")
    p.add_argument('-s', '--bit-size-kb', type=int,
                   help="The bit size in kb")
    p.add_argument('-w', '--write-delay', type=int,
                   help="Write delay")
    p.add_argument('-c', '--clean-ratio', type=int,
                   help="Clean ratio")
    p.add_argument('-d', '--max-delay', type=int,
                   help="Max pending")
    p.add_argument('-r', '--max-resync', type=int,
                   help="Max resync")
    p.add_argument('-m', '--meta-size', type=int,
                   help="Set meta data size")
    p.add_argument('-y', '--synced', action='store_true',
                   help="Whether the underling bdevs are synched")
    p.add_argument('-i', '--ignore-zero-block', action='store_true',
                   help="Do not sync up from bdev0 to bdev1 if a block is all zero")
    p.set_defaults(func=bdev_raid1_create)

    def bdev_raid1_delete(args):
        print_json(bdev_raid1_delete_func(args.client,
                                          raid1_name=args.raid1_name))

    p = subparsers.add_parser('bdev_raid1_delete',
                              help='Delte raid1 bdev')
    p.add_argument('-n', '--raid1-name', required=True,
                   help="Name of the raid1 bdev")
    p.set_defaults(func=bdev_raid1_delete)

    def bdev_raid1_dump(args):
        print_json(bdev_raid1_dump_func(args.client,
                                        bdev_name=args.bdev_name))

    p = subparsers.add_parser('bdev_raid1_dump',
                              help='Dump raid1 super block')
    p.add_argument('-b', '--bdev-name', required=True,
                   help="Name of the bdev")
    p.set_defaults(func=bdev_raid1_dump)
