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
    p.add_argument('--name', help='susres bdev name')
    p.set_defaults(func=bdev_susres_delete)
