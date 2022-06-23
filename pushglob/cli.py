import argparse
import os
import socket
import sys
import yaml

from .db import SyncDb

CFG_ENV = 'PUSHGLOB_CONFIG'
DEFAULT_CFG = '~/.pushglob'

CFG_TEMPLATE = """#pushglob config -- yaml
db_file:        ~/.pushglob.sqlite
local_endpoint: %(HOSTNAME)s

# Place-holder
globus_endpoints: {}

#globus_endpoints:
#  short-endpoint-name1: 'a98234780-weird-hex-uuid'
#  short-endpoint-name2: '8defa9091-weird-hex-uuid'

# Place-holder
spaces: {}

#spaces:
#  test_space:
#    short-endpoint-name1: '/path/on/endpoint1/my_test_space'
#    short-endpoint-name2: '/path/on/endpoint2/my_test_space'
#
"""


def get_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config-file', '-c')
    cmd_sp = parser.add_subparsers(dest='command')

    p = cmd_sp.add_parser('auto')

    p = cmd_sp.add_parser('sync')
    p.add_argument('dest_endpoint')
    p.add_argument('--space', '-s')
    p.add_argument('source_path', nargs='?')

    p = cmd_sp.add_parser('config')

    p = cmd_sp.add_parser('test')
    p.add_argument('--endpoint', '-e', default=None, action='append')

    return parser

def setup(args):
    if not os.path.exists(args.config_file):
        print('Creating config file %s...' % args.config_file)

        open(args.config_file, 'w').write(CFG_TEMPLATE %
                                          {'HOSTNAME': socket.gethostname})
    config = yaml.safe_load(open(args.config_file, 'r'))
    db = SyncDb(config)
    return config, db

def main(args=None):
    if args is None:
        args = sys.argv[1:]
    parser = get_parser()
    args = parser.parse_args()

    if args.config_file is None:
        if os.getenv(CFG_ENV):
            args.config_file = os.getenv(CFG_ENV)
        else:
            args.config_file = os.path.expanduser(DEFAULT_CFG)

    config, db = setup(args)

    if args.command == 'auto':
        print('Querying globus transfer jobs status ...')
        check = db.update_syncs()
        print(' ... done')

    elif args.command == 'sync':
        # Check endpoint ...
        assert(args.dest_endpoint in config['globus_endpoints'])
        # Resolve path to a space.
        source_path = args.source_path
        if source_path is None:
            if args.space is not None:
                space_paths = config['spaces'][args.space]
                source_path = space_paths.get('_local', space_paths.get(config['local_endpoint']))
                print(f'Using space root path "{source_path}"')
            else:
                source_path = os.getcwd()
        for space_name, space_paths in config['spaces'].items():
            space_path = space_paths.get('_local', space_paths.get(config['local_endpoint']))
            common = os.path.commonpath([source_path, space_path])
            if common.startswith(space_path):
                break
        else:
            raise RuntimeError(
                f'The source directory "{source_path}" does not appear to be in syncable space.')

        # Make a sync plan.
        rel_path = os.path.relpath(source_path, space_path)
        plan = db.check_sync(space_name, rel_path, args.dest_endpoint)
        if plan['counts']['pending']:
            print('Note there are %i pending transfers for this subspace.' %
                  plan['counts']['pending'])
        if plan['counts']['stale'] > 0:
            print('Yes, we will create a new sync job, for %i items.' %
                  plan['counts']['stale'])
            db.create_sync_job(plan)
        else:
            print('No new sync job; %i targets ok and %i pending.' % (
                plan['counts']['ok'], plan['counts']['pending']))

    elif args.command == 'config':
        print('These are the globus "endpoints" I know about:')
        for k, v in config['globus_endpoints'].items():
            print('  %-20s %s' % (k, v))
        print()
        print('These are the "spaces" (and their local paths) I know about:')
        for k, v in config['spaces'].items():
            print('  %-20s %s' % (k, v.get('_local', v.get(config['local_endpoint']))))
        print()

    elif args.command == 'test':
        to_test = args.endpoint
        if to_test is None:
            print('Probing configured globus endpoints ...')
            to_test = config['globus_endpoints'].keys()  # all
        else:
            print('Probing requested globus endpoints ...')
        if len(to_test) == 0:
            print(' ... no endpoints to test!')

        for k in to_test:
            print(' ... testing endpoint "%s" (%s)' % (k, config['globus_endpoints'][k]))
            report = db.globus_test_endpoint(k)
            if report['ok']:
                print(' ...    looks good, I see %i files at base level' %
                      report['file_count'])
            else:
                print(' ...    looks bad ... here is the error text: \n%s\n' %
                      report['error_text'])
