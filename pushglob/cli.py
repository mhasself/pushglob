import argparse
import os
import sys
import yaml

from .db import SyncDb

CFG_ENV = 'PUSHGLOB_CONFIG'
DEFAULT_CFG = '~/.pushglob'

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

    return parser

def setup(args):
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
