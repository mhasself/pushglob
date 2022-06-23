import sqlite3
import json
import os
import yaml
import subprocess as sp


__all__ = ['SyncDb']

DEFAULT_GLOBUS_BIN = 'globus'


def get_output(args, stdin=None):
    P = sp.Popen(args, stdout=sp.PIPE, stderr=sp.PIPE, stdin=sp.PIPE)
    if isinstance(stdin, str):
        stdin = stdin.encode('ascii')
    out, err = P.communicate(input=stdin)
    return P.returncode, out, err


TABLE_DEFS = {
    'transfers': [
        '"id" integer primary key',
        '"task_id" str',
        '"status" str',
    ],
    'files': [
        '"id" integer primary key',
        '"space", str',
        '"name" str',
        '"timestamp" float',
        'constraint "file" unique (space, name)',
    ],
    'syncs': [
        '"file_id" integer',
        '"endpoint" str',
        '"timestamp" float',
        '"ok" integer',
        '"transfer_id" integer',
        'FOREIGN KEY(file_id) REFERENCES files(id)'
    ],
}


class SyncDb:
    def __init__(self, config):
        if isinstance(config, str):
            config = yaml.safe_load(open(config, 'r'))
        self.config = config
        db_file = os.path.expanduser(self.config['db_file'])
        if os.path.exists(db_file):
            conn = sqlite3.connect(db_file)
        else:
            conn = self._create_db(db_file)
        self.conn = conn
        self.globus_bin = config.get('globus_bin', DEFAULT_GLOBUS_BIN)

    def _create_db(self, db_file):
        conn = sqlite3.connect(db_file)
        c = conn.cursor()
        for table, tdef in TABLE_DEFS.items():
            if table[0] == '#': continue
            q = ('create table if not exists `%s` (' % table  +
                 ','.join(tdef) + ')')
            c.execute(q)
        conn.commit()
        return conn

    def scan_dir(self, space, path):
        c = self.conn.cursor()
        while len(path) and path[0] == '/':
            path = path[1:]
        space_path = self.config['spaces'][space]['_local']
        base = os.path.join(space_path, path)
        if not os.path.exists(base):
            raise RuntimeError(f"Requested scan on non-existant path: {base}")
        for root, dirs, files in os.walk(base):
            _space, _root = root[:len(space_path)], root[len(space_path):]
            for f in files: # + dirs:
                name = os.path.join(_root, f)
                mtime = os.path.getmtime(os.path.join(root, f))
                c.execute('insert into files '
                          '(space, name, timestamp) '
                          'values (?,?,?) '
                          'on conflict (space, name) do update set timestamp=?',
                          (space, name, mtime, mtime))

        self.conn.commit()

    def check_sync(self, space, path, endpoint):
        print('Updating file database ...')
        self.scan_dir(space, path)

        print('Checking sync records ...')
        query = ('select files.id, files.name, ok, '
                 'files.timestamp as latest, max(S.timestamp) as synced '
                 'from files '
                 'left join (select * from syncs where endpoint=?) as S '
                 'on files.id=S.file_id '
                 'where space=? '
                 'group by files.id')
        rows = self.conn.execute(query, (endpoint, space))
        results = {
            'background': {
                'space': space,
                'endpoint': endpoint,
            },
            'counts': {
                'ok': 0,
                'pending': 0,
                'stale': 0,
            },
            'update': []
        }
        for r in rows:
            file_id, name, ok, local_time, remote_time = r
            if ok == 0:
                results['counts']['pending'] += 1
            elif remote_time is None or remote_time < local_time:
                results['counts']['stale'] += 1
                results['update'].append((file_id, name, local_time))
            else:
                results['counts']['ok'] += 1
        return results

    def create_sync_job(self, results):
        # Create the blob file
        fout = open('tfer.txt', 'w')
        paths = []
        pairs = []
        for _id, _name, _time in results['update']:
            fout.write(f'{_name} {_name}\n')
            pairs.append((_name,_name))

        # Initiate batch transfer
        src_endpoint = self.config['local_endpoint']
        dest_endpoint = results['background']['endpoint']
        space = results['background']['space']

        info = self.globus_request_transfer(
            src_endpoint,
            dest_endpoint,
            self.config['spaces'][space][src_endpoint],
            self.config['spaces'][space][dest_endpoint],
            file_pairs=pairs
        )
        
        ## Add transfer record
        tfer_id = self.add_transfer(info['task_id'])

        # Create the sync entries for each file.
        c = self.conn.cursor()
        for _id, _name, _time in results['update']:
            c.execute('insert into syncs '
                      '(file_id, endpoint, timestamp, ok, transfer_id) '
                      'values (?,?,?,?,?)',
                      (_id, results['background']['endpoint'], _time, 0, tfer_id))
        self.conn.commit()

    def update_syncs(self):
        tfers = self.conn.execute(
            'select transfers.id as tfer_id, task_id, count(file_id) as n from '
            'transfers join syncs '
            'where syncs.transfer_id=transfers.id '
            'group by task_id')
        for row in tfers:
            tfer_id, task_id, n = row
            print(f'Checking status of {task_id} ...')
            info = self.globus_check_transfer(task_id)
            print(' ... ', info['status'])
            if info['status'] == 'SUCCEEDED':
                print(f' ... marking {n} syncs as ok ...')
                self.conn.execute('update syncs set transfer_id=-1, ok=1 '
                                  'where transfer_id=?', (tfer_id,))
            elif info['status'] == 'FAILED':
                print(f' ... marking {n} syncs as failed ...')
                self.conn.execute('delete from  syncs '
                                  'where transfer_id=?', (tfer_id,))
            # Purge stale sync records
            self.conn.commit()

    def add_transfer(self, task_id, _commit=True):
        c = self.conn.execute('insert into transfers (task_id,status) values (?,?)',
                              (task_id, 'unknown'))
        transfer_id = c.lastrowid
        if _commit:
            self.conn.commit()
        return transfer_id

    def list_transfers(self):
        c = self.conn.execute('select distinct task_id, status from transfers')
        for row in c:
            task_id, status = row
            if status not in ['SUCCEEDED']:
                info = self.globus_check_transfer(row[0])
            print(task_id, status, info['status'])

    # Globus stuff

    def globus_request_transfer(self, src_node, dest_node, src_path, dest_path,
                                file_pairs=None):
        src_id = self.config['globus_endpoints'][src_node]
        dest_id = self.config['globus_endpoints'][dest_node]
        args = [self.globus_bin, '-F', 'json', 'transfer']

        stdin_text = ''
        if file_pairs is None:
            args.append('-r')
            args.extend([f'{src_id}:{src_path}', f'{dest_id}:{dest_path}'])
        else:
            args.extend(['--batch', '-'])
            stdin_text = ''.join([f'{src_path}{a} {dest_path}{b}\n' for a, b in file_pairs])
            args.extend([f'{src_id}:', f'{dest_id}:'])

        code, out, err = get_output(args, stdin_text)
        if code != 0:
            raise RuntimeError(f'Error start transfer: {args}\n'
                               f'exit={code}, out={out}, err={err}')
        return json.loads(out)

    def globus_check_transfer(self, task_id=None, _dumpfile=None):
        code, out, err = get_output([self.globus_bin, 'api', 'transfer', 'GET', f'/task/{task_id}'])
        if code != 0:
            raise RuntimeError(f'Error looking up task_id={task_id}; '
                               f'exit={code}, out={out}, err={err}')
        if _dumpfile:
            open(_dumpfile, 'w').write(out.decode('utf8'))
        info = json.loads(out)
        return info

    def globus_test_endpoint(self, endpoint_name):
        endpoint_id = self.config['globus_endpoints'][endpoint_name]
        code, out, err = get_output([self.globus_bin, 'ls', f'{endpoint_id}:', '-F', 'json'])
        report = {
            'ok': (code==0),
            'exit_code': code,
        }
        if code == 0:
            info = json.loads(out)
            report['file_count'] = len(info['DATA'])
        else:
            report['error_text'] = err
        return report
