#!/usr/bin/env python3
import argparse, hashlib, os, re, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
MIGRATIONS = ROOT / 'migrations'
NAME_RE = re.compile(r'^V(\d+)__(.+)\.sql$', re.I)

class MigrationError(Exception): pass

def connect():
    dsn = os.getenv('PG_CONN')
    if not dsn: raise MigrationError('PG_CONN is not set.')
    try:
        import psycopg2
    except ImportError: raise MigrationError('Install dependencies: python -m pip install -r requirements.txt')
    try: return psycopg2.connect(dsn)
    except Exception as e: raise MigrationError(f'PostgreSQL connection failed: {e}')

def checksum(content): return hashlib.sha256(content.encode('utf-8')).hexdigest()

def split_sql(content):
    lines = content.splitlines(); up = down = None
    for i, line in enumerate(lines):
        marker = line.strip().upper()
        if marker == '-- UP':
            if up is not None: raise MigrationError('Duplicate -- UP section.')
            up = i + 1
        elif marker == '-- DOWN':
            if down is not None: raise MigrationError('Duplicate -- DOWN section.')
            down = i
    if up is None or down is None or down <= up: raise MigrationError('Each migration needs -- UP followed by -- DOWN.')
    up_sql, down_sql = '\n'.join(lines[up:down]).strip(), '\n'.join(lines[down + 1:]).strip()
    if not up_sql or not down_sql: raise MigrationError('Both UP and DOWN sections need SQL.')
    return up_sql, down_sql

def load_migrations():
    items=[]
    for path in MIGRATIONS.glob('*.sql'):
        m=NAME_RE.match(path.name)
        if not m: continue
        content=path.read_text(encoding='utf-8'); up,down=split_sql(content)
        items.append({'version':f'V{int(m.group(1)):03d}','description':m.group(2).replace('_',' '),'content':content,'up':up,'down':down,'checksum':checksum(content)})
    items.sort(key=lambda x:int(x['version'][1:]))
    versions=[x['version'] for x in items]
    if len(versions)!=len(set(versions)): raise MigrationError('Duplicate migration versions.')
    return items

def validate_history(migrations, rows):
    files={m['version']:m for m in migrations}; applied={r[0]:r[2] for r in rows}
    for version, saved in applied.items():
        if version not in files: raise MigrationError(f'Applied migration {version} has no file.')
        if files[version]['checksum'] != saved: raise MigrationError(f'Checksum mismatch for {version}. Do not edit applied migrations.')
    pending=False
    for m in migrations:
        v=m['version']
        if v not in applied: pending=True
        elif pending: raise MigrationError(f'Out-of-order migration history: {v} is applied while an earlier migration is missing.')

def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute('''CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, description TEXT, applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), checksum TEXT NOT NULL)''')
    conn.commit()

def applied_rows(conn):
    with conn.cursor() as cur:
        cur.execute('SELECT version, description, checksum FROM schema_migrations ORDER BY version')
        return cur.fetchall()

def init():
    c=connect()
    try: ensure_table(c); print('schema_migrations table is ready.')
    finally: c.close()

def status():
    ms=load_migrations(); c=connect()
    try:
        ensure_table(c); rows=applied_rows(c); validate_history(ms,rows); done={r[0] for r in rows}
        print('Migration status:')
        for m in ms: print(f"{m['version']} - {m['description']} [{'APPLIED' if m['version'] in done else 'PENDING'}]")
        if not ms: print('No migration files found.')
    finally: c.close()

def up(target=None):
    ms=load_migrations(); c=connect()
    try:
        ensure_table(c); rows=applied_rows(c); validate_history(ms,rows); done={r[0] for r in rows}
        if target and target not in {m['version'] for m in ms}: raise MigrationError(f'Target {target} was not found.')
        limit=int(target[1:]) if target else None
        pending=[m for m in ms if m['version'] not in done and (limit is None or int(m['version'][1:])<=limit)]
        if not pending: print('No pending migrations.'); return
        for m in pending:
            earlier=[x['version'] for x in ms if int(x['version'][1:])<int(m['version'][1:])]
            now={r[0] for r in applied_rows(c)}; missing=[v for v in earlier if v not in now]
            if missing: raise MigrationError(f"Cannot apply {m['version']}: earlier migration {missing[0]} has not been applied.")
            try:
                with c.cursor() as cur:
                    cur.execute(m['up'])
                    cur.execute('INSERT INTO schema_migrations(version,description,checksum) VALUES (%s,%s,%s)',(m['version'],m['description'],m['checksum']))
                c.commit(); print(f"Applied {m['version']} - {m['description']}")
            except Exception as e:
                c.rollback(); raise MigrationError(f"{m['version']} failed; transaction rolled back: {e}")
    finally: c.close()

def down(target=None):
    ms=load_migrations(); c=connect()
    try:
        ensure_table(c); rows=applied_rows(c); validate_history(ms,rows); done={r[0] for r in rows}
        if target and target not in {m['version'] for m in ms}: raise MigrationError(f'Target {target} was not found.')
        applied=[m for m in ms if m['version'] in done]
        rollback=applied[-1:] if target is None else [m for m in applied if int(m['version'][1:])>int(target[1:])][::-1]
        if not rollback: print('No migrations to roll back.'); return
        for m in rollback:
            try:
                with c.cursor() as cur:
                    cur.execute(m['down']); cur.execute('DELETE FROM schema_migrations WHERE version=%s',(m['version'],))
                c.commit(); print(f"Rolled back {m['version']} - {m['description']}")
            except Exception as e:
                c.rollback(); raise MigrationError(f"Rollback of {m['version']} failed; transaction rolled back: {e}")
    finally: c.close()

def new(description):
    safe=re.sub(r'[^a-zA-Z0-9]+','_',description.strip()).strip('_').lower()
    if not safe: raise MigrationError('Provide a migration description.')
    number=max((int(m['version'][1:]) for m in load_migrations()),default=0)+1
    path=MIGRATIONS/f'V{number:03d}__{safe}.sql'; path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists(): raise MigrationError(f'{path.name} already exists.')
    path.write_text('-- UP\n-- Add your schema change here.\n\n-- DOWN\n-- Add the rollback SQL here.\n',encoding='utf-8')
    print(f'Created migrations/{path.name}')

def main(argv=None):
    p=argparse.ArgumentParser(description='Mini PostgreSQL database migration tool')
    sub=p.add_subparsers(dest='command',required=True)
    sub.add_parser('init'); sub.add_parser('status')
    a=sub.add_parser('up'); a.add_argument('--to',dest='target')
    d=sub.add_parser('down'); d.add_argument('--to',dest='target')
    n=sub.add_parser('new'); n.add_argument('description')
    args=p.parse_args(argv)
    try:
        if args.command=='init': init()
        elif args.command=='status': status()
        elif args.command=='up':
            if args.target and not re.fullmatch(r'V\d{3,}',args.target): raise MigrationError('Use a target such as V002.')
            up(args.target)
        elif args.command=='down':
            if args.target and not re.fullmatch(r'V\d{3,}',args.target): raise MigrationError('Use a target such as V001.')
            down(args.target)
        elif args.command=='new': new(args.description)
        return 0
    except MigrationError as e:
        print(f'ERROR: {e}',file=sys.stderr); return 1

if __name__=='__main__': raise SystemExit(main())
