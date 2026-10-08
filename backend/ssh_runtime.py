"""Optional owner-scoped SSH runtime for a user-configured remote host.

The remote host, Codex home, and optional router endpoint are supplied through
CODEX_SUBAGENTS_REMOTE_* environment variables. No credentials are copied from
the local machine.
"""
from __future__ import annotations
import base64, json, os, shlex

DRIVER = r'''
import json, os, signal, subprocess, sys, tempfile, threading
from pathlib import Path
config = json.loads(sys.argv[1]); uid = os.getuid()
if uid == 0: raise RuntimeError('root remote runtime is refused')
key_file = Path(config['keyFile']).expanduser() if config.get('keyFile') else None
if key_file:
    stat = key_file.stat()
    if stat.st_uid != uid or stat.st_mode & 0o077: raise RuntimeError('remote key must belong to SSH user and be owner-only')
def schema():
    with tempfile.TemporaryDirectory(prefix='codex-subagents-schema-') as directory:
        subprocess.run(['codex','app-server','generate-json-schema','--experimental','--out',directory],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=30)
        root=Path(directory); requests=json.loads((root/'ClientRequest.json').read_text()); methods={}
        for variant in requests['oneOf']:
            fields=variant.get('properties',{})
            for method in fields.get('method',{}).get('enum',[]):
                ref=fields.get('params',{}).get('$ref','').split('/')[-1]; methods[method]=requests.get('definitions',{}).get(ref,{})
        responses={}
        for method,name in config['responses'].items():
            for path in (root/'v2'/(name+'.json'),root/(name+'.json')):
                if path.exists(): responses[method]=json.loads(path.read_text()); break
        print(json.dumps({'methods':methods,'responses':responses}),flush=True)
if config['mode']=='schema': schema(); raise SystemExit(0)
runtime=Path.home()/'.codex-subagents'/'provider'; runtime.mkdir(parents=True,exist_ok=True,mode=0o700); os.chmod(runtime,0o700)
env=dict(os.environ); env['CODEX_HOME']=str(runtime); env.pop('OPENAI_API_KEY',None); env.pop('CODEX_API_KEY',None)
if key_file: env['OPENAI_API_KEY']=key_file.read_text().strip()
proc=subprocess.Popen(['codex','app-server',*config['args'],'--stdio'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1,env=env)
print(json.dumps({'jsonrpc':'2.0','method':'_process/started','params':{'pid':proc.pid,'uid':uid}}),flush=True)
closing=threading.Event()
def close(*_):
    if closing.is_set(): return
    closing.set()
    if proc.poll() is None:
        proc.terminate()
        try: proc.wait(timeout=3)
        except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=3)
    if _: raise SystemExit(0)
for sig in (signal.SIGHUP,signal.SIGTERM,signal.SIGINT): signal.signal(sig,close)
def pump():
    try:
        for line in sys.stdin:
            if closing.is_set(): break
            proc.stdin.write(line); proc.stdin.flush()
    except (OSError,ValueError): pass
    finally: close()
threading.Thread(target=pump,daemon=True).start()
threading.Thread(target=lambda: [line for line in proc.stderr],daemon=True).start()
try:
    for line in proc.stdout: sys.stdout.write(line); sys.stdout.flush()
finally: close()
'''

def ssh_command(mode, app_args, responses):
    config={'mode':mode,'keyFile':os.environ.get('CODEX_SUBAGENTS_REMOTE_KEY_FILE',''),'args':app_args,'responses':responses}
    encoded=base64.b64encode(DRIVER.encode()).decode(); code=f'import base64; exec(compile(base64.b64decode({encoded!r}),"<codex-subagents-ssh>","exec"))'
    host=os.environ.get('CODEX_SUBAGENTS_REMOTE_SSH_HOST')
    if not host: raise RuntimeError('remote host is not configured; set CODEX_SUBAGENTS_REMOTE_SSH_HOST')
    return ['ssh','-T','-o','BatchMode=yes','-o','ConnectTimeout=10',host,shlex.join(['python3','-u','-c',code,json.dumps(config,separators=(',',':'))])]
