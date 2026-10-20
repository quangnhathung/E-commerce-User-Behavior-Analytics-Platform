"""Run the local API; no Docker API access or host-global Python installs."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import venv

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from infra.manage import read_env

SERVICE=ROOT/'services'
VENV=SERVICE/'.venv'
ARTIFACTS=ROOT/'artifacts/serving'


def environment():
    values=read_env(ROOT/'.env') if (ROOT/'.env').exists() else {}
    env=os.environ.copy()
    env.pop('MONGO_ROOT_USERNAME',None)
    env.pop('MONGO_ROOT_PASSWORD',None)
    for key in ['MONGO_USERNAME','MONGO_PASSWORD','MONGO_DATABASE']:
        value=values.get(key,env.get(key))
        if not value:raise RuntimeError(f'Missing {key}; run make setup or configure the API environment')
        env[key]=value
    env['MONGO_HOST']='127.0.0.1'
    env['MONGO_PORT']=values.get('MONGO_HOST_PORT',env.get('MONGO_HOST_PORT','27018'))
    env['API_HOST_PORT']=values.get('API_HOST_PORT',env.get('API_HOST_PORT','8000'))
    for key in ['MONGO_PORT','API_HOST_PORT']:
        if not env[key].isdigit() or not 1024<=int(env[key])<=65535:raise RuntimeError(f'Invalid {key}')
    env['PYTHONPATH']=os.pathsep.join([str(SERVICE),str(ROOT/'app')])
    env['PYTHONDONTWRITEBYTECODE']='1'
    return env


def execute(command,env=None):
    result=subprocess.run(command,cwd=SERVICE,env=env)
    if result.returncode:raise RuntimeError(f'Command failed (exit {result.returncode})')


def recorded(command,env,label):
    ARTIFACTS.mkdir(parents=True,exist_ok=True)
    result=None
    path=ARTIFACTS/(label+'.log')
    with path.open('w') as log,subprocess.Popen(command,cwd=SERVICE,env=env,text=True,
            stdout=subprocess.PIPE,stderr=subprocess.STDOUT) as child:
        for line in child.stdout:
            print(line,end='',flush=True);log.write(line)
            if line.startswith('SERVING_RESULT='):result=json.loads(line.partition('=')[2])
        code=child.wait()
    result={**(result or {}),'passed':code==0 and bool(result and result.get('passed')),
            'exit_code':code,'log':str(path.relative_to(ROOT))}
    (ARTIFACTS/(label+'.json')).write_text(json.dumps(result,indent=2)+'\n')
    if not result['passed']:raise RuntimeError(f'{label} failed; inspect {path.relative_to(ROOT)}')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['api-setup','api','api-indexes','test-api','test-api-offline','test-web','smoke-web'])
    args=parser.parse_args()
    if args.action=='api-setup':
        if sys.version_info<(3,11):raise RuntimeError('API requires Python 3.11+; use PYTHON=python3.11 make api-setup')
        if not (VENV/'pyvenv.cfg').exists():venv.create(VENV,with_pip=True)
        execute([str(VENV/'bin/python'),'-m','pip','install','-r',str(SERVICE/'api-requirements.txt')])
        execute([str(VENV/'bin/python'),'-m','pip','check'])
        return
    if args.action=='test-web':
        node=shutil.which('node')
        if node is None:raise RuntimeError('Node 18+ is needed only for JS tests; the dashboard itself uses Live Server')
        execute([node,str(ROOT/'web/tests/dashboard.test.mjs')])
        return
    python=VENV/'bin/python'
    if not python.exists():raise RuntimeError('API environment missing; run make api-setup')
    if args.action=='test-api-offline':
        env=os.environ.copy();env['PYTHONPATH']=os.pathsep.join([str(SERVICE),str(ROOT/'app')])
        recorded([str(python),'-m','serving.checks','api','--offline'],env,'test-api-offline')
        return
    env=environment()
    if args.action=='api':
        print('API: http://127.0.0.1:'+env['API_HOST_PORT']+'/docs',flush=True)
        execute([str(python),'-m','uvicorn','serving.api:app','--host','127.0.0.1','--port',env['API_HOST_PORT'],
                 '--workers','1','--no-access-log'],env)
    elif args.action=='api-indexes':execute([str(python),'-m','serving.indexes'],env)
    elif args.action=='test-api':
        env['SERVING_LIVE_MONGO']='1'
        recorded([str(python),'-m','serving.checks','api'],env,'test-api')
    elif args.action=='smoke-web':
        run_id=os.getenv('RUN_ID','')
        if not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}',run_id):raise RuntimeError('RUN_ID is required')
        recorded([str(python),'-m','serving.smoke','--run-id',run_id],env,'smoke-web-'+run_id)


if __name__=='__main__':
    try:main()
    except KeyboardInterrupt:
        sys.exit(130)
    except (RuntimeError,OSError) as error:
        print('ERROR: '+str(error),file=sys.stderr)
        sys.exit(1)
