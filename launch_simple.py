"""Open the single local simple runtime; never activate the strategy."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from urllib.request import build_opener,ProxyHandler
import webbrowser

HERE=Path(__file__).resolve().parent


def ready(url, runtime_root):
    try:
        with build_opener(ProxyHandler({})).open(url+'health',timeout=1) as response:
            data=json.load(response)
        return data.get('application')=='c801-simple' and Path(data.get('database','')).resolve()==runtime_root.resolve()/'c801_simple.sqlite'
    except (OSError,ValueError):return False


def launch(args):
    url='http://127.0.0.1:'+str(args.port)+'/'
    if getattr(args,'restart',False):
        restart_existing(url,args.runtime_root,args.port)
    if ready(url,args.runtime_root):
        if not args.no_browser:webbrowser.open(url)
        return 0
    with socket.socket() as probe:
        probe.settimeout(.5)
        if probe.connect_ex(('127.0.0.1',args.port))==0:
            print('The local port is already in use. No additional instance was started.');return 1
    exe=Path(sys.executable)
    if os.name=='nt' and exe.with_name('pythonw.exe').is_file():exe=exe.with_name('pythonw.exe')
    command=[str(exe),'-X','utf8','-B',str(HERE/'simple_app.py'),'--runtime-root',str(args.runtime_root.resolve()),'--port',str(args.port),'--no-browser']
    if args.legacy_market:command+=['--legacy-market',str(args.legacy_market.resolve())]
    options=dict(cwd=HERE,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,close_fds=True)
    if os.name=='nt':options['creationflags']=subprocess.CREATE_NO_WINDOW|subprocess.CREATE_NEW_PROCESS_GROUP
    else:options['start_new_session']=True
    process=subprocess.Popen(command,**options)
    deadline=time.monotonic()+10
    while time.monotonic()<deadline:
        if ready(url,args.runtime_root):
            if not args.no_browser:webbrowser.open(url)
            return 0
        if process.poll() is not None:
            print('Startup failed. Check the local runtime directory and port.');return 1
        time.sleep(.15)
    # Do not kill a merely slow process or create another owner.
    print('The application is still starting. Try the launcher again shortly.');return 2


def restart_existing(url,runtime_root,port):
    """Explicit user-invoked update only; replacement always starts OFF."""
    import psutil
    if not ready(url,runtime_root):return
    candidates=[]
    for connection in psutil.net_connections(kind='tcp'):
        if connection.status!='LISTEN' or not connection.laddr or connection.laddr.port!=port:continue
        if connection.laddr.ip!='127.0.0.1' or connection.pid is None:
            raise RuntimeError('Port ownership does not match. No process was stopped.')
        p=psutil.Process(connection.pid);command=p.cmdline();born=p.create_time()
        scripts=[arg for arg in command if arg.lower().endswith('simple_app.py')]
        if len(scripts)!=1 or Path(scripts[0]).resolve()!=HERE/'simple_app.py':
            raise RuntimeError('Application path does not match. No process was stopped.')
        if '--runtime-root' not in command:raise RuntimeError('The runtime directory could not be verified.')
        root=command[command.index('--runtime-root')+1]
        if Path(root).resolve()!=runtime_root.resolve():raise RuntimeError('The runtime directory does not match.')
        candidates.append((p.pid,born))
    candidates=list(dict.fromkeys(candidates))
    if len(candidates)!=1:raise RuntimeError('Cannot uniquely identify the strategy process. No process was stopped.')
    pid,born=candidates[0];process=psutil.Process(pid)
    if process.create_time()!=born:raise RuntimeError('The process changed. Run the update command again.')
    process.terminate();process.wait(timeout=10)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description='Open the C801 Trading Workbench')
    parser.add_argument('--runtime-root',type=Path,required=True)
    parser.add_argument('--legacy-market',type=Path)
    parser.add_argument('--port',type=int,default=58445)
    parser.add_argument('--no-browser',action='store_true')
    parser.add_argument('--restart',action='store_true',help='Explicitly apply an update; the replacement process starts with trading disabled')
    args=parser.parse_args()
    try:raise SystemExit(launch(args))
    except Exception as exc:
        if args.restart:
            import tkinter as tk
            from tkinter import messagebox
            root=tk.Tk();root.withdraw()
            messagebox.showerror('Strategy update not completed',str(exc));root.destroy()
        else:raise
