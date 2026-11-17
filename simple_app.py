"""User-invoked simple local panel. Starting the process leaves trading off."""
import argparse
from pathlib import Path
import webbrowser

from c801.simple_runtime import DirectRuntime,public_transport
from c801.simple_dashboard import ChartWorker,dashboard_server
from c801.owner_lock import OwnerLock

HERE=Path(__file__).resolve().parent


def main():
    parser=argparse.ArgumentParser(description='C801: account fills and historical candlestick dashboard')
    parser.add_argument('--runtime-root',type=Path,required=True)
    parser.add_argument('--legacy-market',type=Path)
    parser.add_argument('--port',type=int,default=58445)
    parser.add_argument('--no-browser',action='store_true')
    args=parser.parse_args()
    with OwnerLock(args.runtime_root.resolve()/'simple.lock'):
        serve(args)


def serve(args):
    path=args.runtime_root.resolve()/'c801_simple.sqlite'
    runtime=DirectRuntime(path,legacy_market=args.legacy_market)
    charts=ChartWorker(path,public_transport)
    # Bind before starting any worker, so a second launch cannot create a
    # second execution loop on the published local port.
    server=dashboard_server(path,html_path=HERE/'simple_index.html',port=args.port,chart_worker=charts,
        start_strategy=runtime.control.start,pause_entries=runtime.control.pause,clock_ms=runtime.control.now_ms)
    try:
        charts.start();runtime.start_background()
        if not args.no_browser:webbrowser.open('http://127.0.0.1:'+str(args.port)+'/')
        server.serve_forever(poll_interval=.2)
    finally:
        runtime.close();charts.close();server.server_close()


if __name__=='__main__':main()
