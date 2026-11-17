"""OS-held single-owner lock; a stale file is never treated as a live lock."""
import os
from pathlib import Path
from .account import InvalidState


class OwnerLock:
    def __init__(self,path):self.path=Path(path).resolve();self.stream=None

    def acquire(self):
        if self.stream is not None:raise InvalidState('本实例已持有运行锁')
        self.path.parent.mkdir(parents=True,exist_ok=True)
        stream=self.path.open('a+b')
        try:
            stream.seek(0,2)
            if stream.tell()==0:stream.write(b'0');stream.flush()
            stream.seek(0)
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(stream.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError:
            stream.close()
            raise InvalidState('已有运行进程持有此账户目录；不能重复启动') from None
        self.stream=stream
        return self

    def close(self):
        stream,self.stream=self.stream,None
        if stream is None:return
        try:
            stream.seek(0)
            if os.name=='nt':
                import msvcrt
                msvcrt.locking(stream.fileno(),msvcrt.LK_UNLCK,1)
            else:
                import fcntl
                fcntl.flock(stream.fileno(),fcntl.LOCK_UN)
        finally:stream.close()

    def __enter__(self):return self.acquire()
    def __exit__(self,*args):self.close()
