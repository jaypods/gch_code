"""Read the existing current-user Windows credential; never print its values."""
from dataclasses import dataclass, field
from .account import InvalidState


@dataclass(frozen=True)
class SecretPair:
    api_key: str = field(repr=False)
    api_secret: str = field(repr=False)

    def __repr__(self):
        return "SecretPair(<masked>)"


def load_local_credentials():
    import ctypes
    import ctypes.wintypes as W
    import json
    import os
    from credential_store import PCREDENTIAL, TARGET, validate_pair
    if os.name != "nt":
        raise InvalidState("此凭据入口仅支持当前 Windows 用户")
    api = ctypes.WinDLL("Advapi32.dll", use_last_error=True)
    api.CredReadW.argtypes = [W.LPCWSTR, W.DWORD, W.DWORD, ctypes.POINTER(PCREDENTIAL)]
    api.CredReadW.restype = W.BOOL
    api.CredFree.argtypes = [ctypes.c_void_p]
    api.CredFree.restype = None
    ptr = PCREDENTIAL()
    if not api.CredReadW(TARGET, 1, 0, ctypes.byref(ptr)):
        raise InvalidState("未能读取本机已保存的 API 配置")
    try:
        size = ptr.contents.CredentialBlobSize
        if not 0 < size <= 2560:
            raise InvalidState("本机 API 配置格式不合法")
        blob = json.loads(ctypes.string_at(ptr.contents.CredentialBlob, size))
        if blob.get("version") != 1 or blob.get("key_type") != "HMAC":
            raise InvalidState("本机 API 配置版本不受支持")
        key, secret = validate_pair(blob["api_key"], blob["api_secret"])
        return SecretPair(key, secret)
    except Exception:
        raise InvalidState("本机 API 配置无法解码或格式不合法") from None
    finally:
        api.CredFree(ptr)
