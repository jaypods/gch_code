"""Local Windows credential storage. No network, plaintext files or secret logs.

Only the user-operated configuration window calls save(). The web server only
calls metadata(), which never dereferences or returns a credential blob.
"""
import ctypes
from ctypes import wintypes as W
import json
import os
import re

TARGET = 'C801/Binance/Futures/HMAC/v1'


class CredentialError(RuntimeError):
    pass


class Credential(ctypes.Structure):
    _fields_ = [('Flags', W.DWORD), ('Type', W.DWORD), ('TargetName', W.LPWSTR),
                ('Comment', W.LPWSTR), ('LastWritten', W.FILETIME),
                ('CredentialBlobSize', W.DWORD), ('CredentialBlob', ctypes.POINTER(W.BYTE)),
                ('Persist', W.DWORD), ('AttributeCount', W.DWORD), ('Attributes', ctypes.c_void_p),
                ('TargetAlias', W.LPWSTR), ('UserName', W.LPWSTR)]


PCREDENTIAL = ctypes.POINTER(Credential)


COPY_FORMATTING = frozenset('\u200b\u200e\u200f\u2060\ufeff')


def normalize_field(value):
    # Remove only copy/layout whitespace. Never guess OCR substitutions (I/l/1,
    # O/0), case, punctuation, truncated characters, or key type.
    return ''.join(c for c in value if not c.isspace() and c not in COPY_FORMATTING)


def validate_field(value, label):
    if '-----BEGIN' in value or value.lstrip().startswith(('ssh-rsa ', 'ssh-ed25519 ')):
        raise CredentialError(f'{label} 看起来是私钥文件或 SSH 密钥。此入口只保存币安系统生成的 HMAC 密钥。')
    normalized = normalize_field(value)
    if not normalized:
        raise CredentialError(f'{label} 为空，请使用币安对应字段的“复制”按钮。')
    if not 32 <= len(normalized) <= 256:
        raise CredentialError(f'{label} 清理空白后为 {len(normalized)} 个字符，长度不在本入口支持范围内。请复制完整字段，不要使用截图文字识别。')
    # Credential values are opaque. Common base64/base64url characters must
    # not be mislabeled as RSA/Ed25519. Local format checks are not validation
    # of an actual Binance account or permission.
    if not re.fullmatch(r'[A-Za-z0-9+/=_-]+', normalized):
        raise CredentialError(f'{label} 含有额外符号或非英文字符。请只复制字段本身，不带标签或引号；程序不会猜改字母和数字。')
    return normalized


def validation_summary(key, secret):
    summaries = []
    for value, label in ((key, 'API Key'), (secret, 'Secret Key')):
        try:
            normalized = validate_field(value, label)
            extra = '，保存时清理空白' if normalized != value else ''
            summaries.append(f'{label}：{len(normalized)} 字符，格式通过{extra}')
        except CredentialError:
            summaries.append(f'{label}：待填写' if not normalize_field(value)
                             else f'{label}：格式需核对（保存时显示原因）')
    return '\n'.join(summaries)


def validate_pair(key, secret):
    key, secret = validate_field(key, 'API Key'), validate_field(secret, 'Secret Key')
    if key == secret:
        raise CredentialError('两个字段不能相同，请核对 API Key 和 Secret Key。')
    return key, secret


class WindowsCredentialStore:
    def __init__(self, api=None):
        if api is not None:
            self.api = api  # isolated fake API for tests; never writes a real test credential
            return
        if os.name != 'nt':
            raise CredentialError('本机配置入口仅支持 Windows。')
        self.api = ctypes.WinDLL('Advapi32.dll', use_last_error=True)
        self.api.CredWriteW.argtypes = [PCREDENTIAL, W.DWORD]
        self.api.CredWriteW.restype = W.BOOL
        self.api.CredEnumerateW.argtypes = [W.LPCWSTR, W.DWORD, ctypes.POINTER(W.DWORD),
                                          ctypes.POINTER(ctypes.POINTER(PCREDENTIAL))]
        self.api.CredEnumerateW.restype = W.BOOL
        self.api.CredFree.argtypes = [ctypes.c_void_p]
        self.api.CredFree.restype = None

    def metadata(self):
        count = W.DWORD()
        credentials = ctypes.POINTER(PCREDENTIAL)()
        if not self.api.CredEnumerateW(TARGET, 0, ctypes.byref(count), ctypes.byref(credentials)):
            if ctypes.get_last_error() == 1168:  # ERROR_NOT_FOUND
                return {'configured': False, 'storage': 'Windows Credential Manager', 'permissions_verified': False}
            raise CredentialError('无法读取本机配置状态；未读取或显示密钥。')
        try:
            present = any(credentials[n].contents.TargetName == TARGET and credentials[n].contents.Type == 1
                          for n in range(count.value))
            return {'configured': present, 'storage': 'Windows Credential Manager', 'permissions_verified': False}
        finally:
            self.api.CredFree(credentials)

    def save(self, key, secret):
        key, secret = validate_pair(key, secret)
        blob = json.dumps({'version': 1, 'key_type': 'HMAC', 'api_key': key, 'api_secret': secret},
                          separators=(',', ':')).encode('utf-8')
        if len(blob) > 2560:
            raise CredentialError('密钥内容超过本地存储限制。')
        buffer = (W.BYTE * len(blob)).from_buffer_copy(blob)
        cred = Credential()
        cred.Type = 1  # CRED_TYPE_GENERIC
        cred.TargetName = TARGET
        cred.Comment = 'C801 local configuration; storing does not enable trading'
        cred.CredentialBlobSize = len(blob)
        cred.CredentialBlob = ctypes.cast(buffer, ctypes.POINTER(W.BYTE))
        cred.Persist = 2  # Current Windows user on this machine; not roaming
        cred.UserName = 'C801'
        try:
            if not self.api.CredWriteW(ctypes.byref(cred), 0):
                raise CredentialError('Windows 未能保存凭据；未写入项目文件，请检查当前 Windows 登录状态。')
        finally:
            ctypes.memset(buffer, 0, len(buffer))
        # Do not return a key, masked key suffix, credential blob or exception data.
        return {'configured': True, 'permissions_verified': False}


def configuration_status():
    try:
        return WindowsCredentialStore().metadata()
    except (CredentialError, OSError):
        return {'configured': False, 'storage': 'unavailable', 'permissions_verified': False}
