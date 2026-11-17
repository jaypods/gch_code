"""Single-origin connection reuse without retries, redirects or netrc auth.

Only the configured OS/environment proxy is used; no alternate host/egress.
Adapter accepts urllib Requests to preserve the transport's exact signed bytes.
The response interface keeps bounded reads and existing sanitized error handling.
"""
import io
import urllib.error
import urllib.parse
import urllib.request
import requests


class Response:
    def __init__(self,response):
        self.response=response;self.status=response.status_code;self.headers=response.headers
    def __enter__(self):return self
    def __exit__(self,*args):self.response.close()
    def geturl(self):return self.response.url
    def read(self,size):return self.response.raw.read(size,decode_content=True)


class PersistentHTTPS:
    ORIGIN='https://fapi.binance.com'
    def __init__(self,*,session=None,origin='https://fapi.binance.com'):
        if origin not in ('https://fapi.binance.com','https://api.binance.com'):
            raise ValueError('HTTP_ORIGIN_REFUSED')
        self.ORIGIN=origin;self.host=urllib.parse.urlsplit(origin).netloc
        self.session=session or requests.Session()
        self.session.trust_env=False  # Never add netrc credentials implicitly.
        self.session.auth=None
        self.session.mount(self.ORIGIN+'/',requests.adapters.HTTPAdapter(
            pool_connections=1,pool_maxsize=1,max_retries=0,pool_block=True))
        configured=urllib.request.getproxies()
        self.proxies={} if urllib.request.proxy_bypass(self.host) else {
            k:v for k,v in configured.items() if k in ('http','https','all')}
        self.closed=False

    def open(self,request,timeout,*,before_attempt=None):
        if self.closed:raise OSError('HTTP_CONNECTION_CLOSED')
        parsed=urllib.parse.urlsplit(request.full_url)
        if (parsed.scheme!='https' or parsed.netloc!=self.host or parsed.fragment
                or request.get_method() not in ('GET','POST','PUT','DELETE')):
            raise OSError('HTTP_ORIGIN_OR_METHOD_REFUSED')
        self.session.cookies.clear()
        prepared=self.session.prepare_request(requests.Request(request.get_method(),request.full_url,
            headers=dict(request.header_items()),data=request.data))
        if prepared.url!=request.full_url or prepared.body!=request.data:
            raise OSError('HTTP_ENCODING_CHANGED')
        if before_attempt is not None:before_attempt()
        response=self.session.send(prepared,timeout=timeout,stream=True,allow_redirects=False,
                                   verify=True,proxies=self.proxies)
        if response.status_code>=400:
            try:body=response.raw.read(8192,decode_content=True)
            finally:response.close()
            raise urllib.error.HTTPError(request.full_url,response.status_code,'HTTP_RESPONSE_REJECTED',
                                         response.headers,io.BytesIO(body))
        return Response(response)

    def close(self):
        if not self.closed:self.session.close();self.closed=True
