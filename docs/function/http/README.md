# HTTP Functions

See [HTTP SQL Functions](../../http.md) for configuration and shared contracts.

| Function | Signature |
| --- | --- |
| [`HTTP_REQUEST`](http_request.md) | `HTTP_REQUEST(method, url [, options])` |
| [`HTTP_GET`](http_get.md) | `HTTP_GET(url [, options])` |
| [`HTTP_HEAD`](http_head.md) | `HTTP_HEAD(url [, options])` |
| [`HTTP_OPTIONS`](http_options.md) | `HTTP_OPTIONS(url [, options])` |
| [`HTTP_DELETE`](http_delete.md) | `HTTP_DELETE(url [, options])` |
| [`HTTP_POST`](http_post.md) | `HTTP_POST(url, body [, options])` |
| [`HTTP_PUT`](http_put.md) | `HTTP_PUT(url, body [, options])` |
| [`HTTP_PATCH`](http_patch.md) | `HTTP_PATCH(url, body [, options])` |


## Local example setup

The function-page examples use an isolated local HTTP server. The default mydb
policy denies loopback, so prepare both a test service and a node policy. The
following files and database state are temporary and do not use external endpoints.

Create the parent directory with `mkdir -p /tmp/mydb/function-docs`, then write
`/tmp/mydb/function-docs/http-local.json`:

```json
{
  "version": 1,
  "egress": {
    "allowed_origins": ["http://127.0.0.1:8080"],
    "allow_plaintext_origins": ["http://127.0.0.1:8080"],
    "private_destinations": ["127.0.0.1/32"]
  }
}
```

Run this Python 3 test service in a separate terminal:

```python
from http.server import BaseHTTPRequestHandler, HTTPServer
import json

class Handler(BaseHTTPRequestHandler):
    def respond(self):
        self.rfile.read(int(self.headers.get("Content-Length", "0")))
        payload = json.dumps({"ok": True, "method": self.command}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    do_GET = do_POST = do_PUT = do_PATCH = respond
    do_DELETE = do_OPTIONS = do_HEAD = respond

    def log_message(self, *args):
        pass

HTTPServer(("127.0.0.1", 8080), Handler).serve_forever()
```

Start an isolated mydb with the policy:

```bash
mkdir -p /tmp/mydb/function-docs
cargo run -- --host 127.0.0.1 --port 4837 \
  --data-dir /tmp/mydb/function-docs/data \
  --http-local-config /tmp/mydb/function-docs/http-local.json
```

Use bootstrap root, or grant the test caller global HTTP_USE through an authorized
administrator. Connect to this isolated server and run the function examples.
GET/POST/PUT/PATCH/DELETE/OPTIONS produce a response envelope with status 200 and
a UTF-8 JSON body such as `{"ok":true,"method":"GET"}`. HEAD returns an empty body.
The test server does not implement authentication, redirects or application storage;
its only purpose is to make the documented request/response examples reproducible.
Stop both processes after testing. Full policy and SQL execution rules remain in
[HTTP SQL Functions](../../http.md).
