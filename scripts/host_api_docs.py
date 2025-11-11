#!/usr/bin/env python3

from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler, test
import os  # type: ignore[attr-defined]


class CORSRequestHandler(SimpleHTTPRequestHandler):
    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        SimpleHTTPRequestHandler.end_headers(self)


if __name__ == "__main__":
    directory = "doc/sphinx/build/html"
    handler = partial(CORSRequestHandler, directory=directory)

    with HTTPServer(("", 8000), handler) as httpd:
        print(
            f"Serving HTTP on port 8000 from directory: {os.path.abspath(directory)} at http://{httpd.server_address[0]}:{httpd.server_address[1]}"
        )
        httpd.serve_forever()
