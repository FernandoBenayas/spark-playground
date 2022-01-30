import argparse
from datetime import datetime, timezone, timedelta
from http import server
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from random import randint


class DataPoint:
    """Simple datapoint object"""
    def __init__(self):
        self.value = randint(0, 1000)
        self.timestamp = datetime.now(timezone.utc)
        self.tags = {}

    def __init__(self, value, timestamp, tags):
        if isinstance(value, int) and isinstance(timestamp, datetime):
            self.value = value
            self.timestamp = timestamp
            self.tags = tags
        else:
            raise TypeError("Wrong argument type - value must be int, timestamp must be datetime")
    
    def toJSON(self):
        return {self.timestamp.strftime("%Y-%m-%dT%H:%M:%S"): self.value}



class DataServer(BaseHTTPRequestHandler):
    """Simple data server"""
    def _set_headers(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

    def do_HEAD(self):
        self._set_headers()
    
    def do_GET(self):
        self._set_headers()
        self.wfile.write(bytes(json.dumps(_datapoint_list(entries = 5)), "utf-8"))


def _datapoint_list(entries = 5) -> list:
    if entries < 1:
        raise TypeError("Argument must be an integer higher than 1")
    datapoint_list = []
    for n in range(entries):
        datapoint_list.append(DataPoint(randint(0, 1000), datetime.utcnow() - timedelta(seconds=n), {}).toJSON())
    
    return datapoint_list

def run(server_class = HTTPServer, handler_class = DataServer, host = "localhost", port = 8080):
    httpd = server_class((host, port), handler_class)
    print("Starting httpd on port %d..." % port)
    httpd.serve_forever()

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description = "Run a simple data server",
        epilog = "This script just implements a temporary solution to the lack of a data endpoint"
    )
    parser.add_argument(
        "-p", "--port",
        type = int,
        default = 9090,
        required = False,
        help = "Port where the server will be listening",
    )
    parser.add_argument(
        "-s", "--server",
        type = str,
        default = "dataserver",
        required = False,
        help = "Hostname for the server"
    )
    args = parser.parse_args()
    run(host = args.server, port = args.port)
