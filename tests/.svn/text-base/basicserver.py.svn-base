from SocketServer import ThreadingMixIn
from BaseHTTPServer import HTTPServer
import threading
import time
import router

class MultiThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass

from BaseHTTPServer import BaseHTTPRequestHandler
import urlparse


class GetHandler(BaseHTTPRequestHandler):
    
    def do_GET(self):
        """ 3 possibilities:
            /holmes/offset/n - n chars from holmes.txt starting at offset
            /alice/offset/n - n chars from alice.txt starting at offset
            /sleep/n - sleep n seconds return message "slept <n> seconds"
        """
        parsed_path = urlparse.urlparse(self.path)
        try:
            # NB: router is a separate module so that we can import it here
            # and in our test code
            message,sleepsec = router.getmessage(parsed_path.path)
        except:
            self.send_error(404)
            return
        if sleepsec: time.sleep(sleepsec)
        self.send_response(200)
        self.end_headers()
        self.wfile.write(message)
        return

    def log_message(self, format, *args):
        return

if __name__ == '__main__':
    server = MultiThreadedHTTPServer(('localhost', 9080), GetHandler)
    print 'Starting server, use <Ctrl-C> to stop'
    server.serve_forever()
