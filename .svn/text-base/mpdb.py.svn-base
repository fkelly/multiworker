__doc__="""
This code is based on code found here:

http://snippets.dzone.com/posts/show/7248

"""

import pdb
import socket
import sys

class Mpdb(pdb.Pdb):
    def __init__(self, port=4000, sockettimeout=10):
        """ Open a socket on the given port and listen for an incoming
            connection timeout after the given number of seconds (or
            never if timeout is None/0). Once a connection is established
            (normally via telnet), map a Pdb object's stdin/out to the
            socket.
        """
        self.port=port
        self.old_stdout = sys.stdout
        self.old_stdin = sys.stdin
        self.skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
        self.skt.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.skt.bind((socket.gethostname(), self.port))
        if sockettimeout: 
            self.skt.settimeout(sockettimeout)
        self.skt.listen(1)
        (clientsocket, address) = self.skt.accept()
        handle = clientsocket.makefile('rw')
        pdb.Pdb.__init__(self, completekey='tab', stdin=handle, stdout=handle)
        sys.stdout = sys.stdin = handle
 
    def __repr__(self):
        return "Mpdb object listening on port %s (%s)" % (self.port,self.skt) 
   
    def do_continue(self, arg):
        """ Closes the socket on the 'continue' command """
        sys.stdout = self.old_stdout
        sys.stdin = self.old_stdin
        self.skt.close()
        self.set_continue()
        return 1

    do_c = do_cont = do_continue
