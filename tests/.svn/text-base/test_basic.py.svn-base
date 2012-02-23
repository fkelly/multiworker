import sys
import unittest
import os
import urllib2
import random
import logging
import processor
import router
import time
import signal
import threading
import mlogger
from subprocess import Popen,PIPE
from logging.handlers import RotatingFileHandler

PORT=9080
# log nothing by default
LOGLEVEL=logging.CRITICAL + 1

def factor(n):
    yield 1
    i = 2
    limit = n**0.5
    while i <= limit:
        if n % i == 0: 
            yield i
            n = n / i
            limit = n**0.5
        else:
            i += 1
    if n > 1: yield n

class Factor(object):

    def __init__(self): pass

    def factor(self,n): 
        return tuple([f for f in factor(n)])

class FetchURL(object):

    def __init__(self): pass

    def get(self,url):
        u=urllib2.urlopen(url)
        data=u.read()
        return data

class TestProcessor(unittest.TestCase):
    """
    Test case for processor.py
    """

    def setUp(self):
        """
        Setup method that is called before each unit test.
        """
        self.server_url='http://localhost:%s' % PORT
        self.N=1
        self.logfile='./multi.log'
        # zero out the logfile
        #open(self.logfile,'w').close()

    def tearDown(self):
        """
        Teardown method that is called after each unit test.
        """
        pids=self.leftover_procs()
        #for pid in pids:
        #    os.kill(pid,signal.SIGKILL)

    def leftover_procs(self):
        ps = Popen(["ps", "-ef"], stdout=PIPE)
        out, err = ps.communicate()
        lines = out.split("\n")
        procs= filter(lambda l: l.find("TestProcessor") > -1, lines)
        pids=[]
        this_pid=os.getpid()
        for proc in procs:
            pid=int([x  for x in proc.split(' ') if x][1])
            if pid!=this_pid: pids.append(pid)
        return pids

    def get_refdata(self, qlist):
        messages=[]
        for q in qlist:
            message,sleepsec=router.getmessage(q)
            messages.append(message)
        return messages

    def get_factors(self, nlist):
        factors=[]
        for n in nlist:
            factors.append(tuple([f for f in factor(n)]))
        return factors 
        
        
    def test_process_factor(self):
        N=1000000
        nlist=random.sample(range(N,2*N),5)
        results,failures=self._process_factor(nlist)
        self.assertFalse(failures)
        refdata=self.get_factors(nlist)
        self.assertEquals(set(refdata),set(results))

    def test_process_factor_noblock(self):
        N=1000000
        nlist=random.sample(range(N,2*N),20)
        p=None
        try:
            p=processor.Processor(Factor(),'factor',self.N, block=False,loglevel=LOGLEVEL,logfile=self.logfile)
            for da in nlist:
                p.add(da)
            p.no_more_data()
            p.start()
            while not p.done():
                time.sleep(3)
            results,failures=p.get_results()
            self.assertFalse(failures)
            refdata=self.get_factors(nlist)
            self.assertEquals(set(refdata),set(results))
        finally:
            if p: p.stop()

    def test_process_factor_feed_drain(self):
        """ test feeding the data queue and draining the result queue """
        N=1000000
        p=processor.Processor(Factor(),'factor', self.N, block=False,loglevel=LOGLEVEL)
        # start the Processor object -- data queue is empty
        p.start()
        max_iter=20
        num_ints=20
        try:
            ct=0
            for i in range(0,10):
                nlist=random.sample(range(N,2*N),num_ints)
                ct += len(nlist)
                for n in nlist: p.add(n)
                # data queue now has num_ints in it
                refdata=self.get_factors(nlist)
                count=0
                # the 2 seconds here isn't crucial -- just giving
                # the processes a chance to get some work done
                time.sleep(2)
                # get some initial results
                results,failures=p.get_results()
                #print "Results (1): %s" % results
                # there should NEVER be any failures
                self.assertFalse(failures)
                # if we don't have all the results, keep checking
                while len(results)<len(refdata): 
                    time.sleep(1)
                    results_ext,failures=p.get_results()
                    self.assertFalse(failures)
                    results.extend(results_ext)
                    count += 1 
                    if count>=max_iter: self.fail("Unable to retrive results.")
                self.assertEquals(set(refdata),set(results), "%s\n----\n%s" % (refdata,results))
        finally:
            p.no_more_data()
            count=0
            while not p.done(): 
                count += 1 
                #print "result queue: %s" % p.result_queue.qsize() 
                #print "data queue: %s" % p.data_queue.qsize() 
                #print "# alive: %s" % len(p.get_alive())
                time.sleep(1)
                if count>=max_iter: return 

    def test_process_url(self):
        qlist=['/sleep/1/','/holmes/1000/1000','/alice/1000/1000']
        results,failures=self._process_url(qlist)
        self.assertFalse(failures)
        refdata=self.get_refdata(qlist)
        self.assertEquals(set(refdata),set(results))

    def test_process_url_error(self):
        qlist=['/foo/1']
        results,failures=self._process_url(qlist)
        self.assert_(len(failures)==1)

    def _process_url(self,qlist):
        datalist=['%s%s' % (self.server_url,q) for q in qlist]
        workers=[]
        p=processor.Processor(FetchURL(),'get',self.N,loglevel=LOGLEVEL)
        for da in datalist:
            p.add(da)
        p.no_more_data()
        p.start()
        return p.get_results()

    def _process_factor(self,nlist):
        workers=[]
        #p=processor.DbgProcessor(workers,'factor',loglevel=logging.DEBUG)
        p=processor.Processor(Factor(),'factor',self.N,loglevel=logging.DEBUG,logfile=self.logfile)
        for da in nlist:
            p.add(da)
        p.no_more_data()
        p.start()
        return p.get_results()

    def test_logfile(self):
        """ test over-writing the worker object's log file """
        class ManicLogger(object):
            def __init__(self,logfile):
                self.logger=logging.getLogger()
                hdlr=RotatingFileHandler(logfile)
                fmt = logging.Formatter("%(asctime)s %(module)s %(levelname)-5s - %(message)s")
                hdlr.setFormatter(fmt)
                self.logger.addHandler(hdlr)
            def logalot(self,N):
                """ write out 100 copies of N """
                bigstring=str(N)*100
                self.logger.debug(bigstring)
                return N
        logfile='/tmp/manic.log'
        # make sure the log file is empty
        open(logfile,'w').close()
        p=processor.Processor(ManicLogger('/tmp/out.log'),'logalot',self.N,loglevel=logging.DEBUG,logfile=logfile)
        #p=processor.DbgProcessor(ManicLogger(self.logfile),'logalot',self.N,loglevel=logging.DEBUG,logfile=self.logfile,dbg_port=2300)
        M=5000
        for da in range(0,M):
            p.add(da)
        p.no_more_data()
        p.start()
        # open the logfile and randomly sample it for the strings that should be there
        logdata=open(logfile,'r').read()
        for i in random.sample(range(0,M),10):
            s=str(i)*100
            self.assert_(s in logdata)
       

    # other test cases:
    # N>1

    # other test cases:
    # N>1
    # errors
    # logging
    # processes that take a really long time
    # add method -- p.add(*(da,))
    # need to spin up the server as well


def startHTTPServer():
    from basicserver import MultiThreadedHTTPServer, GetHandler
    server = MultiThreadedHTTPServer(('localhost', PORT), GetHandler)
    dthread = threading.Thread(target=server.serve_forever)
    dthread.setDaemon(True)
    dthread.start()
    return server

if __name__=='__main__': 
    s=None
    try:
        s=startHTTPServer()
        unittest.main()
    finally: 
        # main call sys.exit, so this has to be in 
        # a finally block or http shutdown isn't clean
        if s: s.shutdown()



"""
    def set_data(self,data):
    def get_data(self):
    def time_on_task(self):
    def get_alive(self):
    def get_current_start_time(self,worker):
"""
