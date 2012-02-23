__doc__=="""A worker-pattern class that takes N worker objects has a data queue and a result queue and uses the N workers to process the data in the data queue and add it to the result queue using the multiprocessing module."""

import sys
import multiprocessing
import traceback
import socket
import Queue
import time
import datetime
import logging
import copy
from logging.handlers import RotatingFileHandler

from mlogger import QueueHandler
import mpdb

def getTraceBack(mesg=''):
    """ """
    t,v,tb = sys.exc_info()
    extb = traceback.extract_tb(tb)
    del tb
    tb = traceback.format_list(extb)
    if mesg: tb.insert(0,mesg)
    return tb

class dummyfigleaf(object):

    def start(self): pass
    def stop(self): pass
    def write_coverage(self,f): pass

# If you want to actually run figleaf, uncomment the import
# and comment out the lines following it
#import figleaf
global figleaf
figleaf=dummyfigleaf()


def logqueue_listener(queue,loglevel,logfile):
    """ """
    logger = logging.getLogger()
    for h in logger.handlers: logger.removeHandler(h)
    h = logging.handlers.RotatingFileHandler(logfile)
    # I think processName may be missing in Python < 2.7, so just change this statement
    # if the code breaks on your system
    f = logging.Formatter('%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s')
    h.setFormatter(f)
    logger.addHandler(h)
    logger.setLevel(loglevel)
    while True:
        try:
            record = queue.get()
            if record is None: # We send this as a sentinel to tell the listener to quit.
                break
            logger.handle(record) # No level or filter logic applied - just do it!
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            import sys, traceback
            print >> sys.stderr, 'Problem starting listener process:'
            traceback.print_exc(file=sys.stderr)


class StopException(Exception): pass

SENTINEL=(None,None)

_CST='current_start_time'
_DATA='data'

class Result(object):
    """ Wrapper class used to return results from a worker's operation.
        The value of this is that normal results as well as exceptions can be returned.
    """

    result=None
    tb=None
    errormsg=None
    data=None
    error=False

    def __init__(self,result,data=None):
        self.result=result
        self.data=data


class ErrorResult(Result):

    error=True
    def __init__(self,errormsg,tb,data=None):
        self.errormsg=errormsg
        self.tb=tb
        self.data=data


# TODO: it would be nice to allow this logger to more general
def addFileHandler(logger,logfile):
    # remove existing handlers
    for h in logger.handlers: logger.removeHandler(h)
    hdlr=RotatingFileHandler(logfile)
    fmt = logging.Formatter("%(asctime)s %(module)s %(levelname)-5s - %(message)s")
    hdlr.setFormatter(fmt)
    # Need to make sure this is the one and only time we're instantiating MultiHandler
    #logger.addHandler(mlogger.MultiHandler(logfile))
    logger.addHandler(hdlr)

class Worker(multiprocessing.Process):

    def __init__(self,method,data_queue,result_queue,debugger=0,logq=None,loglevel=None): 
        """ 
            method: a function
            data_queue: a queue containing the data to be processed
            result_queue: a queue into which the results of applying
            method to the processed data will be placed
        """
        # base class initialization
        multiprocessing.Process.__init__(self)
        self.data_queue=data_queue
        self.result_queue=result_queue
        self.method=method
        self.stopped=False
        self.debugger=debugger
        self.logq=logq
        self.loglevel=loglevel
        figleaf.start()

    def config_logging(self):
        """ Set-up a QueueHandler for logging for this worker """
        self.logger = logging.getLogger()
        if not self.logq: return
        for h in self.logger.handlers: self.logger.removeHandler(h)
        h = QueueHandler(self.logq)
        self.logger.addHandler(h)
        self.logger.setLevel(self.loglevel) 
        
    def stop(self):
        self.stopped=True
   
    def run(self):
        # Wait until you get here to configure logging so that you're configuring
        # it in the actual child process
        self.config_logging()
        while not self.stopped:
            self.logger.debug('%s: %s' % (self.name,self.data_queue.qsize()))
            try:
                self.logger.debug('%s: fetching data' % (self.name))
                data = self.data_queue.get()
                self.logger.debug('%s: got data - %s' % (self.name, str(data)))
                if data==SENTINEL:
                    self.logger.debug('%s: got sentinel' % (self.name))
                    self.stopped=True
                    break
            except Queue.Empty:
                self.logger.debug('%s: queue is empty.' % (self.name))
                break
            # the actual processing
            try:
                args=data[0]
                kwargs=data[1]
                result=apply(self.method,args,kwargs)
                result=Result(result)
            except Exception,e:
                trace=getTraceBack()
                tbmsg='%s: %s\n' % (self.name, str(e)) + '\n'.join(trace) 
                self.logger.error(tbmsg)
                result=ErrorResult(str(e),trace,data=data)
            finally:
                self.data_queue.task_done()
                self.result_queue.put(result)
            # store the result
        self.logger.debug('%s: loop is done.' % (self.name))
        self.stop()
        figleaf.stop()
        figleaf.write_coverage('.figleaf')
        self.logger.debug('%s: returning.' % (self.name))
        return


class Processor(object):

    def __init__(self,worker,methodname, N, block=True,
                 monitor_delay=2, max_time_on_task=60, loglevel=1000, logfile=None): 
        """ Constructor takes a list of worker objects and a methodname to call
            on the objects. The optional monitor_delay parameter is the number of seconds
            between calls to the monitor() method -- at this point the monitor method
            simply logs information about the the worker processes
        """
        if not worker: raise ValueError("Worker parameter must not be None.")
        self.data_queue = multiprocessing.JoinableQueue()
        self.result_queue = multiprocessing.JoinableQueue()
        # a priori we don't care so much about the objects or the methodname, we care
        # about the underlying bound function, so we create a list of those bound
        # functions
        self.workers=[]
        self.loglevel=loglevel
        if logfile: 
            self.config_logging(worker,loglevel,logfile)
        else:
            self.logq=None
            self.listener=None
            self.logger=logging.getLogger()
        # try to over-write the worker object's logger
        worker.logger=self.logger
        for i in range(0,N):
            f=getattr(worker,methodname)
            self.addWorker(f)
        self.stopped=False
        self.monitor_delay=monitor_delay
        self.block=block
        self.max_time_on_task=max_time_on_task
        self._results_buffer=[]
        self._failures_buffer=[]
        

    def config_logging(self,worker,loglevel,logfile):
        """ Start up the logqueue_listener process to listen for logging records
            passed into the logging queue by the workers.
            TODO: the main process's logging needs to also use a QueueHandler
        """
        # create a queue that will be shared by all loggers across
        # all processes
        self.logq=multiprocessing.Queue(-1)
        self.listener = multiprocessing.Process(target=logqueue_listener,
                                       args=(self.logq, loglevel, logfile))
        self.listener.start()
        self.logger = logging.getLogger()

    def addWorker(self,method):
        """ Create a new worker object and add it to the list of workers """
        worker_obj=Worker(method,self.data_queue,self.result_queue,
            logq=self.logq,loglevel=self.loglevel)
        self.workers.append(worker_obj)
        return worker_obj

    def start(self):
        """ As expected, this starts the workers. There are 3 possible
            modes of execution:
            (1) start the workers and monitor their progress (effectively
                blocking in the process)
            (2) start the workers and block until they are done
            (3) do not block
        """
        try:
            for w in self.workers: 
                w.start()
            if self.block and self.monitor_delay>0: 
                self.monitor()
                self._stop_logging()
            elif self.block:
                self.data_queue.join()
                self._stop_logging()
            else: return
        except KeyboardInterrupt:
            # TODO: not sure if this is the way I want to handle this....
            self.stop_all()
            alive=self.get_alive()
            self.logger.info("Received ctrl-c. There are currently: %s processes alive." % len(alive))
            sys.exit(1)

    def stop_all(self):
        """ This method brutally stops all child processes -- not recommended """
        self.logger.info("Stopping all processes.")
        for w in self.workers:
            w.terminate()
            w.join()

    def get_alive(self):
        """ Returns a list of worker processes that are still alive """
        return [w for w in self.workers if w.is_alive()]

    def monitor(self):
        """ monitor() is responsible for polling the system to check the status of workers,
        """ 
        while 1:
            if self.data_queue.qsize()==0:
                self._collect_results()
            alive=self.get_alive()
            for w in self.workers:
                if not w.is_alive(): w.join()
            self.logger.debug("There are currently: %s processes alive." % len(alive))
            self.logger.debug("There are %s items in the queue." % self.data_queue.qsize())
            if not alive: return 
            time.sleep(self.monitor_delay)
         
    def add(self, *args, **kwargs):
        """ adds data to the data queue for processing """
        if self.stopped: raise StopException("Processor has been stopped.")
        data=[args,kwargs]
        self.data_queue.put(data)

    def stop(self): 
        """ If the main process is started in non-blocking mode, stop() should
            be called after done() returns True """
        self.stopped=True
        self._stop_logging()

    def no_more_data(self):
        """ signals that no more data will be added to the queue
            the data processing should stop when the queue is empty -
        """
        for i in range(0,len(self.workers)):
            self.data_queue.put(SENTINEL)
        self.stopped=True

    def __del__(self):
        self._stop_logging()

    def _stop_logging(self):
        if self.logq: 
            self.logq.put_nowait(None)
        if self.listener: 
            self.listener.join()
        self.logq=None
        self.listener=None

    def done(self):
        """ Returns True if the Processor object has completed processing the data in
            queue: this means that the queue is empty and the calling method has called
            stop().
        """
        return self.stopped and (self.data_queue.empty()) and not len(self.get_alive())


    @property
    def result_count(self):
        return self.result_queue.qsize()

    def _collect_results(self):
        """ Collect the results of processing. Returns two lists:
            results, which contains the "normal" list of results
            failures, which contains a list of ErrorResult objects
            cataloging the cases were workers failed to process the data 
            correctly.
            @TODO: do I need to check self.stopped?
            @TODO: this might be better with yield
        """ 
        results=[]
        failures=[]
        count=0
        while not self.result_queue.empty():
            result=self.result_queue.get()
            if result.error:
                failures.append(result)
            else:
                results.append(result.result)
            count+=1
        self.logger.debug("Retrieved %s items from queue." % count)
        self._results_buffer.extend(results)
        self._failures_buffer.extend(failures)
            
        
    def get_results(self):
        self._collect_results()
        # copy the contents of the buffers
        results=copy.copy(self._results_buffer)
        failures=copy.copy(self._failures_buffer)
        self._results_buffer=[]
        self._failures_buffer=[]
        return results, failures
        
""" Experimental debugger friendly code:

    In order to provide simple debugging of multiprocessing code, we can
    instantiate a DbgProcessor object instead of a simple Processor object.
    The DbgProcessor object takes an optional additional dbg_port parameter
    that specifies the port on which the debugger will listen.

    Suppose we instantiate a DbgProcessor object with dbg_port=6789,
    then when the DbgProcessor creates its worker objects, it creates one
    of the objects as a DbgWorker object rather than a simple Worker object.
    When that DbgWorker object's run() method is called it will instantiate
    an Mpdb object, which will listen on port specified by dbg_port, 6789 in 
    this example. 

    If the user telnet's to 6789 within a specified number of seconds (10 by
    default) after the Mpdb object is created, then the Mpdb object will create
    a remote Pdb session for that one worker object. The user can then
    use the Pdb session via telnet. The user can end the session by using
    continue command, which will close the socket.

    This code should be considered experimental!

    Some possible future directions:
    * allow multiple ports for multiple workers so that one can choose which
      worker to debug
    * allow for break points within the method executed in Worker.run() - right
      now the break point is fixed right before the run() method is executed

"""
   
DBG_PORT=4000
 
class DbgProcessor(Processor):

    def __init__(self, workers, methodname, N, block=True, monitor_delay=2, 
                 max_time_on_task=60, loglevel=1000, dbg_port=DBG_PORT, logfile=None): 
        self.dbg_port=dbg_port
        super(DbgProcessor,self).__init__(workers,methodname, N, block=block,
              monitor_delay=monitor_delay,max_time_on_task=max_time_on_task,
              loglevel=loglevel,logfile=logfile)
      
    def addWorker(self,method):
        debugger=0
        # set up a debugger for only 1 worker
        if not any([hasattr(w,'debugger') and w.debugger for w in self.workers]):
            worker_obj=DbgWorker(method,self.data_queue,self.result_queue,
                self.dbg_port,debugger=1,logq=self.logq,loglevel=self.loglevel)
        else:
            worker_obj=Worker(method,self.data_queue,self.result_queue, 
                logq=self.logq,loglevel=self.loglevel)
        self.workers.append(worker_obj)
        return worker_obj
        
class DummyPdb(object):
    def set_trace(self): pass

class DbgWorker(Worker):

    _pdb = DummyPdb()

    def __init__(self,method,data_queue,result_queue,dbg_port,debugger=0,logq=None, loglevel=None): 
        self.dbg_port=dbg_port
        super(DbgWorker,self).__init__(method,data_queue,result_queue,debugger=debugger,
          logq=logq,loglevel=loglevel)

    def _create_mpdb(self):
        """ If this worker is to be equipped with a debugger (self.debugger=1), then
            create the Mpdb object and wait for a connection on the port. If the
            listening socket times out, no debugger object is created.
        """
        if not self.debugger: return
        try: self._pdb=mpdb.Mpdb(port=self.dbg_port,logger=None)
        except socket.timeout: pass

    def run(self):
        """ This is just like the Worker's run method, except that if self.debugger=1,
            we create an Mpdb debugger object and, if someone telnets to the appropriate
            port, we jump into a debugger session just before exiting the super class's
            run() method.
        """
        self._create_mpdb()
        self._pdb.set_trace()
        super(DbgWorker,self).run()
