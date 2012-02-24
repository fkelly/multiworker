--------------------
Overview
--------------------

Multiworker is a simple Python module for taking code that is designed to be run in a single thread and running it across multiple processes. The primary use-case for the library arises when you have a method in a code base that you would like to easily parallelize without worrying about the code being thread-safe.

The particular motivation for me was a crawler I had written that was originally single-threaded. I wanted a simple library that would allow me to do crawls in parallel easily.

Certain constraints came up:

* I wanted to be able to use instance methods so that ruled out using Pools.
* I didn't want to have to install third-party code (since I'd eventually like to bundle the crawler with multiworker and open-source both); this ruled out using gevent, twisted, and so forth.
* As mentioned above, I didn't want to worry about thread-safety.
* I wanted to be able to take advantage of multiple cores, which made parallel processes desirable.
* For personal, pedagogical reasons, I wanted to use Python's multiprocessing library.

The design of the library is extremely simply. You instantiate a Processor object with your object and the name of the method you want to call. The Processor object then instantiates N workers each of which executes the given method on data it pulls from a queue. It places the results of the method on a results queue.


--------------------
Example
--------------------

Here is a simple example that crawls all the Wikipedia entries for specific years (e.g. http://en.wikipedia.org/woki/1954) and checks to see if the string 'marilyn monroe' is present:

import urllib2
from processor import Processor, DbgProcessor
import logging
base='http://en.wikipedia.org/wiki/%s'
urls=[base % year for year in range(1950,2000)]

class Checker(object):

    def __init__(self):
        # need to set the user agent in order to crawl wikipedia
        self.opener = urllib2.build_opener()
        self.opener.addheaders = [('User-agent', 'Mozilla/5.0')]

    def check(self,url,text):
        infile = self.opener.open(url)
        data=infile.read()
        return url.split("/")[-1], text.lower() in data.lower()

N=5 # we'll use 5 processes
p=Processor(Checker(),'check',N,logfile='/tmp/wiki.log',loglevel=logging.DEBUG)

search_string='marilyn monroe'

# Add the 50 urls to the Processor object's data queue
for url in urls: p.add(url,search_string)
# start crawling
p.start()
while not p.done(): time.sleep(1)
results=p.get_results()[0]
results.sort()
print "Years in which the string '%s' appears: %s" % (search_string, [int(r[0]) for r in results if r[1]])
    

--------------------
Design Considerations
--------------------

I had originally wanted to enable the Processor object to kill off child processes that didn't finish their tasks quickly enough. In the example above, if fetching a Wikipedia page took too long, the Processor object would terminate the child process and replace it with another. I abandoned this approach for two reasons. The documentation on multiprocessing warns that terminating child processes can corrupt queues that they use. It would be possible to work around such a problem by using pipes or the multiprocessing.Manager object. Likewise, I though of a scheme involving a threaded worker in which one thread listens for kill signals and only kills the other thread when it's "sure" that the queue won't be corrupted. Even more compelling was the fact that killing a child process asynchronously while it's executing more or less arbitrary code is simply too dangerous. It's better to require that arbitrary code to be responsible for timing out if necessary.

I had also originally planned to allow for fairly rich communication between parent (Processor) and child processes by exchange of key/value pairs via the Manager object. I decided this simply wasn't necessary and since it adds a fair amount of complexity, I took it out.

Interestingly enough, one of the hardest things to get working was logging. And, indeed, I used Vinay Sajip's code (in the mlogger.py file) to do it. There's nothing conceptually hard about getting logging to work, it's simply that you need to set-up the right handlers in the right places at the right time and if you don't, you get deadlocks. 

Another somewhat non-obvious issue is that child processes won't quit if they've added data to a queue and the data is still in the queue. As a result, I spent some time wondering why these children were hanging around when their run() method had exited. Once I drained the queue, of course, they stopped as expected.

--------------------
Debugging
--------------------

Because I love using the Python debugger, I added some "experimental" debugging code. Basically, if you use DbgProcessor instead of Processor, you can specify a port on which a debugger object will listen for a connection. If you telnet to that port, it drops you into the run() method of one of the workers. It's not perfect, but it's pretty helpful for being able to step through worker code while it's running.
