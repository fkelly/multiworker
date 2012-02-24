import urllib2
from processor import Processor, DbgProcessor
import logging
base='http://en.wikipedia.org/wiki/%s'
urls=[base % year for year in range(1950,2000)]

class Checker(object):

    def __init__(self):
        self.opener = urllib2.build_opener()
        self.opener.addheaders = [('User-agent', 'Mozilla/5.0')]

    def check(self,url,text):
        infile = self.opener.open(url)
        data=infile.read()
        return url.split("/")[-1], text.lower() in data.lower()

N=5
p=Processor(Checker(),'check',N,logfile='/tmp/wiki.log',loglevel=logging.DEBUG)

search_string='marilyn monroe'
for url in urls: p.add(url,search_string)
p.start()
while not p.done(): time.sleep(1)
results=p.get_results()[0]
results.sort()
print "Years in which the string '%s' appears: %s" % (search_string, [int(r[0]) for r in results if r[1]])
    
