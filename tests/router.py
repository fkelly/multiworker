global BUFFERS
BUFFERS={}
BUFFERS['holmes']=open('./holmes.txt').read()
BUFFERS['alice']=open('./alice.txt').read()

def serve_from_buffer(name,offset,n):
    """ serves n bytes from named buffer starting at offset """
    return BUFFERS[name][offset:offset+n]


def getmessage(path):
    """ returns the message to return and the number of seconds to sleep """
    path_elements = [p for p in path.split("/") if p]
    if path_elements[0] in ['holmes','alice']:
        offset=int(path_elements[1])
        n=int(path_elements[2])
        message=serve_from_buffer(path_elements[0],offset,n) + '\r\n'
        return message,0
    elif path_elements[0]=='sleep':
        n=int(path_elements[1])
        message='slept %s seconds\r\n' % n
        return message,n
    else:
        raise Exception("Unknown path")

