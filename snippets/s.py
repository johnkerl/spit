#!/usr/bin/env python

import sys, socket

port_number = 2345
if len(sys.argv) == 2:
	port_number = int(sys.argv[1])

#s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind(('', port_number))
s.listen(1)
print 'Listening on port %d' % port_number
while 1:
	conn, addr = s.accept()
	print type(addr)
	print 'Client: %s:%d' % (addr[0], addr[1])
	request = conn.recv(1024)
	if request:
		print "request: " + request
		reply = "s.py:" + request
		print "reply:   " + reply
		conn.sendall(reply)
		conn.close()
