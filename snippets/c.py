#!/usr/bin/env python

import sys, socket

server_host_name = 'localhost'
server_port_number = 2345
if len(sys.argv) == 2:
	server_host_name = sys.argv[1]
if len(sys.argv) == 3:
	server_host_name = sys.argv[1]
	server_port_number = int(sys.argv[2])
server_address = (server_host_name, server_port_number)

#sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
sock.connect(server_address)
request = 'show'
print "request: "+request
sock.sendall(request)

#reply = sock.recv(1024)
#print "reply:   "+reply

#bufsize=1
bufsize=1024
reply = ''
while True:
	piece = sock.recv(bufsize)
	if len(piece) == 0: # server-side socket close
		break
	reply += piece
print "reply:   " + reply
