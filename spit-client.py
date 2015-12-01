#!/usr/bin/env python

import sys, socket, getopt

DEFAULT_SERVER_HOST_NAME   = 'localhost'
DEFAULT_SERVER_PORT_NUMBER = 2345

# ================================================================
def usage(ostream):
	a = sys.argv[0]
	string = \
"""Usage: %s [options] ask
Or:    %s [options] show
Or:    %s [options] output      {DKVP text}
Or:    %s [options] stats       {DKVP text}
Or:    %s [options] mark-done   {task_id}
Or:    %s [options] mark-failed {task_id}

Options:
-s {server host name} Defaults to %s
-p {server port number} Defaults to %d
""" % (a,a,a,a,a,a,DEFAULT_SERVER_HOST_NAME,DEFAULT_SERVER_PORT_NUMBER)
	ostream.write(string)

# ----------------------------------------------------------------
def main():
	server_host_name = DEFAULT_SERVER_HOST_NAME
	server_port_number = DEFAULT_SERVER_PORT_NUMBER

	try:
		optargs, non_option_args = getopt.getopt(sys.argv[1:], "s:p:h", ['help'])
	except getopt.GetoptError, err:
		print >> sys.stderr, str(err)
		usage(sys.stderr)
		sys.exit(1);

	for opt, arg in optargs:
	    if opt == '-s':
			server_host_name = arg
	    elif opt == '-p':
			server_port_number = int(arg)
	    elif opt == '-h':
			usage(sys.stdout)
			sys.exit(0)
	    elif opt == '--help':
			usage(sys.stdout)
			sys.exit(0)
	    else:
			print >> sys.stderr, "Unhandled option \"%s\"." % opt
			sys.exit(1)

	non_option_arg_count = len(non_option_args)
	if non_option_arg_count < 1:
		usage(sys.stderr)
		sys.exit(1)
	verb = non_option_args[0]
	non_option_args = non_option_args[1:]
	non_option_arg_count = len(non_option_args)

	client = SpitClient(server_host_name, server_port_number)
	if verb == 'ask':
		if non_option_arg_count != 0:
			usage(sys.stderr)
			sys.exit(1)
		task_id = client.ask("wreq:")
		print task_id
	elif verb == 'show':
		if non_option_arg_count != 0:
			usage(sys.stderr)
			sys.exit(1)

		output = client.ask("show:")
		print output
	elif verb == 'output':
		text = " ".join(non_option_args)
		client.send("output:"+text)
	elif verb == 'stats':
		text = " ".join(non_option_args)
		client.send("stats:"+text)
	elif verb == 'mark-done':
		if non_option_arg_count != 1:
			usage(sys.stderr)
			sys.exit(1)
		task_id = non_option_args[0]
		client.send("mark-done:"+task_id)
	elif verb == 'mark-failed':
		if non_option_arg_count != 1:
			usage(sys.stderr)
			sys.exit(1)
		task_id = non_option_args[0]
		client.send("mark-failed:"+task_id)
	else:
		usage(sys.stderr)
		sys.exit(1)


# ================================================================
BUFSIZE=1024
class SpitClient:
	def __init__(self, server_host_name, server_port_number):
		self.server_host_name = server_host_name
		self.server_port_number = server_port_number
		self.server_address = (server_host_name, server_port_number)

	def send(self, msg):
		sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
		sock.connect(self.server_address)
		sock.sendall(msg+"\n")
		sock.close()

	def ask(self, msg):
		sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
		sock.connect(self.server_address)
		sock.sendall(msg+"\n")
		reply = ''
		while True:
			piece = sock.recv(BUFSIZE)
			if len(piece) == 0: # server-side socket close
				break
			reply += piece
		sock.close()
		return reply.rstrip()

# ================================================================
main()

