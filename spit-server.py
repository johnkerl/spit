#!/usr/bin/env python

# xxx to do:
# * estdone file; and/or just do the estimation inline (in-mem -- don't try to regress over restarts)
# * note client handle somehow inside op=give -- hostname at least
# * work client self-identified hostname & worker_id (no server-side dns lookups) into the protocol
# * open/'a' -> persistent handle w/ flush

import sys, os, time, socket, getopt

DEFAULT_PORT_NUMBER = 2345

# ================================================================
def usage(ostream):
	string = """Usage: %s [options]

Options:
-p {port number}   Defaults to %d if omitted.
-i {infile}        Defaults to stdin.
-o {outfile}       Defaults to stdout.
-d {donefile}      Required.

The workfile should contain work IDs, one per line.  What this means is up to
the client; nominally they will be program arguments to be executed by worker
programs.
""" % (sys.argv[0], DEFAULT_PORT_NUMBER)
	ostream.write(string)

# ================================================================
def main():
	port_number = DEFAULT_PORT_NUMBER
	infile = None
	outfile = None
	donefile = None

	try:
		optargs, non_option_args = getopt.getopt(sys.argv[1:], "p:i:o:d:h", ['help'])
	except getopt.GetoptError, err:
		print >> sys.stderr, str(err)
		usage(sys.stderr)
		sys.exit(1);

	for opt, arg in optargs:
	    if opt == '-p':
			port_number = int(arg)
	    elif opt == '-i':
			infile = arg
	    elif opt == '-o':
			outfile = arg
	    elif opt == '-d':
			donefile = arg
	    elif opt == '-h':
			usage(sys.stdout)
			sys.exit(0)
	    elif opt == '--help':
			usage(sys.stdout)
			sys.exit(0)
	    else:
			print >> sys.stderr, "Unhandled option \"%s\"." % opt
			sys.exit(1)

	if donefile == None:
		usage(sys.stderr)
		sys.exit(1)
	non_option_arg_count = len(non_option_args)
	if non_option_arg_count != 0:
		usage(sys.stderr)
		sys.exit(1)

	server = SpitServer(port_number, infile, outfile, donefile)
	server.server_loop()


# ================================================================
BUFSIZE=1024
class SpitServer:

	def __init__(self, port_number, infile, outfile, donefile):
		self.task_ids_to_do    = set()
		self.task_ids_assigned = set()
		self.task_ids_done     = set()
		self.task_ids_failed   = set()

		print("%s,op=read_in_file,port=%d,%s" % (self.format_time(), port_number, self.format_counts()))
		sys.stdout.flush()
		self.task_ids_to_do = self.load_file_to_set(infile)

		print("%s,op=read_done_file,port=%d,%s" % (self.format_time(), port_number, self.format_counts()))
		sys.stdout.flush()
		if os.path.isfile(donefile):
			self.task_ids_done = self.load_file_to_set(donefile)
			for e in self.task_ids_done:
				self.task_ids_to_do.remove(e)

		self.outfile = outfile
		self.donefile = donefile

		self.tcp_server = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
		self.tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.tcp_server.bind(('', port_number))
		self.tcp_server.listen(1)

		print("%s,op=ready,port=%d,%s" % (self.format_time(), port_number, self.format_counts()))
		sys.stdout.flush()

	# ----------------------------------------------------------------
	def load_file_to_set(self, filename):
		s = set()
		if filename == None:
			while True:
				line = sys.stdin.readline()
				if line == '':
					break
				s.add(line.rstrip())
		else:
			mode = 'r'
			try:
				file_handle = open(filename, mode)
			except:
				print >> sys.stderr, \
					"%s: couldn't open file \"%s\" for mode \"%s\"." \
					% (sys.argv[0], filename, mode)
				sys.exit(1)
			while True:
				line = file_handle.readline()
				if line == '':
					break
				s.add(line.rstrip())
			file_handle.close()

		return s

	# ----------------------------------------------------------------
	def format_time(self):
		return "t=" + str(time.time())

	# ----------------------------------------------------------------
	def format_counts(self):
		ntodo     = len(self.task_ids_to_do)
		nassigned = len(self.task_ids_assigned)
		ndone     = len(self.task_ids_done)
		nfailed   = len(self.task_ids_failed)
		ntotal = ntodo + nassigned + ndone + nfailed

		string  = "ntodo="+str(ntodo)
		string += ",nassigned="+str(nassigned)
		string += ",ndone="+str(ndone)
		string += ",nfailed="+str(nfailed)
		string += ",ntotal="+str(ntotal)

		return string

	# ----------------------------------------------------------------
	def server_loop(self):
		while True:
			if len(self.task_ids_to_do) == 0:
				print("%s,op=empty,%s" % (self.format_time(), self.format_counts()))
				sys.stdout.flush()
				# Don't break out of the loop: we're done assigning but workers
				# are still running. Stay around waiting for their final
				# mark-done messages to come in.

			client, addr = self.tcp_server.accept() # blocking call
			self.handle_client(client, addr)
			client.close()

	# ----------------------------------------------------------------
	def handle_client(self, client, addr):
		client_host = addr[0]
		client_port = addr[1]

		print("%s,op=accept,client_host=%s,client_port=%d" % (self.format_time(), client_host, client_port))
		sys.stdout.flush()

		request = ''
		while True:
			piece = client.recv(BUFSIZE)
			request += piece
			if len(piece) > 0 and piece[-1] == '\n':
				break

		line = request.rstrip()

		fields1 = line.split(',', 2)
		if len(fields1) != 3:
			print("%s,op=drop1,line=%s" % (self.format_time(), line))
			return
		client_hostname, worker_id, msg = fields1

		fields2 = msg.split(':', 1)
		if len(fields2) != 2:
			print("%s,op=drop2,line=%s,msg=%s" % (self.format_time(), line, msg))
			return
		verb, payload = fields2

		if verb == 'wreq':
			reply = self.handle_wreq(client_hostname, worker_id)
		elif verb == 'show':
			reply = self.handle_show(client_hostname, worker_id)
		elif verb == 'mark-done':
			reply = self.handle_mark_done(client_hostname, worker_id, payload)
		elif verb == 'mark-failed':
			reply = self.handle_mark_failed(client_hostname, worker_id, payload)
		elif verb == 'output':
			reply = self.handle_output(client_hostname, worker_id, payload)
		elif verb == 'stats':
			reply = self.handle_stats(client_hostname, worker_id, payload)
		else:
			reply = 'dropped'
			print("%s,op=drop3,verb=%s,payload=%s" % (self.format_time(), verb, payload))
			sys.stdout.flush()
		client.sendall(reply)

	# ----------------------------------------------------------------
	def handle_wreq(self, client_hostname, worker_id):
		if len(self.task_ids_to_do) > 0:
			task_id = self.task_ids_to_do.pop()
			print("%s,op=give,h=%s,w=%s,%s,task_id=%s" %
				((self.format_time(), client_hostname, worker_id, self.format_counts(), task_id)))
			sys.stdout.flush()
			self.task_ids_assigned.add(task_id)
			return task_id
		else:
			return "no-work-left"

	# ----------------------------------------------------------------
	def handle_show(self, client_hostname, worker_id):
		return "%s,%s" % (self.format_time(), self.format_counts())

	# ----------------------------------------------------------------
	def handle_mark_done(self, client_hostname, worker_id, payload):
		task_id = payload
		if task_id in self.task_ids_assigned:
			self.task_ids_assigned.remove(task_id)
			self.task_ids_done.add(task_id)
			self.mark_to_done_file(task_id)
			print("%s,op=mark-done,ok=true,h=%s,w=%s,%s,task_id=%s" % (
				(self.format_time(), client_hostname, worker_id, self.format_counts(), payload)))
			sys.stdout.flush()
			return "ack"
		else:
			print("%s,op=mark-done,ok=rando,h=%s,w=%s,%s,task_id=%s" % (
				(self.format_time(), client_hostname, worker_id, self.format_counts(), payload)))
			sys.stdout.flush()
			return "nak"

	# ----------------------------------------------------------------
	def handle_mark_failed(self, client_hostname, worker_id, payload):
		task_id = payload
		if task_id in self.task_ids_assigned:
			self.task_ids_assigned.remove(task_id)
			self.task_ids_failed.add(task_id)
			print("%s,op=mark-failed,ok=true,h=%s,w=%s,%s,task_id=%s" % (
				(self.format_time(), client_hostname, worker_id, self.format_counts(), payload)))
			sys.stdout.flush()
			return "ack"
		else:
			print("%s,op=mark-failed,ok=rando,h=%s,w=%s,%s,task_id=%s" % (
				(self.format_time(), client_hostname, worker_id, self.format_counts(), payload)))
			sys.stdout.flush()
			return "nak"

	# ----------------------------------------------------------------
	def handle_output(self, client_hostname, worker_id, payload):
		if self.outfile == None:
			print(payload)
			sys.stdout.flush()
		else:
			self.write_to_out_file(payload)
		return "ack"

	# ----------------------------------------------------------------
	def handle_stats(self, client_hostname, worker_id, payload):
		print("%s,op=stats,h=%s,w=%s,%s" %
			(self.format_time(), client_hostname, worker_id, payload))
		sys.stdout.flush()
		return "ack"

	# ----------------------------------------------------------------
	def write_to_out_file(self, payload):
		mode = 'a'
		try:
			file_handle = open(self.outfile, mode)
			file_handle.write(payload + "\n")
		except:
			print >> sys.stderr, \
				"%s: couldn't open file \"%s\" for mode \"%s\"." \
				% (sys.argv[0], self.outfile, mode)
			sys.exit(1)
		file_handle.close()

	# ----------------------------------------------------------------
	def mark_to_done_file(self, task_id):
		mode = 'a'
		try:
			file_handle = open(self.donefile, mode)
			file_handle.write(task_id + "\n")
		except:
			print >> sys.stderr, \
				"%s: couldn't open file \"%s\" for mode \"%s\"." \
				% (sys.argv[0], self.donefile, mode)
			sys.exit(1)
		file_handle.close()

# ================================================================
main()
