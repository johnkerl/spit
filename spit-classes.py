import sys, os, time, socket, errno

BUFSIZE                         = 1024
DEFAULT_SPIT_SERVER_HOST_NAME   = 'localhost'
DEFAULT_SPIT_SERVER_PORT_NUMBER = 2345

# ----------------------------------------------------------------
# Concurrent print: python's print is *two* write calls, one for the string and
# one for the newline, and if there are multiple processes, those two can
# interleave on the terminal.
def cprint(s):
	sys.stdout.write(s+"\n")
	sys.stdout.flush()

# ================================================================
class SpitClient:
	def __init__(self, server_host_name, server_port_number, worker_id):
		self.server_host_name = server_host_name
		self.server_port_number = server_port_number
		self.server_address = (server_host_name, server_port_number)
		self.hostname = socket.gethostname()
		self.worker_id = worker_id

	def send_wreq(self):
		return self.send("wreq:")
	def send_show(self):
		return self.send("show:")
	def send_output(self, payload):
		return self.send("output:"+payload)
	def send_stats(self, payload):
		return self.send("stats:"+payload)
	def send_mark_done(self, task_id):
		return self.send("mark-done:"+task_id)
	def send_mark_failed(self, task_id):
		return self.send("mark-failed:"+task_id)

	def send(self, msg):
		sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
		try:
			sock.connect(self.server_address)
			sock.sendall("%s,%s,%s\n" % (self.hostname, self.worker_id, msg))
		except socket.error, exc:
			err = exc.args[0]
			if err == errno.ECONNREFUSED:
				return 'spit-server-unavailable'
			else:
				try:
					return 'error='+errno.errorcode[err]
				except:
					return 'error=???'

		reply = ''
		while True:
			piece = '?'
			try:
				piece = sock.recv(BUFSIZE)
				if len(piece) == 0: # server-side socket close
					break
				reply += piece
			except socket.error, exc:
				err = exc.args[0]
				if err == errno.EAGAIN or err == errno.EWOULDBLOCK:
					pass
				elif err == errno.EPIPE:
					return 'error:EPIPE'
		sock.close()
		if reply == '':
			return 'error:empty-reply'
		return reply.rstrip()

# ================================================================
class SpitServer:

	def __init__(self, port_number, infile, outfile, donefile):
		self.task_ids_to_do    = set()
		self.task_ids_assigned = set()
		self.task_ids_done     = set()
		self.task_ids_failed   = set()

		cprint("%s,op=read_in_file,port=%d,%s" % (self.format_time(), port_number, self.format_counts()))
		self.task_ids_to_do = self.load_file_to_set(infile)

		cprint("%s,op=read_done_file,port=%d,%s" % (self.format_time(), port_number, self.format_counts()))
		if os.path.isfile(donefile):
			self.task_ids_done = self.load_file_to_set(donefile)
			for e in self.task_ids_done:
				self.task_ids_to_do.remove(e)

		if outfile == None:
			self.outhandle = sys.stdout
		else:
			self.outhandle = self.create_append_handle(outfile)
		self.donehandle = self.create_append_handle(donefile)

		self.tcp_server = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
		self.tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.tcp_server.bind(('', port_number))
		self.tcp_server.listen(20)

		cprint("%s,op=ready,port=%d,%s" % (self.format_time(), port_number, self.format_counts()))

	# ----------------------------------------------------------------
	def create_append_handle(self, filename):
		mode = 'a'
		try:
			return open(filename, mode)
		except:
			print >> sys.stderr, \
				"%s: couldn't open file \"%s\" for mode \"%s\"." \
				% (sys.argv[0], filename, mode)
			sys.exit(1)

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
				cprint("%s,op=empty,%s" % (self.format_time(), self.format_counts()))
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

		cprint("%s,op=accept,client_host=%s,client_port=%d" % (self.format_time(), client_host, client_port))
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
			cprint("%s,op=drop1,line=%s" % (self.format_time(), line))
			return
		client_hostname, worker_id, msg = fields1

		fields2 = msg.split(':', 1)
		if len(fields2) != 2:
			cprint("%s,op=drop2,line=%s,msg=%s" % (self.format_time(), line, msg))
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
			cprint("%s,op=drop3,verb=%s,payload=%s" % (self.format_time(), verb, payload))
		client.sendall(reply)

	# ----------------------------------------------------------------
	def handle_wreq(self, client_hostname, worker_id):
		if len(self.task_ids_to_do) > 0:
			task_id = self.task_ids_to_do.pop()
			cprint("%s,op=give,h=%s,w=%s,%s,task_id=%s" %
				((self.format_time(), client_hostname, worker_id, self.format_counts(), task_id)))
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
			cprint("%s,op=mark-done,ok=true,h=%s,w=%s,%s,task_id=%s" % (
				(self.format_time(), client_hostname, worker_id, self.format_counts(), payload)))
			return "ack"
		else:
			cprint("%s,op=mark-done,ok=rando,h=%s,w=%s,%s,task_id=%s" % (
				(self.format_time(), client_hostname, worker_id, self.format_counts(), payload)))
			return "nak"

	# ----------------------------------------------------------------
	def handle_mark_failed(self, client_hostname, worker_id, payload):
		task_id = payload
		if task_id in self.task_ids_assigned:
			self.task_ids_assigned.remove(task_id)
			self.task_ids_failed.add(task_id)
			cprint("%s,op=mark-failed,ok=true,h=%s,w=%s,%s,task_id=%s" % (
				(self.format_time(), client_hostname, worker_id, self.format_counts(), payload)))
			return "ack"
		else:
			cprint("%s,op=mark-failed,ok=rando,h=%s,w=%s,%s,task_id=%s" % (
				(self.format_time(), client_hostname, worker_id, self.format_counts(), payload)))
			return "nak"

	# ----------------------------------------------------------------
	def handle_output(self, client_hostname, worker_id, payload):
		self.outhandle.write(payload+'\n')
		self.outhandle.flush()
		return "ack"

	# ----------------------------------------------------------------
	def handle_stats(self, client_hostname, worker_id, payload):
		cprint("%s,op=stats,h=%s,w=%s,%s" %
			(self.format_time(), client_hostname, worker_id, payload))
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
