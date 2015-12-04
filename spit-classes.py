import sys, os, time, socket, errno, math

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

	def __init__(self, port_number, infile, outfile, donefile, reply_exit_now, estimator_window_size):
		self.task_ids_to_do    = set()
		self.task_ids_assigned = set()
		self.task_ids_done     = set()
		self.task_ids_failed   = set()

		self.done_time_estimator = None
		self.est_time = "TBD"
		self.est_time_uncert = "TBD"

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

		self.done_time_estimator = DoneTimeEstimator(estimator_window_size, 100.0)
		self.update_done_time_estimate()

		self.reply_exit_now = reply_exit_now

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
		ntotal    = ntodo + nassigned + ndone + nfailed

		string  = "ntodo="+str(ntodo)
		string += ",nassigned="+str(nassigned)
		string += ",ndone="+str(ndone)
		string += ",nfailed="+str(nfailed)
		string += ",ntotal="+str(ntotal)
		if ntotal > 0:
			percent = (100.0 * ndone) / ntotal
			string += ",percent="+str(percent)
		else:
			string += ",percent="

		string += ",est_time="+self.est_time
		string += ",est_time_uncert="+self.est_time_uncert

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
		elif self.reply_exit_now:
			return "exit-now"
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
			self.update_done_time_estimate()

			cprint("%s,op=mark-done,ok=true,h=%s,w=%s,%s,task_id=%s" % (
				(self.format_time(), client_hostname, worker_id, self.format_counts(), payload)))
			return "ack"
		else:
			cprint("%s,op=mark-done,ok=rando,h=%s,w=%s,%s,task_id=%s" % (
				(self.format_time(), client_hostname, worker_id, self.format_counts(), payload)))
			return "nak"


	# ----------------------------------------------------------------
	def update_done_time_estimate(self):
		ntodo     = len(self.task_ids_to_do)
		nassigned = len(self.task_ids_assigned)
		ndone     = len(self.task_ids_done)
		nfailed   = len(self.task_ids_failed)
		ntotal    = ntodo + nassigned + ndone + nfailed
		percent   = 0.0
		if ntotal != 0:
			percent = (100.0 * ndone) / ntotal
		self.done_time_estimator.add(time.time(), percent)
		if self.done_time_estimator.size >= 4:
			self.est_time, self.est_time_uncert = self.done_time_estimator.predict_dhms()

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
	def mark_to_done_file(self, task_id):
		self.donehandle.write(task_id+'\n')
		self.donehandle.flush()

# ================================================================
class DoneTimeEstimator:
	def __init__(self, winsz, goalval):
		self.time_offsets = []
		self.value_offsets = []

		# Subtract start time for linear regression: epoch seconds are ~1.5
		# gigaseconds (2015) so shifting by start time makes for more
		# reasonable-sized numbers for the regression.
		self.start_time = time.time()
		self.goalval = goalval

		self.capacity = winsz
		self.size = 0
		self.idx = 0;

	def add(self, timestamp, curval):
		time_offset = timestamp - self.start_time
		value_offset = self.goalval - curval
		if self.size < self.capacity:
			self.time_offsets.append(time_offset)
			self.value_offsets.append(value_offset)
			self.idx += 1
			self.size += 1
		else:
			self.idx = (self.idx + 1) % self.capacity
			self.time_offsets[self.idx] = time_offset
			self.value_offsets[self.idx] = value_offset

	# Returns [number of seconds from current time, error bar]
	def predict_dhms(self):
		etc_sec, bar_sec = self.predict_seconds()
		return [self.dhms(etc_sec), self.dhms(bar_sec)]

	def predict_seconds(self):

		m, b, sm, sb = self.linear_regression(self.time_offsets, self.value_offsets)

		# "etc" = "estimated time of completion"
		if m == 0.0:
			return [None, None]

		# Compute estimated done time, relative to start
		etc_offset_sec = -b/m 

		# Compute error bar on the estimate
		lo_offset_sec = -b/m
		hi_offset_sec = -b/m
		for mm in [m-2*sm,m,m+2*sm]:
			for bb in [b-2*sb,b,b+2*sb]:
				ss = -bb/mm
				lo_offset_sec = min(lo_offset_sec, ss)
				hi_offset_sec = max(hi_offset_sec, ss)
		bar_sec = abs(etc_offset_sec - lo_offset_sec)
		bar_sec = max(bar_sec, abs(etc_offset_sec - hi_offset_sec))

		etc_sec = etc_offset_sec + self.start_time - time.time()

		return [etc_sec, bar_sec]

	def linear_regression(self, xs, ys):
		sumxi   = 0.0
		sumyi   = 0.0
		sumxiyi = 0.0
		sumxi2  = 0.0

		N = len(xs)
		for i in range(0, N):
			x = xs[i]
			y = ys[i]
			sumxi   += x
			sumyi   += y
			sumxiyi += x*y
			sumxi2  += x*x

		D =  N * sumxi2 - sumxi**2
		m = (N * sumxiyi - sumxi * sumyi) / D
		b = (-sumxi * sumxiyi + sumxi2 * sumyi) / D

		# Young 1962, pp. 122-124.  Compute sample variance of linear
		# approximations, then variances of m and b.
		var_z = 0.0
		for i in range(0, N):
			var_z += (m * xs[i] + b - ys[i])**2
		var_z /= N

		var_m = (N * var_z) / D
		var_b = (var_z * sumxi2) / D

		return [m, b, math.sqrt(var_m), math.sqrt(var_b)]

	def dhms(self, seconds):
		if seconds == None:
			return "TBD"

		seconds  = int(seconds)

		ss  = seconds % 60
		seconds = seconds / 60

		mm  = seconds % 60
		seconds = seconds / 60

		hh  = seconds % 24
		seconds = seconds / 24

		dd  = seconds

		if dd == 0 and hh == 0 and mm == 0:
			return "%us" % (ss)
		elif dd == 0 and hh == 0:
			return "%um:%02us" % (mm, ss)
		elif dd == 0:
			return "%uh:%02um:%02us" % (hh, mm, ss)
		else:
			return "%ud:%02uh:%02um:%02us" % (dd, hh, mm, ss)

