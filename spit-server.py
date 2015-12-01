#!/usr/bin/env python

import sys, time, socket, getopt

#DEFAULT_PORT_NUMBER = 2345
DEFAULT_PORT_NUMBER = 2347

# ================================================================
def usage(ostream):
	string = """Usage: %s [options] {workfile} {donefile}

Options:
-p {port number}   Default %d"

The workfile should contain work IDs, one per line.  What this means is up to
the client; nominally they will be program arguments to be executed by worker
programs.
""" % (sys.argv[0], DEFAULT_PORT_NUMBER)
	ostream.write(string)

# ================================================================
def main():
	port_number = DEFAULT_PORT_NUMBER

#  opts = GetoptLong.new(
#    [ '-p', GetoptLong::REQUIRED_ARGUMENT ],
#    [ '-h', '--help', GetoptLong::NO_ARGUMENT ]
#  )
#
#  begin
#    opts.each do |opt, arg|
#      case opt
#        when '-p'; port_number = Integer(arg)
#        when '-h'; usage
#        when '--help'; usage
#      end
#    end
#  rescue GetoptLong::Error
#    usage
#  end
#  usage unless ARGV.length == 2
#  infile = ARGV.shift
#  donefile = ARGV.shift
	infile = 'foo'
	donefile = 'bar'
#
	server = SpitServer(infile, donefile, port_number)
	server.server_loop()


# ================================================================
BUFSIZE=1024
class SpitServer:

	def __init__(self, infile, donefile, port_number):
		self.task_ids_to_do    = set()
		self.task_ids_assigned = set()
		self.task_ids_done     = set()
		self.task_ids_failed   = set()

		print(self.format_time()+"op=read_in_file,port="+str(port_number)+","+self.format_counts())
#    File.foreach(infile) do |line|
#      task_id = line.chomp
#      @task_ids_to_do << task_id
#    end
#
		print(self.format_time()+"op=read_done_file,port="+str(port_number)+","+self.format_counts())
#    if File.exist?(donefile)
#      File.foreach(donefile) do |line|
#        task_id = line.chomp
#        @task_ids_done << task_id
#        @task_ids_to_do.delete(task_id)
#      end
#    end
#
#    @donefile = donefile
#
#    @port_number   = port_number
#    @tcp_server    = TCPServer.new(port_number)
		self.tcp_server = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
		self.tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.tcp_server.bind(('', port_number))
		self.tcp_server.listen(1)
#
		# xxx printf-style thruout
		print(self.format_time()+"op=ready,port="+str(port_number)+","+self.format_counts())
#
#  end

	# ----------------------------------------------------------------
	def format_time(self):
		return str(time.time())

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
				print(self.format_time()+"op=empty,"+self.format_counts())
				# Don't break out of the loop: we're done assigning but workers
				# are still running. Stay around waiting for their final
				# mark-done messages to come in.

			client, addr = self.tcp_server.accept() # blocking call
			self.handle_client(client, addr)
			client.close()

#  # ----------------------------------------------------------------
	def handle_client(self, client, addr):
		print "TYPE "+str(type(client))
		print "TYPE "+str(type(addr))
		client_host = addr[0]
		client_port = addr[1]

		print("%s,op=accept,client=%s:%d" % (self.format_time(), client_host, client_port))

		request = ''
		while True:
			piece = client.recv(BUFSIZE)
			request += piece
			if len(piece) > 0 and piece[-1] == '\n':
				break

		line = request.rstrip()

		verb, payload = line.split(':', 2)

		if verb == 'wreq':
			self.handle_wreq(client)
		elif verb == 'show':
			self.handle_show(client)
		elif verb == 'mark-done':
			self.handle_mark_done(client, payload)
		elif verb == 'mark-failed':
			self.handle_mark_failed(client, payload)
		elif verb == 'output':
			self.handle_output(client, payload)
		elif verb == 'stats':
			self.handle_stats(client, payload)
		else:
			print("%s,op=drop,verb=%s,payload=%s" % (self.format_time(), verb, payload))

	# ----------------------------------------------------------------
	def handle_wreq(self, client):
		if len(self.task_ids_to_do) > 0:
			task_id = self.task_ids_to_do.pop()
			client.sendall(task_id)
			print(self.format_time()+"op=give,"+self.format_counts()+"task_id="+task_id)
			self.task_ids_assigned.add(task_id)
			self.task_ids_to_do.delete(task_id)
		else:
			client.sendall("no-work-left\n")

	# ----------------------------------------------------------------
	# xxx have the handlers ret string or none; & rm client from the handler intf
	def handle_show(self, client):
		client.sendall(self.format_time() + "," + self.format_counts())

	# ----------------------------------------------------------------
	def handle_mark_done(self, client, payload):
		task_id = payload
		if task_id in self.task_ids_assigned:
			self.task_ids_assigned.remove(task_id)
			self.task_ids_done.add(task_id)
			print(self.format_time()+"op=mark-done,ok=true,"+self.format_counts()+"task_id="+payload)
		else:
			print(self.format_time()+"op=mark-done,ok=rando,"+self.format_counts()+"task_id="+payload)

	# ----------------------------------------------------------------
	def handle_mark_failed(self, client, payload):
		task_id = payload
		if task_id in self.task_ids_assigned:
			self.task_ids_assigned.remove(task_id)
			self.task_ids_failed.add(task_id)
			print(self.format_time()+"op=mark-failed,ok=true,"+self.format_counts()+"task_id="+payload)
		else:
			print(self.format_time()+"op=mark-failed,ok=rando,"+self.format_counts()+"task_id="+payload)

	# ----------------------------------------------------------------
	def handle_output(self, client, payload):
		print(self.format_time() + "op=output," + payload)

	# ----------------------------------------------------------------
	def handle_stats(self, client, payload):
		print(self.format_time() + "op=stats," + payload)

# ================================================================
main()
