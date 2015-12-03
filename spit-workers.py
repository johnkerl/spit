#!/usr/bin/env python

# ================================================================
# xxx to do:
# * better shutdown handling -- needed? not if control-C simply kills all child processes.
# ================================================================

import sys, os, re, getopt, time, commands, random, signal

OURDIR = os.path.dirname(sys.argv[0])
if OURDIR == '':
	OURDIR = '.'
execfile(OURDIR + '/spit-classes.py')

# ----------------------------------------------------------------
def usage(stream):
	string = \
"""Usage: %s [options]
Options:
-c {task-worker command}  Required. Please wrap in quotes if it contains
                          whitespace.  This command should be able to take task
                          IDs as the last item(s) on its command line.
-d {work directory}       Required.
-x {max parallel workers} Defaults to 1.
-n {max tasks per worker} Defaults to unlimited (run until done).
-s {hostname}             Defaults to spit-client.py default.
-p {hostname}             Defaults to spit-client.py default.
-O                        Let worker stdout/stderr go to the screen. (Default
                          is to redirect worker stdout/stderr to files in the
                          work directory.)
-P                        Send worker stdout go to the spit server. (Default
                          is to redirect worker stdout/stderr to files in the
                          work directory.)
"""
	stream.write(string)

# ----------------------------------------------------------------
#interrupted = False
#def intr_handler(signum, frame):
#	interrupted = True

def main():
	params = {}
	params['spit-server-host-name']      = 'localhost'
	params['spit-server-port-number']    = 2345
	params['program-to-run-with-task-id']= None
	params['work-dir']                   = None
	params['num-workers']                = 1
	params['max-tasks-per-worker']       = None
	params['redirect-output']            = 'file'

	try:
		optargs, non_option_args = getopt.getopt(sys.argv[1:], "c:d:x:n:s:p:OPh", ['help'])
	except getopt.GetoptError, err:
		print >> sys.stderr, str(err)
		usage(sys.stderr)
		sys.exit(1);

	for opt, arg in optargs:
	    if opt == '-c':
			params['program-to-run-with-task-id'] = arg
	    elif opt == '-d':
			params['work-dir'] = arg
	    elif opt == '-x':
			params['num-workers'] = int(arg)
	    elif opt == '-n':
			params['max-tasks-per-worker'] = int(arg)
	    elif opt == '-s':
			params['spit-server-host-name'] = arg
	    elif opt == '-p':
			params['spit-server-port-number'] = int(arg)
	    elif opt == '-O':
			params['redirect-output'] = 'terminal'
	    elif opt == '-P':
			params['redirect-output'] = 'socket'

	    elif opt == '-h':
			usage(sys.stdout)
			sys.exit(0)
	    elif opt == '--help':
			usage(sys.stdout)
			sys.exit(0)
	    else:
			print >> sys.stderr, "Unhandled option \"%s\"." % opt
			sys.exit(1)

	if len(non_option_args) != 0:
		usage(sys.stderr)
		sys.exit(1)

	if params['program-to-run-with-task-id'] == None:
		print >> sys.stderr, "%s: need task-worker command." % sys.argv[0]
		print >> sys.stderr, ""
		usage(sys.stderr)
		sys.exit(1)
	if params['work-dir'] == None and params['redirect-output'] == 'file':
		print >> sys.stderr, "%s: need work dir." % sys.argv[0]
		print >> sys.stderr, ""
		usage(sys.stderr)
		sys.exit(1)

	batch_id = "%s-%d" % (socket.gethostname(), int(time.time()))
	cprint("worker_count=%d" % params['num-workers'])
	if params['work-dir'] != None:
		os.makedirs(params['work-dir'])
		print("work_dir=%s" % params['work-dir'])
	cprint("batch_id=%s" % batch_id)

	worker_id = 0
	pids = set()

	while worker_id < params['num-workers']:
		worker_id += 1
		uuid = "%s-%d" % (batch_id, worker_id)

		pid = os.fork()
		if pid == 0: # child process
			if params['redirect-output'] == 'file':
				out = "%s/%s.out" % (params['work-dir'], uuid)
				err = "%s/%s.err" % (params['work-dir'], uuid)
				sys.stdout = open(out, 'w')
				sys.stderr = open(err, 'w')
			worker(worker_id, params)
			sys.exit(0)
		else:
			pids.add(pid)

	#signal.signal(signal.SIGINT, intr_handler)
	while True:
		#print "interrupted="+str(interrupted)
		if len(pids) == 0:
			break
		try:
			pid, status = os.waitpid(0, 0)
			pids.remove(pid)
			cprint("op=waitpid,pid=%d,status=%d" % (pid, status))
		except os.error, exc:
			err = exc.args[0]
			if err == errno.ECHILD:
				cprint("COORDINATOR EXITING2")
				break
			else:
				cprint(str(exc))
				break

# ----------------------------------------------------------------
def worker(worker_id, params):
	spit_client = SpitClient(params['spit-server-host-name'], params['spit-server-port-number'], worker_id)
	num_run = 0
	prog = params['program-to-run-with-task-id']
	cprint("worker_id=%s,loop=enter" % worker_id)
	while True:
		if params['max-tasks-per-worker'] != None:
			if num_run >= params['max-tasks-per-worker']:
				break

		task_id = spit_client.send("wreq:")

		if task_id == 'exit-now':
			cprint("worker_id=%s,op=exit-now,num_run=%d" % (worker_id, num_run))
			break

		if task_id == 'no-work-left':
			cprint("worker_id=%s,op=done,num_run=%d" % (worker_id, num_run))
			#break
			time.sleep(10 + random.random()*5) # xxx could make this configurable
			continue

		if task_id == 'spit-server-unavailable':
			cprint("worker_id=%s,op=server-down,num_run=%d" % (worker_id, num_run))
			#break
			time.sleep(10 + random.random()*5) # xxx could make this configurable
			continue

		if task_id == 'error' or re.match("^error=.*$", task_id):
			cprint("worker_id=%s,op=socket-error,%s,num_run=%d" % (worker_id, task_id, num_run))
			time.sleep(0.5 + random.random()*1.5) # xxx could make this configurable
			continue

		cmd = prog + " " + task_id
		start = time.time()
		if params['redirect-output'] == 'socket':
			(status, output) = commands.getstatusoutput(cmd)
			# Don't send multiline output on one call since the server might
			# miss a TCP buffer-chunk-fragment ending in newline as the entire
			# payload.
			for line in output.split('\n'):
				spit_client.send("output:"+line)
		else:
			status = os.system(cmd)

		end = time.time()

		spit_client.send(("stats:start=%f,end=%f" % (start, end)))

		if status == 0:
			spit_client.send("mark-done:"+task_id)
		else:
			spit_client.send("mark-failed:"+task_id)

		num_run += 1
		cprint("worker_id=%s,op=continue,task_id=%s,num_run=%d" % (worker_id, task_id, num_run))

	cprint("worker_id=%s,loop=exit" % worker_id)

# ----------------------------------------------------------------
main()
