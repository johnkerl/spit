#!/usr/bin/env python

import sys, os, getopt, socket, errno

OURDIR = os.path.dirname(sys.argv[0])
if OURDIR == '':
	OURDIR = '.'
execfile(OURDIR + '/spit-classes.py')

DEFAULT_WORKER_ID = "0"

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
-w {worker ID} Defaults to %s
""" % (a,a,a,a,a,a,DEFAULT_SPIT_SERVER_HOST_NAME,DEFAULT_SPIT_SERVER_PORT_NUMBER,DEFAULT_WORKER_ID)
	ostream.write(string)

# ----------------------------------------------------------------
def main():
	server_host_name   = DEFAULT_SPIT_SERVER_HOST_NAME
	server_port_number = DEFAULT_SPIT_SERVER_PORT_NUMBER
	worker_id          = DEFAULT_WORKER_ID

	try:
		optargs, non_option_args = getopt.getopt(sys.argv[1:], "s:p:w:h", ['help'])
	except getopt.GetoptError, err:
		print >> sys.stderr, str(err)
		usage(sys.stderr)
		sys.exit(1);

	for opt, arg in optargs:
	    if opt == '-s':
			server_host_name = arg
	    elif opt == '-p':
			server_port_number = int(arg)
	    elif opt == '-w':
			worker_id = arg
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

	client = SpitClient(server_host_name, server_port_number, worker_id)
	if verb == 'ask':
		if non_option_arg_count != 0:
			usage(sys.stderr)
			sys.exit(1)
		task_id = client.send_wreq()
		cprint(task_id)
	elif verb == 'show':
		if non_option_arg_count != 0:
			usage(sys.stderr)
			sys.exit(1)
		output = client.send_show()
		cprint(output)
	elif verb == 'output':
		client.send_output(",".join(non_option_args))
	elif verb == 'stats':
		client.send_stats(",".join(non_option_args))
	elif verb == 'mark-done':
		if non_option_arg_count != 1:
			usage(sys.stderr)
			sys.exit(1)
		task_id = non_option_args[0]
		client.send_mark_done(task_id)
	elif verb == 'mark-failed':
		if non_option_arg_count != 1:
			usage(sys.stderr)
			sys.exit(1)
		task_id = non_option_args[0]
		client.send_mark_failed(task_id)
	else:
		usage(sys.stderr)
		sys.exit(1)

# ================================================================
if __name__ == "__main__":
	main()
