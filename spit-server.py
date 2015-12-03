#!/usr/bin/env python

# xxx to do:
# * estdone file; and/or just do the estimation inline (in-mem -- don't try to regress over restarts)
# * exit-now response back to workers

import sys, os, time, socket, getopt

OURDIR = os.path.dirname(sys.argv[0])
if OURDIR == '':
	OURDIR = '.'
execfile(OURDIR + '/spit-classes.py')

# ================================================================
def usage(ostream):
	string = """Usage: %s [options]

Options:
-p {port number}   Defaults to %d if omitted.
-i {infile}        Defaults to stdin.
-o {outfile}       Defaults to stdout.
-d {donefile}      Required.
-z                 Reply to all client work-requests with 'exit-now'.

The workfile should contain work IDs, one per line.  What this means is up to
the client; nominally they will be program arguments to be executed by worker
programs.
""" % (sys.argv[0], DEFAULT_SPIT_SERVER_PORT_NUMBER)
	ostream.write(string)

# ================================================================
def main():
	port_number = DEFAULT_SPIT_SERVER_PORT_NUMBER
	infile         = None
	outfile        = None
	donefile       = None
	reply_exit_now = False

	try:
		optargs, non_option_args = getopt.getopt(sys.argv[1:], "p:i:o:d:zh", ['help'])
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
	    elif opt == '-z':
			reply_exit_now = True
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

	server = SpitServer(port_number, infile, outfile, donefile, reply_exit_now)
	server.server_loop()

# ================================================================
if __name__ == "__main__":
	main()
