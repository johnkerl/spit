# Why

You have a lot of program instances to run at the command line. It doesn't
matter what language -- Java, C++, Python, Haskell, what have you -- they're
going to be launched from the command line. Meanwhile servers today have
dozens of CPUs, gigabytes of RAM, and very fast Ethernet. You could
run the program instances in parallel but it's easy to lose track of what's already run.

There's that nice Bash paradigm of

```
/path/to/program -a -b --flag arg1 &
/path/to/program -a -b --flag arg2 &
/path/to/program -a -b --flag arg3 &
...
/path/to/program -a -b --flag arg40 &
wait
```

but the problem is that one instance might take far longer to run than another --
so partitioning the work ahead of time will leave a few long poles running while
most other workers are done with their piece.

Enter `spit`: **s**scripting for **p**iles of **i**dempotent **t**asks. Each of those
words is significant here:

* This is plain old scripting. It isn't a massive Apache project with a manual. It's just a few lines of code for doing small-scale work -- using maybe a few dozen hosts at once.
* Piles of work: tasks which are independent of one another.
* Idempotent tasks: things which can be run more than once. (You can set up your program such that if it's invoked twice, it exits early.)
* Tasks which are operable by something invoked at the command line on your systems.

# Demo

## Defining task inputs

Tasks are identified by lines of text. For this example, just run the numbers 1 to 1000 into `infile.txt`:
```
$ seq 1 1000 > infile.txt
```

## Executing tasks

Start the spit server:
```
$ ./spit-server.rb infile.txt donefile.txt | tee -a log.txt
time=1448499925.649899,op=read_in_file,ntodo=0,nassigned=0,ndone=0,nfailed=0
time=1448499925.650602,op=read_done_file,ntodo=1000,nassigned=0,ndone=0,nfailed=0
time=1448499925.651202,op=ready,port=2345,ntodo=1000,nassigned=0,ndone=0,nfailed=0
```

A task process is any program that can take a line of text from the input file
as the last part of its command line: for this example, `echo` is fine.  First
run a single task for sanity check, using `-o` to get output on the screen:
```
$ ./spit-workers.sh -n 1 -x 1 -o -c 'echo This is a sample task executor:' 
worker_count=1
work_dir=
batch_id=name.of.the.host-1448500780
worker_id=1,loop=enter
This is a sample task executor: 1
worker_id=1,op=continue,task_id=1,num_run=1
worker_id=1,op=break,num_run=1
worker_id=1,loop=exit
```

Since that looks good, run 20 simultaneous workers on the same host:
```
$ ./spit-workers.sh -x 20 -d work-dir -c 'echo This is a sample task executor:'
worker_count=20
work_dir=work-dir
batch_id=name.of.host-1448500003
```

Output is in the work directory:
```
$ ls work-dir/|head
name.of.the.host-1448500003-1.err
name.of.the.host-1448500003-1.out
name.of.the.host-1448500003-10.err
name.of.the.host-1448500003-10.out
name.of.the.host-1448500003-11.err
name.of.the.host-1448500003-11.out
name.of.the.host-1448500003-12.err
name.of.the.host-1448500003-12.out
name.of.the.host-1448500003-13.err
name.of.the.host-1448500003-13.out
```

At the server window:
```
...
time=1448500946.9942348,op=accept,client=127.0.0.1:63566
time=1448500946.994581,op=stats,start=1448500946.626452,end=1448500946.722344
time=1448500946.999955,op=accept,client=127.0.0.1:63567
time=1448500947.000096,op=mark-done,task_id=439,ok=true,ntodo=544,nassigned=17,ndone=439,nfailed=0
time=1448500947.077194,op=accept,client=127.0.0.1:63568
time=1448500947.0773818,op=give,task_id=457,ntodo=544,nassigned=17,ndone=439,nfailed=0
time=1448500947.077981,op=accept,client=127.0.0.1:63569
time=1448500947.078729,op=mark-done,task_id=443,ok=true,ntodo=543,nassigned=17,ndone=440,nfailed=0
...
time=1448500981.455488,op=accept,client=127.0.0.1:65280
time=1448500981.455862,op=exit,ntodo=0,nassigned=0,ndone=1000,nfailed=0
time=1448500981.460535,op=accept,client=127.0.0.1:65281
time=1448500981.46084,op=exit,ntodo=0,nassigned=0,ndone=1000,nfailed=0
time=1448500981.464467,op=accept,client=127.0.0.1:65282
time=1448500981.464531,op=exit,ntodo=0,nassigned=0,ndone=1000,nfailed=0
```

## Re-running

The server writes all done tasks to the done file so you can kill the server
and workers, and restart them.

## Analyzing progress

The input file is just lines of program arguments and so is the done file.
The log file has plain-text key-value-pair data which you can analyze however you like.
I prefer to use Miller.

```
grep stats o|mlr --oxtab put '$sec=$end-$start' then stats1 -a p0,p10,p50,p90,p100,co
sec_p0    0.042923
sec_p10   0.187047
sec_p50   0.353362
sec_p90   0.412769
sec_p100  0.664510
sec_count 1813

```

* percent-done autocompute
* inter-arrival autocompute
* write up ect
* cat msg-check-log.txt|grep mark-|mlr --opprint put '$pct=100*$ndone/($ntodo+$ndone);$goal=100' then cut -o -f pct,goal,time | estdonetime
* fage workdir
