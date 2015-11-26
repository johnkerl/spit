#!/bin/bash

set -u

us=$(basename $0)
ourdir=$(dirname $0)

program_to_run_with_task_id=
work_dir=
spit_client=$ourdir/spit-client.rb
num_workers=1
max_tasks_per_worker=
redirect_output=true

# ----------------------------------------------------------------
usage() {
	cat 1>&2 <<EOF
Usage: $(basename $0) [options]
Options:
-c {task-worker command}  Required. Please wrap in quotes if it contains
                          whitespace.  This command should be able to take task
                          IDs as the last item(s) on its command line.
-d {work directory}       Required.
-x {max parallel workers} Defaults to 1.
-n {max tasks per worker} Defaults to unlimited (run until done).
-s {hostname}             Defaults to spit-client.rb default.
-p {hostname}             Defaults to spit-client.rb default.
-o                        Let worker stdout/stderr go to the screen. (Default
                          is to redirect worker stdout/stderr to files in the
                          work directory.)
EOF
	exit 1
}

# ----------------------------------------------------------------
worker() {
  worker_id="$1"

  echo "worker_id=$worker_id,loop=enter"
  num_run=0
  while true; do
    if [ ! -z "$max_tasks_per_worker" ]; then
      if [ "$num_run" -ge "$max_tasks_per_worker" ]; then
        echo "worker_id=$worker_id,op=break,num_run=$num_run"
        break
      fi
    fi

    task_id=$($spit_client ask)
    if [ "$task_id" == 'no-work-left' ]; then
      echo "worker_id=$worker_id,op=done,num_run=$num_run"
      break
    fi
    if [ "$task_id" == 'spit-server-unavailable' ]; then
      echo "worker_id=$worker_id,op=server-down,num_run=$num_run"
      break
    fi

    start=$($ourdir/spit-systime.rb)

    $program_to_run_with_task_id $task_id
    status=$?

    end=$($ourdir/spit-systime.rb)
    $spit_client "stats" "start=$start,end=$end"

    if [ $status -eq 0 ]; then
      $spit_client "mark-done" "$task_id"
    else
      $spit_client "mark-failed" "$task_id"
    fi

    num_run=$[num_run+1]
    echo "worker_id=$worker_id,op=continue,task_id=$task_id,num_run=$num_run"
  done
  echo "worker_id=$worker_id,loop=exit"
}

# ----------------------------------------------------------------
main() {
  spit_server_host_name_args=
  spit_server_port_number_args=
  while getopts c:d:x:n:s:p:oh? f
  do
  	case $f in
  	c)  program_to_run_with_task_id="$OPTARG"; continue;;
  	d)  work_dir="$OPTARG"; continue;;
  	x)  num_workers="$OPTARG"; continue;;
  	n)  max_tasks_per_worker="$OPTARG"; continue;;
  	s)  spit_server_host_name_args=" -s $OPTARG"; continue;;
  	p)  spit_server_port_number_args=" -p $OPTARG"; continue;;
  	o)  redirect_output=false; continue;;
  	h)  usage;          continue;;
  	\?) echo; usage;;
  	esac
  done
  shift $(($OPTIND-1))
  non_option_args="$@"
  non_option_arg_count=$#
  if [ "$non_option_arg_count" -ne 0 ]; then
    echo "$0: extraneous arguments \"$@\"" 1>&2
    echo "" 1>&2
    usage
  fi
  if [ -z "$program_to_run_with_task_id" ]; then
    echo "$0: need task-workder command." 1>&2
    echo "" 1>&2
    usage
  fi
  if [ -z "$work_dir" -a "$redirect_output" = "true" ]; then
    echo "$0: need work_dir." 1>&2
    echo "" 1>&2
    usage
  fi

  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  spit_client="$ourdir/spit-client.rb$spit_server_host_name_args$spit_server_port_number_args"

  if [ ! -z "$work_dir" ]; then
    mkdir -p $work_dir
  fi
  batch_id=$(hostname)-$(date +%s)
  echo "worker_count=$num_workers"
  echo "work_dir=$work_dir"
  echo "batch_id=$batch_id"
  worker_id=0
  pids=""
  while [ $worker_id -lt $num_workers ]; do
    worker_id=$[worker_id+1]
    uuid=${batch_id}-${worker_id}
    if [ "$redirect_output" = "true" ]; then
      worker $worker_id 1> $work_dir/$uuid.out 2> $work_dir/$uuid.err &
      pid=$!
    else
      worker $worker_id &
      pid=$!
    fi
    pids="$pids $pid"
    trap 'kill $pids' 1 2 3 15
  done
  wait

}

# ----------------------------------------------------------------
main "$@"
