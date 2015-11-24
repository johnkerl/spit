#!/bin/bash -e

ourdir=$(dirname $0)

# ----------------------------------------------------------------
program_to_run_with_task_id="./sample-task-script.rb"
spit_client_args="-s localhost"
workdir="./work-dir"
npar=20

spit_client="$ourdir/spit-client.rb $spit_client_args"

# ----------------------------------------------------------------
worker() {
  echo "worker_id=$worker_id,loop=enter"
  while true; do
    task_id=$($spit_client ask)
    if [ "$task_id" == 'no-work-left' ]; then
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
  done
  echo "worker_id=$worker_id,loop=exit"
}

# ----------------------------------------------------------------
mkdir -p $workdir
batch_id=$(hostname)-$(date +%s)
echo "worker_count=$npar"
echo "work_dir=$workdir"
echo "batch_id=$batch_id"
worker_id=0
while [ $worker_id -lt $npar ]; do
  worker_id=$[worker_id+1]
  uuid=${batch_id}-${worker_id}
  worker $worker_id 1> $workdir/$uuid.out 2> $workdir/$uuid.err &
done
wait
