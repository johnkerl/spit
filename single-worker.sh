#!/bin/bash -e

ourdir=$(dirname $0)
program_to_run_with_task_id="./sample-task-script.rb"

spit_client_args="$@"
spit_client="$ourdir/spit-client.rb $spit_client_args"
while true; do
  task_id=$($spit_client ask)
  echo ">>$task_id<<"
  if [ "$task_id" == 'no-work-left' ]; then
    break
  fi
  start=$($ourdir/spit-systime.rb)

  $program_to_run_with_task_id $task_id

  end=$($ourdir/spit-systime.rb)

  $spit_client "stats" "start=$start,end=$end"
  $spit_client "mark-done" "$task_id"
done
