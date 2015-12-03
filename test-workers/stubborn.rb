#!/usr/bin/env ruby

nkill = rand(5)
ngot = 0
done = false

args = ARGV.join(' ')

Signal.trap("INT") do
  ngot += 1
end

  puts "pid=#{$$},args=#{args}ngot=#{ngot}/#{nkill},state=entering"
while not done
  sleep 1
  if ngot < nkill
    puts "pid=#{$$},args=#{args}ngot=#{ngot}/#{nkill},state=continuing"
  else
    puts "pid=#{$$},args=#{args}ngot=#{ngot}/#{nkill},state=exiting"
    done = true
  end
end
