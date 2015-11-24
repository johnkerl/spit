#!/usr/bin/env ruby

args = ARGV.join(' ')
puts "STARTING #{args}"
sleep (0.5 + rand*2.0)
puts "FINISHING #{args}"
