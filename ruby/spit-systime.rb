#!/usr/bin/env ruby
STDOUT.sync = true
STDERR.sync = true
require 'time'

begin
  puts Time.now.to_f
rescue Errno::EPIPE
end
