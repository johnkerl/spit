#!/usr/bin/env ruby

require 'socket'

server_host_name = 'localhost'
server_port_number = 2345
if ARGV.length == 1
	server_host_name = ARGV[0]
end
if ARGV.length == 2
	server_host_name = ARGV[0]
	server_port_number = Integer(ARGV[1])
end

begin
  socket = TCPSocket.open(server_host_name, server_port_number)
rescue Errno::ECONNREFUSED
  puts "Could not connect to #{server_host_name}:#{server_port_number}"
  exit 1
end

request = 'show'
puts "request: #{request}"
socket.puts(request)
reply = socket.gets
socket.close

if reply.nil?
  puts "reply:   (nil)"
else
  puts "reply:   " + reply.chomp
end
