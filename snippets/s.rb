#!/usr/bin/env ruby

require 'socket'

port_number = 2345
if ARGV.length == 1
  port_number = Integer(ARGV[0])
end

socket = TCPServer.new(port_number)
socket.setsockopt(Socket::SOL_SOCKET, Socket::SO_REUSEADDR, true)

puts "Listening on port #{port_number}"
while true
  client = socket.accept
  client_info = client.peeraddr
  client_host = client_info[2]
  client_port = client_info[1]
  puts "Client: #{client_host}:#{client_port}"
  request = client.gets.chomp
  reply = "s.rb:" + request
  client.puts(reply)
  client.close
end
