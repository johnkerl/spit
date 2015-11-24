#!/usr/bin/env ruby

STDOUT.sync = true
STDERR.sync = true
$us = File.basename $0

require 'socket'
require 'getoptlong'

$default_server_host_name = 'localhost'
$default_server_port_number = 2345

# ================================================================
def usage()
  $stderr.puts <<EOF
Usage: #{$us} [options] ask
Or:    #{$us} [options] show
Or:    #{$us} [options] output      {DKVP text}
Or:    #{$us} [options] stats       {DKVP text}
Or:    #{$us} [options] mark-done   {task_id}
Or:    #{$us} [options] mark-failed {task_id}

Options:
-s {server host name} Defaults to #{$default_server_host_name}
-p {server port number} Defaults to #{$default_server_port_number}
EOF
   exit 1
end

# ----------------------------------------------------------------
def main()
  server_host_name = $default_server_host_name
  server_port_number = $default_server_port_number

  opts = GetoptLong.new(
      [ '-s', GetoptLong::REQUIRED_ARGUMENT ],
      [ '-p', GetoptLong::REQUIRED_ARGUMENT ],
      [ '-h', '--help', GetoptLong::NO_ARGUMENT ]
  )

  begin
    opts.each do |opt, arg|
      case opt
        when '-s'; server_host_name = arg
        when '-p'; server_port_number = Integer(arg)
        when '-h'; usage
        when '--help'; usage
     end
    end
  rescue GetoptLong::Error
      usage
  end
  usage unless ARGV.length >= 1
  verb = ARGV.shift

  client = SpitClient.new(server_host_name, server_port_number)
  if verb == 'ask'
    usage unless ARGV.length == 0
    task_id = client.ask "wreq:"
    puts task_id
  elsif verb == 'show'
    usage unless ARGV.length == 0
    output = client.ask "show:"
    puts output
  elsif verb == 'output'
    text = ARGV.join(',')
    client.send "output:#{text}"
  elsif verb == 'stats'
    text = ARGV.join(',')
    client.send "stats:#{text}"
  elsif verb == 'mark-done'
    usage unless ARGV.length == 1
    task_id = ARGV[0]
    client.send "mark-done:#{task_id}"
  else
    usage
  end
end

# ================================================================
class SpitClient
  def initialize(server_host_name, server_port_number)
    @server_host_name = server_host_name
    @server_port_number = server_port_number
  end
  def send(msg)
    socket = TCPSocket.open(@server_host_name, @server_port_number)
    socket.puts(msg)
    socket.close
    nil
  end
  def ask(msg)
    begin
      socket = TCPSocket.open(@server_host_name, @server_port_number)
    rescue Errno::ECONNREFUSED => e
      $stderr.puts "#{$us}: could not connect to #{@server_host_name}:#{@server_port_number}"
      exit 1
    end
    socket.puts(msg)
    reply = socket.gets.chomp
    socket.close
    reply
  end
end

# ================================================================
main()
