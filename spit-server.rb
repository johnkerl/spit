#!/usr/bin/env ruby
STDOUT.sync = true
STDERR.sync = true

require 'socket'
require 'set'
require 'getoptlong'

$default_port_number = 2345

# ================================================================
def usage()
  $stderr.puts <<EOF
Usage: #{$us} [options] {workfile} {donefile}

Options:
-p {port number}   Default #{$default_port_number}"

The workfile should contain work IDs, one per line.  What this means is up to
the client; nominally they will be program arguments to be executed by worker
programs.
EOF
  exit 1
end

# ================================================================
def main()
  port_number = $default_port_number

  opts = GetoptLong.new(
    [ '-p', GetoptLong::REQUIRED_ARGUMENT ],
    [ '-h', '--help', GetoptLong::NO_ARGUMENT ]
  )

  begin
    opts.each do |opt, arg|
      case opt
        when '-p'; port_number = Integer(arg)
        when '-h'; usage
        when '--help'; usage
      end
    end
  rescue GetoptLong::Error
    usage
  end
  usage unless ARGV.length == 2
  infile = ARGV.shift
  donefile = ARGV.shift

  server = SpitServer.new(infile, donefile, port_number)
  server.server_loop
end

# ================================================================
class SpitServer

  # ----------------------------------------------------------------
  def initialize(infile, donefile, port_number)

    @task_ids_to_do    = Set.new
    @task_ids_assigned = Set.new
    @task_ids_done     = Set.new
    @task_ids_failed   = Set.new

    puts "#{format_time},op=read_in_file,#{format_counts}"
    File.foreach(infile) do |line|
      task_id = line.chomp
      @task_ids_to_do << task_id
    end

    puts "#{format_time},op=read_done_file,#{format_counts}"
    if File.exist?(donefile)
      File.foreach(donefile) do |line|
        task_id = line.chomp
        @task_ids_done << task_id
        @task_ids_to_do.delete(task_id)
      end
    end

    @donefile = donefile

    @port_number   = port_number
    @tcp_server    = TCPServer.new(port_number)

    puts "#{format_time},op=ready,port=#{port_number},#{format_counts}"

  end

  # ----------------------------------------------------------------
  def format_time
    "time=#{Time.now.to_f}"
  end
  def format_counts
    string  = "ntodo=#{@task_ids_to_do.length}"
    string += ",nassigned=#{@task_ids_assigned.length}"
    string += ",ndone=#{@task_ids_done.length},"
    string += "nfailed=#{@task_ids_failed.length}"
    string
  end

  # ----------------------------------------------------------------
  def server_loop
    loop do
      if @task_ids_to_do.length == 0
        puts "#{format_time},op=exit,#{format_counts}"
        break
      end
      client = @tcp_server.accept # blocking call
      handle_client(client)
      client.close
    end
  end

  # ----------------------------------------------------------------
  def handle_client(client)
    client_info = client.peeraddr
    client_host = client_info[2]
    client_port = client_info[1]
    puts "#{format_time},op=accept,client=#{client_host}:#{client_port}"

    line = client.gets.chomp
    verb, payload = line.split(':', 2)
    if verb == 'wreq'
      handle_wreq(client)
    elsif verb == 'show'
      handle_show(client)
    elsif verb == 'mark-done'
      handle_mark_done(client, payload)
    elsif verb == 'mark-failed'
      handle_mark_failed(client, payload)
    elsif verb == 'output'
      handle_output(client, payload)
    elsif verb == 'stats'
      handle_stats(client, payload)
    else
      puts "#{format_time},op=drop,verb=#{verb},payload=#{payload}"
    end
  end

  # ----------------------------------------------------------------
  def handle_wreq(client)
    if @task_ids_to_do.length > 0
      task_id = @task_ids_to_do.first
      begin
        client.puts "#{task_id}"
        puts "#{format_time},op=give,task_id=#{task_id},#{format_counts}"
        @task_ids_assigned << task_id
        @task_ids_to_do.delete(task_id)
      rescue Errno::EPIPE
        puts "#{format_time},op=give,exc=epipe"
      end
    else
      begin
        client.puts "no-work-left"
      rescue Errno::EPIPE
      end
    end
  end

  # ----------------------------------------------------------------
  def handle_show(client)
    begin
    client.puts "#{format_time},#{format_counts}"
    rescue Errno::EPIPE
    end
  end

  # ----------------------------------------------------------------
  def handle_mark_done(client, payload)
    task_id = payload
    if @task_ids_assigned.include?(task_id)
      @task_ids_assigned.delete(task_id)
      @task_ids_done << task_id

      File.open(@donefile, "a") {|handle| handle.puts(payload)}

      puts "#{format_time},op=mark-done,task_id=#{payload},ok=true,#{format_counts}"
    else
      puts "#{format_time},op=mark-done,task_id=#{payload},ok=rando,#{format_counts}"
    end
  end

  # ----------------------------------------------------------------
  def handle_mark_failed(client, payload)
    task_id = payload
    if @task_ids_assigned.include?(task_id)
      @task_ids_assigned.delete(task_id)
      @task_ids_failed << task_id
      puts "#{format_time},op=mark-failed,task_id=#{payload},ok=true,#{format_counts}"
    else
      puts "#{format_time},op=mark-failed,task_id=#{payload},ok=rando,#{format_counts}"
    end
  end

  # ----------------------------------------------------------------
  def handle_output(client, payload)
      puts "#{format_time},op=output,#{payload}"
  end

  # ----------------------------------------------------------------
  def handle_stats(client, payload)
      puts "#{format_time},op=stats,#{payload}"
  end
end

# ================================================================
main()
