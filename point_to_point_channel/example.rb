#!/usr/bin/env ruby

require "bundler/setup"
require "aws-sdk"

require_relative "../shared/aws_config"

sqs = Aws::SQS::Client.new
queue = sqs.create_queue(queue_name: "point_to_point_channel")

pids = Array.new(3) {
  fork do
    puts "Child process #{Process.pid} waiting for messages...\n"
    poller = Aws::SQS::QueuePoller.new(queue.queue_url)
    loop do
      begin
        poller.poll do |msg|
          puts "Processing '#{msg.body}' in #{Process.pid}.\n"
        end
      rescue
        sleep 0.2
      end
    end
  end
}
at_exit do
  pids.each do |pid|
    Process.kill("TERM", pid)
  end
end

10.times do |i|
  puts "Pushing message #{i}..."
  sqs.send_message(queue_url: queue[:queue_url], message_body: "Message #{i}")
end

sleep 3  # allow for processing
