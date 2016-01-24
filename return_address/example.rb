#!/usr/bin/env ruby

require "bundler/setup"
require "aws-sdk"

require_relative "../shared/aws_config"

sqs = Aws::SQS::Client.new
request_queue = sqs.create_queue(queue_name: "request_queue")

reply_queues = Array.new(2) do |i|
  sqs.create_queue(queue_name: "reply_queue_#{i}")
end

def request(requestor_id,sqs,request_queue)
  5.times do |i|
    puts "Pushing message #{i}..."
    message_body = "Sender:#{requestor_id} Message #{i} (#{Process.pid})"
    sqs.send_message(queue_url: request_queue.queue_url, message_body: message_body)
  end
end

pids = Array.new(2) do |i|
  fork do
    request(i,sqs,request_queue)
    puts "Child process #{Process.pid} waiting for messages...\n"
    poller = Aws::SQS::QueuePoller.new(reply_queues[i].queue_url)
    loop do
      begin
        poller.poll do |msg|
          puts "got reply '#{msg.body}' in #{Process.pid}.\n"
        end
      rescue
        sleep 0.2
      end
    end
  end
end

at_exit do
  pids.each do |pid|
    Process.kill("TERM", pid)
  end
end

poller = Aws::SQS::QueuePoller.new(request_queue.queue_url)
poller.poll(idle_timeout: 3) do |msg|
  queue_id = msg.body[/Sender:(\d)/,1].to_i
  puts "replying to '#{msg.body}' in #{Process.pid}.\n"
  sqs.send_message(queue_url: reply_queues[queue_id].queue_url, message_body: "reply:#{msg.body}")
end
