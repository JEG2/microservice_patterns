#!/usr/bin/env ruby

require "bundler/setup"
require "aws-sdk"

require_relative "../shared/aws_config"

sqs = Aws::SQS::Client.new
request_queue = sqs.create_queue(queue_name: "request_queue")
reply_queue = sqs.create_queue(queue_name: "reply_queue")

pid =
  fork do
    puts "Child process #{Process.pid} waiting for messages...\n"
    poller = Aws::SQS::QueuePoller.new(request_queue[:queue_url])
    loop do
      begin
        poller.poll do |msg|
          puts "replying to '#{msg.body}' in #{Process.pid}.\n"
          sqs.send_message(queue_url: reply_queue.queue_url, message_body: "reply #{msg.body}")
        end
      rescue
        sleep 0.2
      end
    end
  end

at_exit do
  Process.kill("TERM", pid)
end

10.times do |i|
  puts "Pushing message #{i}..."
  sqs.send_message(queue_url: request_queue.queue_url, message_body: "Message #{i}")
end

poller = Aws::SQS::QueuePoller.new(reply_queue[:queue_url])
poller.poll(idle_timeout: 3) do |msg|
  puts "processing response of '#{msg.body}' in #{Process.pid}.\n"
end

