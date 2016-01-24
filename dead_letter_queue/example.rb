#!/usr/bin/env ruby

require "bundler/setup"
require "aws-sdk"

require_relative "../shared/aws_config"

sqs = Aws::SQS::Client.new
dead_letter_queue = sqs.create_queue(queue_name: "dead_letter_queue")
queue = sqs.create_queue(queue_name: "process_some_queue")

pids = [ ]
pids <<
  fork do
    puts "Child process #{Process.pid} waiting for messages...\n"
    poller = Aws::SQS::QueuePoller.new(queue.queue_url)
    loop do
      begin
        poller.poll do |msg|
          begin
            if msg.body.start_with?("Invalid")
              sqs.send_message(
                queue_url: dead_letter_queue.queue_url,
                message_body: msg.body
              )
            else
              if rand(1..3) == 1
                fail "Oops"
              else
                puts "Processing '#{msg.body}' in #{Process.pid}."
              end
            end
          rescue
            sqs.send_message(
              queue_url: dead_letter_queue.queue_url,
              message_body: "Error:  #{msg.body}"
            )
          end
        end
      rescue
        sleep 0.2
      end
    end
  end
pids <<
  fork do
    puts "Child process #{Process.pid} waiting for messages...\n"
    poller = Aws::SQS::QueuePoller.new(dead_letter_queue.queue_url)
    loop do
      begin
        poller.poll do |msg|
          puts "Processing Dead Letter '#{msg.body}' in #{Process.pid}."
        end
      rescue
        sleep 0.2
      end
    end
  end
at_exit do
  pids.each do |pid|
    Process.kill("TERM", pid)
  end
end

10.times do |i|
  puts "Pushing message #{i}..."
  sqs.send_message(
    queue_url: queue.queue_url,
    message_body: "#{rand(1..3) == 1 ? 'Invalid' : 'Message'} #{i}"
  )
end

sleep 3  # allow for processing
