#!/usr/bin/env ruby

require "bundler/setup"
require "aws-sdk"

require "json"

require_relative "../shared/aws_config"

sqs = Aws::SQS::Client.new
queue = sqs.create_queue(queue_name: "publish_subscribe_channel_0")
queue_attributes = sqs.get_queue_attributes(
  queue_url: queue.queue_url,
  attribute_names: ["QueueArn"]
)

sns = Aws::SNS::Client.new
topic = sns.create_topic(name: "publish_subscribe_channel")
subscription = sns.subscribe(
  topic_arn: topic.topic_arn,
  protocol: "sqs",
  endpoint: queue_attributes.attributes["QueueArn"]
)

sqs.set_queue_attributes(
  queue_url: queue.queue_url,
  attributes: {
    "Policy" => {
      "Version" => "2012-10-17",
      "Statement" => [
        {
          "Effect" => "Allow",
          "Principal" => "*",
          "Action" => "sqs:SendMessage",
          "Resource" => queue_attributes.attributes["QueueArn"],
          "Condition" => {
            "ArnEquals" => {
              "aws:SourceArn" => topic.topic_arn
            }
          }
        }
      ]
    }.to_json
  }
)

# resp = sqs.receive_message(
#   queue_url: queue.queue_url,
#   wait_time_seconds: 20
# )
# p resp

# pids = Array.new(3) {
#   fork do
#     puts "Child process #{Process.pid} waiting for messages...\n"
#     poller = Aws::SQS::QueuePoller.new(queue[:queue_url])
#     poller.poll do |msg|
#       puts "Processing '#{msg.body}' in #{Process.pid}.\n"
#     end
#   end
# }
# at_exit do
#   pids.each do |pid|
#     Process.kill("TERM", pid)
#   end
# end

# 10.times do |i|
#   puts "Pushing message #{i}..."
#   sqs.send_message(queue_url: queue[:queue_url], message_body: "Message #{i}")
# end

# sleep 3  # allow for processing
