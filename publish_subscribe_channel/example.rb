#!/usr/bin/env ruby

require "bundler/setup"
require "aws-sdk"

require "json"

require_relative "../shared/aws_config"

sns = Aws::SNS::Client.new
sqs = Aws::SQS::Client.new
topic = sns.create_topic(name: "publish_subscribe_channel")

queue_urls = Array.new(3) { |i|
  queue = sqs.create_queue(queue_name: "publish_subscribe_channel_#{i}")
  queue_attributes = sqs.get_queue_attributes(
    queue_url: queue.queue_url,
    attribute_names: ["QueueArn"]
  )
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
  queue.queue_url
}

pids = Array.new(queue_urls.size) { |i|
  fork do
    puts "Child process #{Process.pid} waiting for messages...\n"
    poller = Aws::SQS::QueuePoller.new(queue_urls[i])
    poller.poll do |msg|
      puts "Processing '#{msg.body}' in #{Process.pid}.\n"
    end
  end
}
at_exit do
  pids.each do |pid|
    Process.kill("TERM", pid)
  end
end

3.times do |i|
  puts "Pushing message #{i}..."
  sns.publish(topic_arn: topic.topic_arn, message: "Message #{i}")
end

sleep 3  # allow for processing
