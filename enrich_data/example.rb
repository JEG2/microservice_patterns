#!/usr/bin/env ruby

require "bundler/setup"
require "aws-sdk"

require "json"

require_relative "../shared/aws_config"

sns = Aws::SNS::Client.new
sqs = Aws::SQS::Client.new
topic = sns.create_topic(name: "routes")

PATHS = {
  0 => { "1" => %w[a b c] },
  1 => { 'b' => ['z']      }
}

queue_urls = Array.new(2) { |i|
  queue = sqs.create_queue(queue_name: "routes_queue_#{i}")
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
      journey = JSON.parse(msg.body)
      if PATHS[i].include?(journey[:from]) 
        if PATHS[i][journey[:from]].include?(journey[:to])
          puts "final path :#{journey[:from]} to #{journey[:to]}"
          puts "#{journey[:path]}"
        else
          PATHS[i][journey[:from]].each do |midpoint|
            new_journey = {
              from: midpoint ,
              path: journey[:path] << "#{journey[:from]} to #{midpoint}",
              to:   journey[:to]
            }.to_json
            sns.publish(topic_arn: topic.topic_arn, message: new_journey)
          end
        end
      end
      puts "Processing '#{msg.body}' in #{Process.pid}.\n"
    end
  end
}
at_exit do
  pids.each do |pid|
    Process.kill("TERM", pid)
  end
end

1.times do |i|
  puts "Pushing message #{i}..."
  journey = {
    from: '1',
    path: [],
    to:   'z'  
  }.to_json
  sns.publish(topic_arn: topic.topic_arn, message: journey)
end

sleep 3  # allow for processing
