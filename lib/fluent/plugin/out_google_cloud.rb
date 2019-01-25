# Copyright 2014 Google Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
require 'erb'
require 'grpc'
require 'json'
require 'open-uri'
require 'socket'
require 'time'
require 'yaml'
require 'google/apis'
require 'google/apis/logging_v2'
require 'google/cloud/logging/v2'
require 'google/gax'
require 'google/logging/v2/logging_pb'
require 'google/logging/v2/logging_services_pb'
require 'google/logging/v2/log_entry_pb'
require 'googleauth'

require_relative 'monitoring'

module Fluent
  # fluentd output plugin for the Stackdriver Logging API
  class GoogleCloudOutput < BufferedOutput

    Fluent::Plugin.register_output('google_cloud', self)

    PLUGIN_NAME = 'Fluentd Google Cloud Logging plugin'.freeze

    config_param :use_metadata_service, :bool, :default => true
    config_param :use_aws_availability_zone, :bool, :default => true
    config_param :project_id, :string, :default => nil
    config_param :zone, :string, :default => nil
    config_param :vm_id, :string, :default => nil
    config_param :vm_name, :string, :default => nil
    config_param :k8s_cluster_name, :string, :default => nil
    config_param :k8s_cluster_location, :string, :default => nil
    config_param :http_request_key, :string, :default => nil
    config_param :operation_key, :string, :default => nil
    config_param :source_location_key, :string, :default => nil
    config_param :trace_key, :string, :default => nil
    config_param :span_id_key, :string, :default => nil
    config_param :insert_id_key, :string, :default => nil
    config_param :detect_json, :bool, :default => false
    config_param :detect_subservice, :bool, :default => true
    config_param :subservice_name, :string, :default => nil
    config_param :require_valid_tags, :bool, :default => false
    config_param :kubernetes_tag_regexp, :string, :default =>
      '\.(?<pod_name>[^_]+)_(?<namespace_name>[^_]+)_(?<container_name>.+)$'
    config_param :label_map, :hash, :default => nil
    config_param :labels, :hash, :default => nil
    config_param :use_grpc, :bool, :default => false
    config_param :partial_success, :bool, :default => true
    config_param :coerce_to_utf8, :bool, :default => true
    config_param :non_utf8_replacement_string, :string, :default => ' '
    config_param :auth_method, :string, :default => nil
    config_param :private_key_email, :string, :default => nil
    config_param :private_key_path, :string, :default => nil
    config_param :private_key_passphrase, :string,
                 :default => nil,
                 :secret => true
    config_param :logging_api_url, :string, :default => nil
    config_param :enable_monitoring, :bool, :default => false
    config_param :monitoring_type, :string,
                 :default => Monitoring::PrometheusMonitoringRegistry.name
    config_param :enable_metadata_agent, :bool, :default => false
    config_param :metadata_agent_url, :string, :default => nil
    config_param :split_logs_by_tag, :bool, :default => false
    config_param :adjust_invalid_timestamps, :bool, :default => true
    config_param :autoformat_stackdriver_trace, :bool, :default => true

    # rubocop:enable Style/HashSyntax

    def initialize
      super
      # use the global logger
      @log = $log # rubocop:disable Style/GlobalVars
    end

    def configure(conf)
      super
    end

    def start
      super
      @successful_call = false
      @timenanos_warning = false
    end

    def shutdown
      super
    end

    def write(chunk)
      @log.info 'pretent to write log entries.'
    end

    private

    # "enum" of Platform values
    module Platform
      OTHER = 0  # Other/unkown platform
      GCE = 1    # Google Compute Engine
      EC2 = 2    # Amazon EC2
    end

    def format(tag, time, record)
      Fluent::Engine.msgpack_factory.packer.write([tag, time, record]).to_s
    end

    # Increment the metric for the number of successful requests.
    def increment_successful_requests_count
      return unless @successful_requests_count
      @successful_requests_count.increment(grpc: @use_grpc, code: @ok_code)
    end

    # Increment the metric for the number of failed requests, labeled by
    # the provided status code.
    def increment_failed_requests_count(code)
      return unless @failed_requests_count
      @failed_requests_count.increment(grpc: @use_grpc, code: code)
    end

    # Increment the metric for the number of log entries, successfully
    # ingested by the Stackdriver Logging API.
    def increment_ingested_entries_count(count)
      return unless @ingested_entries_count
      @ingested_entries_count.increment({ grpc: @use_grpc, code: @ok_code },
                                        count)
    end

    # Increment the metric for the number of log entries that were dropped
    # and not ingested by the Stackdriver Logging API.
    def increment_dropped_entries_count(count, code)
      return unless @dropped_entries_count
      @dropped_entries_count.increment({ grpc: @use_grpc, code: code }, count)
    end

    # Increment the metric for the number of log entries that were dropped
    # and not ingested by the Stackdriver Logging API.
    def increment_retried_entries_count(count, code)
      return unless @retried_entries_count
      @retried_entries_count.increment({ grpc: @use_grpc, code: code }, count)
    end
  end
end

module Google
  module Apis
    module LoggingV2
      # Override MonitoredResource::dup to make a deep copy.
      class MonitoredResource
        def dup
          ret = super
          ret.labels = labels.dup
          ret
        end
      end
    end
  end
end
