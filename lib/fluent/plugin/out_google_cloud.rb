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

    config_param :detect_json, :bool, :default => false
    config_param :k8s_cluster_name, :string, :default => nil
    config_param :k8s_cluster_location, :string, :default => nil
    config_param :enable_monitoring, :bool, :default => false
    config_param :monitoring_type, :string, :default => nil
    config_param :split_logs_by_tag, :bool, :default => false
    config_param :detect_subservice, :bool, :default => true
    config_param :use_grpc, :bool, :default => false
    config_param :adjust_invalid_timestamps, :bool, :default => true

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

    def format(tag, time, record)
      Fluent::Engine.msgpack_factory.packer.write([tag, time, record]).to_s
    end
  end
end
