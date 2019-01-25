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

module Google
  module Protobuf
    # Alias the has_key? method to have the same interface as a regular map.
    class Map
      alias key? has_key?
    end
  end
end

module Google
  module Auth
    # Extract project_id in initialize.
    class ServiceAccountCredentials
      singleton_class.send(:alias_method, :super_make_creds, :make_creds)
      def self.make_creds(options = {})
        json_key_io, scope = options.values_at(:json_key_io, :scope)
        if json_key_io
          json_key = MultiJson.load(json_key_io.read)
          project_id = json_key['project_id']
        end
        creds = super_make_creds(
          json_key_io: StringIO.new(MultiJson.dump(json_key)), scope: scope)
        creds.instance_variable_set(:@project_id, project_id) if project_id
        creds
      end
      attr_reader :project_id
    end
  end
end

module Fluent
  # fluentd output plugin for the Stackdriver Logging API
  class GoogleCloudOutput < BufferedOutput
    # Constants for service names, resource types and etc.
    module ServiceConstants
      APPENGINE_CONSTANTS = {
        service: 'appengine.googleapis.com',
        resource_type: 'gae_app',
        metadata_attributes: %w(gae_backend_name gae_backend_version)
      }.freeze
      CLOUDFUNCTIONS_CONSTANTS = {
        service: 'cloudfunctions.googleapis.com',
        resource_type: 'cloud_function',
        stream_severity_map: {
          'stdout' => 'INFO',
          'stderr' => 'ERROR'
        }
      }.freeze
      COMPUTE_CONSTANTS = {
        service: 'compute.googleapis.com',
        resource_type: 'gce_instance'
      }.freeze
      GKE_CONSTANTS = {
        service: 'container.googleapis.com',
        resource_type: 'container',
        extra_resource_labels: %w(namespace_id pod_id container_name),
        extra_common_labels: %w(namespace_name pod_name),
        metadata_attributes: %w(cluster-name cluster-location),
        stream_severity_map: {
          'stdout' => 'INFO',
          'stderr' => 'ERROR'
        }
      }.freeze
      K8S_CONTAINER_CONSTANTS = {
        resource_type: 'k8s_container'
      }.freeze
      K8S_NODE_CONSTANTS = {
        resource_type: 'k8s_node'
      }.freeze
      DOCKER_CONSTANTS = {
        service: 'docker.googleapis.com',
        resource_type: 'docker_container'
      }.freeze
      DATAFLOW_CONSTANTS = {
        service: 'dataflow.googleapis.com',
        resource_type: 'dataflow_step',
        extra_resource_labels: %w(region job_name job_id step_id)
      }.freeze
      DATAPROC_CONSTANTS = {
        service: 'cluster.dataproc.googleapis.com',
        resource_type: 'cloud_dataproc_cluster',
        metadata_attributes: %w(dataproc-cluster-uuid dataproc-cluster-name)
      }.freeze
      EC2_CONSTANTS = {
        service: 'ec2.amazonaws.com',
        resource_type: 'aws_ec2_instance'
      }.freeze
      ML_CONSTANTS = {
        service: 'ml.googleapis.com',
        resource_type: 'ml_job',
        extra_resource_labels: %w(job_id task_name)
      }.freeze

      # The map between a subservice name and a resource type.
      SUBSERVICE_MAP =
        [APPENGINE_CONSTANTS, GKE_CONSTANTS, DATAFLOW_CONSTANTS,
         DATAPROC_CONSTANTS, ML_CONSTANTS]
        .map { |consts| [consts[:service], consts[:resource_type]] }.to_h
      # Default back to GCE if invalid value is detected.
      SUBSERVICE_MAP.default = COMPUTE_CONSTANTS[:resource_type]
      SUBSERVICE_MAP.freeze

      # The map between a resource type and expected subservice attributes.
      SUBSERVICE_METADATA_ATTRIBUTES =
        [APPENGINE_CONSTANTS, GKE_CONSTANTS, DATAPROC_CONSTANTS].map do |consts|
          [consts[:resource_type], consts[:metadata_attributes].to_set]
        end.to_h.freeze
    end

    # Constants for configuration.
    module ConfigConstants
      # Default values for JSON payload keys to set the "httpRequest",
      # "operation", "sourceLocation", "trace" fields in the LogEntry.
      DEFAULT_HTTP_REQUEST_KEY = 'httpRequest'.freeze
      DEFAULT_OPERATION_KEY = 'logging.googleapis.com/operation'.freeze
      DEFAULT_SOURCE_LOCATION_KEY =
        'logging.googleapis.com/sourceLocation'.freeze
      DEFAULT_TRACE_KEY = 'logging.googleapis.com/trace'.freeze
      DEFAULT_SPAN_ID_KEY = 'logging.googleapis.com/spanId'.freeze
      DEFAULT_INSERT_ID_KEY = 'logging.googleapis.com/insertId'.freeze

      DEFAULT_METADATA_AGENT_URL =
        'http://local-metadata-agent.stackdriver.com:8000'.freeze
      METADATA_AGENT_URL_ENV_VAR = 'STACKDRIVER_METADATA_AGENT_URL'.freeze
    end

    # Internal constants.
    module InternalConstants
      CREDENTIALS_PATH_ENV_VAR = 'GOOGLE_APPLICATION_CREDENTIALS'.freeze
      DEFAULT_LOGGING_API_URL = 'https://logging.googleapis.com'.freeze

      # The label name of local_resource_id in the json payload. When a record
      # has this field in the payload, we will use the value to retrieve
      # monitored resource from Stackdriver Metadata agent.
      LOCAL_RESOURCE_ID_KEY = 'logging.googleapis.com/local_resource_id'.freeze

      # The regexp matches stackdriver trace id format: 32-byte hex string.
      # The format is documented in
      # https://cloud.google.com/trace/docs/reference/v2/rpc/google.devtools.cloudtrace.v1#trace
      STACKDRIVER_TRACE_ID_REGEXP = Regexp.new('^\h{32}$').freeze

      # The name of the WriteLogEntriesPartialErrors field in the error details.
      PARTIAL_ERROR_FIELD =
        'type.googleapis.com/google.logging.v2.WriteLogEntriesPartialErrors' \
        .freeze
    end

    include self::ServiceConstants
    include self::ConfigConstants
    include self::InternalConstants

    Fluent::Plugin.register_output('google_cloud', self)

    PLUGIN_NAME = 'Fluentd Google Cloud Logging plugin'.freeze

    PLUGIN_VERSION = begin
      # Extract plugin version from file path.
      match_data = __FILE__.match(
        %r{fluent-plugin-google-cloud-(?<version>[0-9a-zA-Z\.]*)/})
      if match_data
        match_data['version']
      else
        # Extract plugin version by finding the spec this file was loaded from.
        dependency = Gem::Dependency.new('fluent-plugin-google-cloud')
        all_specs, = Gem::SpecFetcher.fetcher.spec_for_dependency(dependency)
        matching_version, = all_specs.grep(
          proc { |spec,| __FILE__.include?(spec.full_gem_path) }) do |spec,|
            spec.version.to_s
          end
        matching_version
      end
    end.freeze

    # Name of the the Google cloud logging write scope.
    LOGGING_SCOPE = 'https://www.googleapis.com/auth/logging.write'.freeze

    # Address of the metadata service.
    METADATA_SERVICE_ADDR = '169.254.169.254'.freeze

    # Disable this warning to conform to fluentd config_param conventions.
    # rubocop:disable Style/HashSyntax

    # Specify project/instance metadata.
    #
    # project_id, zone, and vm_id are required to have valid values, which
    # can be obtained from the metadata service or set explicitly.
    # Otherwise, the plugin will fail to initialize.
    #
    # Note that while 'project id' properly refers to the alphanumeric name
    # of the project, the logging service will also accept the project number,
    # so either one is acceptable in this context.
    #
    # Whether to attempt to obtain metadata from the local metadata service.
    # It is safe to specify 'true' even on platforms with no metadata service.
    config_param :use_metadata_service, :bool, :default => true
    # A compatibility option to enable the legacy behavior of setting the AWS
    # location to the availability zone rather than the region.
    config_param :use_aws_availability_zone, :bool, :default => true
    # These parameters override any values obtained from the metadata service.
    config_param :project_id, :string, :default => nil
    config_param :zone, :string, :default => nil
    config_param :vm_id, :string, :default => nil
    config_param :vm_name, :string, :default => nil
    # Kubernetes-specific parameters, only used to override these values in
    # the fallback path when the metadata agent is temporarily unavailable.
    # They have to match the configuration of the metadata agent.
    config_param :k8s_cluster_name, :string, :default => nil
    config_param :k8s_cluster_location, :string, :default => nil

    # Map keys from a JSON payload to corresponding LogEntry fields.
    config_param :http_request_key, :string, :default =>
      DEFAULT_HTTP_REQUEST_KEY
    config_param :operation_key, :string, :default => DEFAULT_OPERATION_KEY
    config_param :source_location_key, :string, :default =>
      DEFAULT_SOURCE_LOCATION_KEY
    config_param :trace_key, :string, :default => DEFAULT_TRACE_KEY
    config_param :span_id_key, :string, :default => DEFAULT_SPAN_ID_KEY
    config_param :insert_id_key, :string, :default => DEFAULT_INSERT_ID_KEY

    # Whether to try to detect if the record is a text log entry with JSON
    # content that needs to be parsed.
    config_param :detect_json, :bool, :default => false
    # TODO(igorpeshansky): Add a parameter for the text field in the payload.

    # Whether to try to detect if the VM is owned by a "subservice" such as App
    # Engine of Kubernetes, rather than just associating the logs with the
    # compute service of the platform. This currently only has any effect when
    # running on GCE.
    #
    # The initial motivation for this is to separate out Kubernetes node
    # component (Docker, Kubelet, etc.) logs from container logs.
    config_param :detect_subservice, :bool, :default => true
    # The subservice_name overrides the subservice detection, if provided.
    config_param :subservice_name, :string, :default => nil

    # Whether to reject log entries with invalid tags. If this option is set to
    # false, tags will be made valid by converting any non-string tag to a
    # string, and sanitizing any non-utf8 or other invalid characters.
    config_param :require_valid_tags, :bool, :default => false

    # The regular expression to use on Kubernetes logs to extract some basic
    # information about the log source. The regexp must contain capture groups
    # for pod_name, namespace_name, and container_name.
    config_param :kubernetes_tag_regexp, :string, :default =>
      '\.(?<pod_name>[^_]+)_(?<namespace_name>[^_]+)_(?<container_name>.+)$'

    # label_map (specified as a JSON object) is an unordered set of fluent
    # field names whose values are sent as labels rather than as part of the
    # struct payload.
    #
    # Each entry in the map is a {"field_name": "label_name"} pair.  When
    # the "field_name" (as parsed by the input plugin) is encountered, a label
    # with the corresponding "label_name" is added to the log entry.  The
    # value of the field is used as the value of the label.
    #
    # The map gives the user additional flexibility in specifying label
    # names, including the ability to use characters which would not be
    # legal as part of fluent field names.
    #
    # Example:
    #   label_map {
    #     "field_name_1": "sent_label_name_1",
    #     "field_name_2": "some.prefix/sent_label_name_2"
    #   }
    config_param :label_map, :hash, :default => nil

    # labels (specified as a JSON object) is a set of custom labels
    # provided at configuration time. It allows users to inject extra
    # environmental information into every message or to customize
    # labels otherwise detected automatically.
    #
    # Each entry in the map is a {"label_name": "label_value"} pair.
    #
    # Example:
    #   labels {
    #     "label_name_1": "label_value_1",
    #     "label_name_2": "label_value_2"
    #   }
    config_param :labels, :hash, :default => nil

    # Whether to use gRPC instead of REST/JSON to communicate to the
    # Stackdriver Logging API.
    config_param :use_grpc, :bool, :default => false

    # Whether valid entries should be written even if some other entries fail
    # due to INVALID_ARGUMENT or PERMISSION_DENIED errors when communicating to
    # the Stackdriver Logging API. This is highly recommended.
    config_param :partial_success, :bool, :default => true

    # Whether to allow non-UTF-8 characters in user logs. If set to true, any
    # non-UTF-8 character would be replaced by the string specified by
    # 'non_utf8_replacement_string'. If set to false, any non-UTF-8 character
    # would trigger the plugin to error out.
    config_param :coerce_to_utf8, :bool, :default => true

    # If 'coerce_to_utf8' is set to true, any non-UTF-8 character would be
    # replaced by the string specified here.
    config_param :non_utf8_replacement_string, :string, :default => ' '

    # DEPRECATED: The following parameters, if present in the config
    # indicate that the plugin configuration must be updated.
    config_param :auth_method, :string, :default => nil
    config_param :private_key_email, :string, :default => nil
    config_param :private_key_path, :string, :default => nil
    config_param :private_key_passphrase, :string,
                 :default => nil,
                 :secret => true

    # The URL of Stackdriver Logging API. Right now this only works with the
    # gRPC path (use_grpc = true). An unsecured channel is used if the URL
    # scheme is 'http' instead of 'https'. One common use case of this config is
    # to provide a mocked / stubbed Logging API, e.g., http://localhost:52000.
    config_param :logging_api_url, :string, :default => DEFAULT_LOGGING_API_URL

    # Whether to collect metrics about the plugin usage. The mechanism for
    # collecting and exposing metrics is controlled by the monitoring_type
    # parameter.
    config_param :enable_monitoring, :bool, :default => false

    # What system to use when collecting metrics. Possible values are:
    #   - 'prometheus', in this case default registry in the Prometheus
    #     client library is used, without actually exposing the endpoint
    #     to serve metrics in the Prometheus format.
    #    - any other value will result in the absence of metrics.
    config_param :monitoring_type, :string,
                 :default => Monitoring::PrometheusMonitoringRegistry.name

    # Whether to call metadata agent to retrieve monitored resource.
    config_param :enable_metadata_agent, :bool, :default => false

    # The URL of the Metadata Agent.
    # If this option is set, its value is used to contact the Metadata Agent.
    # Otherwise, the value of the STACKDRIVER_METADATA_AGENT_URL environment
    # variable is used. If that is also unset, this defaults to
    # 'http://local-metadata-agent.stackdriver.com:8000'.
    config_param :metadata_agent_url, :string, :default => nil

    # Whether to split log entries with different log tags into different
    # requests when talking to Stackdriver Logging API.
    config_param :split_logs_by_tag, :bool, :default => false

    # Whether to attempt adjusting invalid log entry timestamps.
    config_param :adjust_invalid_timestamps, :bool, :default => true

    # Whether to autoformat value of "logging.googleapis.com/trace" to
    # comply with Stackdriver Trace format
    # "projects/[PROJECT-ID]/traces/[TRACE-ID]" when setting
    # LogEntry.trace.
    config_param :autoformat_stackdriver_trace, :bool, :default => true

    # rubocop:enable Style/HashSyntax

    # TODO: Add a log_name config option rather than just using the tag?

    # Expose attr_readers to make testing of metadata more direct than only
    # testing it indirectly through metadata sent with logs.
    attr_reader :project_id
    attr_reader :zone
    attr_reader :vm_id
    attr_reader :resource
    attr_reader :common_labels

    def initialize
      super
      # use the global logger
      @log = $log # rubocop:disable Style/GlobalVars
    end

    def configure(conf)
      super

      # TODO(qingling128): Remove this warning after the support is added. Also
      # remove the comment in the description of this configuration.
      unless @logging_api_url == DEFAULT_LOGGING_API_URL || @use_grpc
        @log.warn 'Detected customized logging_api_url while use_grpc is not' \
                  ' enabled. Customized logging_api_url for the non-gRPC path' \
                  ' is not supported. The logging_api_url option will be' \
                  ' ignored.'
      end

      # 1. If @metadata_agent_url is customized (aka not nil), use that.
      # 2. Otherwise check the presence of the environment variable
      #    STACKDRIVER_METADATA_AGENT_URL and use that if set.
      # 3. Fall back to the default if neither is set.
      if @enable_metadata_agent
        # Convert to string to capture empty string.
        @metadata_agent_url ||=
          if ENV[METADATA_AGENT_URL_ENV_VAR].to_s.empty?
            DEFAULT_METADATA_AGENT_URL
          else
            ENV[METADATA_AGENT_URL_ENV_VAR]
          end
      end

      # If monitoring is enabled, register metrics in the default registry
      # and store metric objects for future use.
      if @enable_monitoring
        registry = Monitoring::MonitoringRegistryFactory.create @monitoring_type
        @successful_requests_count = registry.counter(
          :stackdriver_successful_requests_count,
          'A number of successful requests to the Stackdriver Logging API')
        @failed_requests_count = registry.counter(
          :stackdriver_failed_requests_count,
          'A number of failed requests to the Stackdriver Logging API,'\
            ' broken down by the error code')
        @ingested_entries_count = registry.counter(
          :stackdriver_ingested_entries_count,
          'A number of log entries ingested by Stackdriver Logging')
        @dropped_entries_count = registry.counter(
          :stackdriver_dropped_entries_count,
          'A number of log entries dropped by the Stackdriver output plugin')
        @retried_entries_count = registry.counter(
          :stackdriver_retried_entries_count,
          'The number of log entries that failed to be ingested by the'\
            ' Stackdriver output plugin due to a transient error and were'\
            ' retried')
        @ok_code = @use_grpc ? GRPC::Core::StatusCodes::OK : 200
      end

      # Alert on old authentication configuration.
      unless @auth_method.nil? && @private_key_email.nil? &&
             @private_key_path.nil? && @private_key_passphrase.nil?
        extra = []
        extra << 'auth_method' unless @auth_method.nil?
        extra << 'private_key_email' unless @private_key_email.nil?
        extra << 'private_key_path' unless @private_key_path.nil?
        extra << 'private_key_passphrase' unless @private_key_passphrase.nil?

        raise Fluent::ConfigError,
              "#{PLUGIN_NAME} no longer supports auth_method.\n" \
              "Please remove configuration parameters: #{extra.join(' ')}"
      end

      set_regexp_patterns

      @platform = detect_platform

      # Treat an empty setting of the credentials file path environment variable
      # as unset. This way the googleauth lib could fetch the credentials
      # following the fallback path.
      ENV.delete(CREDENTIALS_PATH_ENV_VAR) if
        ENV[CREDENTIALS_PATH_ENV_VAR] == ''

      # Set required variables: @project_id, @vm_id, @vm_name and @zone.
      set_required_metadata_variables

      # Retrieve monitored resource.
      # Fail over to retrieve monitored resource via the legacy path if we fail
      # to get it from Metadata Agent.
      @resource ||= determine_agent_level_monitored_resource_via_legacy

      # Set regexp that we should match tags against later on. Using a list
      # instead of a map to ensure order. For example, tags will be matched
      # against Cloud Functions first, then GKE.
      @tag_regexp_list = []
      if @resource.type == GKE_CONSTANTS[:resource_type]
        # We only support Cloud Functions logs for GKE right now.
        if fetch_gce_metadata('instance/attributes/'
                             ).split.include?('gcf_region')
          # Fetch this info and store it to avoid recurring
          # metadata server calls.
          @gcf_region = fetch_gce_metadata('instance/attributes/gcf_region')
          @tag_regexp_list << [
            CLOUDFUNCTIONS_CONSTANTS[:resource_type],
            @compiled_cloudfunctions_tag_regexp
          ]
        end
        @tag_regexp_list << [
          GKE_CONSTANTS[:resource_type], @compiled_kubernetes_tag_regexp
        ]
      end

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

    # Determine what platform we are running on by consulting the metadata
    # service (unless the user has explicitly disabled using that).
    def detect_platform
      unless @use_metadata_service
        @log.info 'use_metadata_service is false; not detecting platform'
        return Platform::OTHER
      end

      begin
        open('http://' + METADATA_SERVICE_ADDR) do |f|
          if f.meta['metadata-flavor'] == 'Google'
            @log.info 'Detected GCE platform'
            return Platform::GCE
          end
          if f.meta['server'] == 'EC2ws'
            @log.info 'Detected EC2 platform'
            return Platform::EC2
          end
        end
      rescue StandardError => e
        @log.error 'Failed to access metadata service: ', error: e
      end

      @log.info 'Unable to determine platform'
      Platform::OTHER
    end

    def fetch_gce_metadata(metadata_path)
      raise "Called fetch_gce_metadata with platform=#{@platform}" unless
        @platform == Platform::GCE
      # See https://cloud.google.com/compute/docs/metadata
      open('http://' + METADATA_SERVICE_ADDR + '/computeMetadata/v1/' +
           metadata_path, 'Metadata-Flavor' => 'Google', &:read)
    end

    # EC2 Metadata server returns everything in one call. Store it after the
    # first fetch to avoid making multiple calls.
    def ec2_metadata
      raise "Called ec2_metadata with platform=#{@platform}" unless
        @platform == Platform::EC2
      unless @ec2_metadata
        # See http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
        open('http://' + METADATA_SERVICE_ADDR +
             '/latest/dynamic/instance-identity/document') do |f|
          contents = f.read
          @ec2_metadata = JSON.parse(contents)
        end
      end

      @ec2_metadata
    end

    # Set regexp patterns to parse tags and logs.
    def set_regexp_patterns
      @compiled_kubernetes_tag_regexp = Regexp.new(@kubernetes_tag_regexp) if
        @kubernetes_tag_regexp

      @compiled_cloudfunctions_tag_regexp =
        /\.(?<encoded_function_name>.+)\.\d+-[^-]+_default_worker$/
      @compiled_cloudfunctions_log_regexp = /^
        (?:\[(?<severity>.)\])?
        \[(?<timestamp>.{24})\]
        (?:\[(?<execution_id>[^\]]+)\])?
        [ ](?<text>.*)$/x

      @compiled_http_latency_regexp =
        /^\s*(?<seconds>\d+)(?<decimal>\.\d+)?\s*s\s*$/
    end

    # Set required variables like @project_id, @vm_id, @vm_name and @zone.
    def set_required_metadata_variables
      set_project_id
      set_vm_id
      set_vm_name
      set_location

      # All metadata parameters must now be set.
      missing = []
      missing << 'project_id' unless @project_id
      if @platform != Platform::OTHER
        missing << 'zone' unless @zone
        missing << 'vm_id' unless @vm_id
      end
      return if missing.empty?
      raise Fluent::ConfigError,
            "Unable to obtain metadata parameters: #{missing.join(' ')}"
    end

    # 1. Return the value if it is explicitly set in the config already.
    # 2. If not, try to retrieve it by calling metadata server directly.
    # 3. If still not set, try to obtain it from the credentials.
    def set_project_id
      @project_id ||= CredentialsInfo.project_id
      @project_id ||= fetch_gce_metadata('project/project-id') if
        @platform == Platform::GCE
    end

    # 1. Return the value if it is explicitly set in the config already.
    # 2. If not, try to retrieve it by calling metadata servers directly.
    def set_vm_id
      @vm_id ||= fetch_gce_metadata('instance/id') if @platform == Platform::GCE
      @vm_id ||= ec2_metadata['instanceId'] if @platform == Platform::EC2
    rescue StandardError => e
      @log.error 'Failed to obtain vm_id: ', error: e
    end

    # 1. Return the value if it is explicitly set in the config already.
    # 2. If not, try to retrieve it locally.
    def set_vm_name
      @vm_name ||= Socket.gethostname
    rescue StandardError => e
      @log.error 'Failed to obtain vm name: ', error: e
    end

    # 1. Return the value if it is explicitly set in the config already.
    # 2. If not, try to retrieve it locally.
    def set_location
      # Response format: "projects/<number>/zones/<zone>"
      @zone ||= fetch_gce_metadata('instance/zone').rpartition('/')[2] if
        @platform == Platform::GCE
      aws_location_key = if @use_aws_availability_zone
                           'availabilityZone'
                         else
                           'region'
                         end
      @zone ||= 'aws:' + ec2_metadata[aws_location_key] if
        @platform == Platform::EC2 && ec2_metadata.key?(aws_location_key)
    rescue StandardError => e
      @log.error 'Failed to obtain location: ', error: e
    end

    # Retrieve monitored resource via the legacy way.
    #
    # Note: This is just a failover plan if we fail to get metadata from
    # Metadata Agent. Thus it should be equivalent to what Metadata Agent
    # returns.
    def determine_agent_level_monitored_resource_via_legacy
      resource = Google::Apis::LoggingV2::MonitoredResource.new(
        labels: {})
      resource.type = determine_agent_level_monitored_resource_type
      resource.labels = determine_agent_level_monitored_resource_labels(
        resource.type)
      resource
    end

    # Determine agent level monitored resource type.
    def determine_agent_level_monitored_resource_type
      case @platform
      when Platform::OTHER
        # Unknown platform will be defaulted to GCE instance.
        return COMPUTE_CONSTANTS[:resource_type]

      when Platform::EC2
        return EC2_CONSTANTS[:resource_type]

      when Platform::GCE
        # Resource types determined by @subservice_name config.
        return SUBSERVICE_MAP[@subservice_name] if @subservice_name

        # Resource types determined by @detect_subservice config.
        if @detect_subservice
          begin
            attributes = fetch_gce_metadata('instance/attributes/').split.to_set
            SUBSERVICE_METADATA_ATTRIBUTES.each do |resource_type, expected|
              return resource_type if attributes.superset?(expected)
            end
          rescue StandardError => e
            @log.error 'Failed to detect subservice: ', error: e
          end
        end

        # GCE instance.
        return COMPUTE_CONSTANTS[:resource_type]
      end
    end

    # Determine agent level monitored resource labels based on the resource
    # type. Each resource type has its own labels that need to be filled in.
    def determine_agent_level_monitored_resource_labels(type)
      case type
      # GAE app.
      when APPENGINE_CONSTANTS[:resource_type]
        return {
          'module_id' =>
            fetch_gce_metadata('instance/attributes/gae_backend_name'),
          'version_id' =>
            fetch_gce_metadata('instance/attributes/gae_backend_version')
        }

      # GCE.
      when COMPUTE_CONSTANTS[:resource_type]
        return {
          'instance_id' => @vm_id,
          'zone' => @zone
        }

      # GKE container.
      when GKE_CONSTANTS[:resource_type]
        return {
          'instance_id' => @vm_id,
          'zone' => @zone,
          'cluster_name' =>
            fetch_gce_metadata('instance/attributes/cluster-name')
        }

      # Cloud Dataproc.
      when DATAPROC_CONSTANTS[:resource_type]
        return {
          'cluster_uuid' =>
            fetch_gce_metadata('instance/attributes/dataproc-cluster-uuid'),
          'cluster_name' =>
            fetch_gce_metadata('instance/attributes/dataproc-cluster-name'),
          'region' =>
            fetch_gce_metadata('instance/attributes/dataproc-region')
        }

      # EC2.
      when EC2_CONSTANTS[:resource_type]
        labels = {
          'instance_id' => @vm_id,
          'region' => @zone
        }
        labels['aws_account'] = ec2_metadata['accountId'] if
          ec2_metadata.key?('accountId')
        return labels
      end

      {}
    rescue StandardError => e
      @log.error "Failed to set monitored resource labels for #{type}: ",
                 error: e
      {}
    end

    # Determine the common labels that should be added to all log entries
    # processed by this logging agent.
    def determine_agent_level_common_labels
      labels = {}
      # User can specify labels via config. We want to capture those as well.
      labels.merge!(@labels) if @labels

      case @resource.type
      # GAE, Cloud Dataflow, Cloud Dataproc and Cloud ML.
      when APPENGINE_CONSTANTS[:resource_type],
           DATAFLOW_CONSTANTS[:resource_type],
           DATAPROC_CONSTANTS[:resource_type],
           ML_CONSTANTS[:resource_type]
        labels.merge!(
          "#{COMPUTE_CONSTANTS[:service]}/resource_id" => @vm_id,
          "#{COMPUTE_CONSTANTS[:service]}/resource_name" => @vm_name,
          "#{COMPUTE_CONSTANTS[:service]}/zone" => @zone
        )

      # GCE instance and GKE container.
      when COMPUTE_CONSTANTS[:resource_type],
           GKE_CONSTANTS[:resource_type]
        labels["#{COMPUTE_CONSTANTS[:service]}/resource_name"] = @vm_name

      # EC2.
      when EC2_CONSTANTS[:resource_type]
        labels["#{EC2_CONSTANTS[:service]}/resource_name"] = @vm_name
      end
      labels
    end

    # TODO: This functionality should eventually be available in another
    # library, but implement it ourselves for now.
    module CredentialsInfo
      # Determine the project ID from the credentials, if possible.
      # Returns the project ID (as a string) on success, or nil on failure.
      def self.project_id
        creds = Google::Auth.get_application_default(LOGGING_SCOPE)
        if creds.respond_to?(:project_id)
          return creds.project_id if creds.project_id
        end
        if creds.issuer
          id = extract_project_id(creds.issuer)
          return id unless id.nil?
        end
        if creds.client_id
          id = extract_project_id(creds.client_id)
          return id unless id.nil?
        end
        nil
      end

      # Extracts the project id (either name or number) from str and returns
      # it (as a string) on success, or nil on failure.
      #
      # Recognizes IAM format (account@project-name.iam.gserviceaccount.com)
      # as well as the legacy format with a project number at the front of the
      # string, terminated by a dash (-) which is not part of the ID, i.e.:
      # 270694816269-1l1r2hb813leuppurdeik0apglbs80sv.apps.googleusercontent.com
      def self.extract_project_id(str)
        [/^.*@(?<project_id>.+)\.iam\.gserviceaccount\.com/,
         /^(?<project_id>\d+)-/].each do |exp|
          match_data = exp.match(str)
          return match_data['project_id'] unless match_data.nil?
        end
        nil
      end
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
