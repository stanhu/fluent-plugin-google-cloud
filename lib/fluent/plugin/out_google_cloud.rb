require 'time'

module Fluent
  class GoogleCloudOutput < BufferedOutput
    Fluent::Plugin.register_output('google_cloud', self)

    def write(chunk)
      log_count = 0
      chunk.msgpack_each do |tag, time, record|
        log_count += 1
      end
      $log.info "Wrote a chunk of #{log_count} records."

      # Simply sleeps for 0.06 seconds.
      sleep(0.06)
    end

    def format(tag, time, record)
      Fluent::Engine.msgpack_factory.packer.write([tag, time, record]).to_s
    end
  end
end
