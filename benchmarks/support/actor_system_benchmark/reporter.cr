require "./runner"
require "csv"

module Movie
  module Benchmarks
    module ActorSystem
      module Reporter
        extend self

        def emit(results : Array(Measurement), format : OutputFormat, io : IO = STDOUT) : Nil
          case format
          when OutputFormat::Human
            emit_human(results, io)
          when OutputFormat::Csv
            first = results.first? || return
            CSV.build(io) do |csv|
              csv.row first.csv_headers
              results.each { |measurement| csv.row measurement.csv_values }
            end
          when OutputFormat::JsonLines
            results.each { |measurement| JsonOutput.write_line(measurement, io) }
          else
            raise "unreachable output format"
          end
        end

        private def emit_human(results : Array(Measurement), io : IO)
          io.puts "ActorSystem end-to-end benchmark"
          results.each do |measurement|
            summary = "#{measurement.topology} #{measurement.operation} run=#{measurement.run}: " \
                      "#{measurement.messages_per_second.round(0)} msg/s, " \
                      "#{measurement.nanoseconds_per_message.round(0)} ns/msg, " \
                      "#{measurement.bytes_per_message.round(1)} client B/msg"
            if p99 = measurement.p99_nanoseconds
              summary += ", p99=#{(p99 / 1_000.0).round(1)} us"
            end
            if server_bytes = measurement.server_bytes_per_message
              summary += ", #{server_bytes.round(1)} server B/msg"
            end
            io.puts summary
          end

          results.group_by { |measurement| {measurement.topology, measurement.operation} }.each do |key, group|
            median = Statistics.median(group.map(&.messages_per_second))
            io.puts "median #{key[0]} #{key[1]}: #{median.round(0)} msg/s across #{group.size} runs"
          end
        end
      end

      module CLI
        extend self

        def run(arguments : Array(String)) : Int32
          Log.setup_from_env(default_level: :warn, backend: Log::IOBackend.new(STDERR))
          args = arguments.dup
          command = args.first? == "server" ? args.shift : nil
          config = Config.parse(args)

          if command == "server"
            ServerProcess.run(config)
          else
            Reporter.emit(Runner.new(config).run, config.output_format)
          end
          0
        rescue ex
          STDERR.puts "actor-system-benchmark: #{ex.message}"
          1
        end
      end
    end
  end
end
