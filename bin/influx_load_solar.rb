#!/usr/bin/env ruby

require_relative '../lib/billsnet_solar.rb'

require 'date'
require 'net/http'
require 'uri'
#require 'thread'
require 'optparse'

# OptionParser.
class ProcessArgs
  # Parse args
  def self.parse
    # Default
    options = {
      debug: false,
      filename: '',
      influx_url: 'http://127.0.0.1',
      influx_port: 8086,
      influx_db_monthly: 'solar-monthly',
      influx_db_daily: 'solar-daily',
      influx_db_spot: 'solar-spot',
      influx_precision: 's'
    }

    OptionParser.new do |opts|
      # Set a banner, displayed at the top
      # of the help screen.
      opts.banner = "Usage: #{ARGV[0]}"

      opts.on('-d', '--debug', TrueClass, 'Enable additonal debugging output') do
        options[:debug] = true
      end

      opts.on('-f', '--filename FILENAME', String, 'Path/filename to load into influx') do |filename|
        options[:filename] = filename
      end

      opts.on('-u', '--influx_url URL', String, 'influx url default: \'http://127.0.0.1\'') do |influx_url|
        options[:influx_url] = influx_url
      end

      opts.on('-p', '--influx_port INTEGER', Integer, 'influx port default: \'8086\'') do |influx_port|
        options[:influx_port] = influx_port
      end

      opts.on('-m', '--influx_db_monthly DBNAME', String, 'influx db monthly name default: \'solar-monthly\'') do |influx_db_monthly|
        options[:influx_db_monthly] = influx_db_monthly
      end

      opts.on('-a', '--influx_db_daily DBNAME', String, 'influx db daily name default: \'solar-daily\'') do |influx_db_daily|
        options[:influx_db_daily] = influx_db_daily
      end

      opts.on('-s', '--influx_db_spot DBNAME', String, 'influx db spot name default: \'solar-spot\'') do |influx_db_spot|
        options[:influx_db_spot] = influx_db_spot
      end

      opts.on('-n', '--influx_precision STRING', String, 'influx precision default: \'s\'') do |influx_precision|
        options[:influx_precision] = influx_precision
      end

      # This displays the help screen, all programs are
      # assumed to have this option.
      opts.on('-h', '--help', 'Display this screen') do
        puts opts
        exit
      end
    end.parse!(into: options)

    options
  end
end

#
# Main
#

options = ProcessArgs.parse
puts "options: #{options}" if options[:debug]

if options[:filename] == '' 
  puts "#{$0} --filename argument is required"
  exit 1
end

filename = options[:filename].split(%r{\/})
debug = true

# Determine the SMASpot file type
file_type = if filename[-1] =~ /^.*-\d{6}.csv$/
              'monthly'
            elsif filename[-1] =~ /^.*-Spot-\d{8}.csv$/
              'spot'
            else
              'daily'
            end

# Read device details from the file
solar = BillsnetSolarData.new(debug: debug)

solar.read_file(file: options[:filename],
                file_type: file_type, # spot|monthly|daily
                debug: debug)
