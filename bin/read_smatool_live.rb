#!/usr/bin/env ruby

require_relative '../lib/billsnet_solar.rb'

require 'date'
require 'net/http'
require 'uri'
require 'thread'
require 'optparse'

# OptionParser.
class ProcessArgs
  # Parse args
  def self.parse
    # Default
    options = {
      debug: false,
      sleep: 60,
      data_dir: '/home/bill/work/git/solar',
      dir_date: '%Y%m',
      filename_date: '%Y%m%d',
      filename_prefix: 'thehahns_1-',
      filename_suffix: '.csv',
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

      opts.on('-l', '--sleep INTEGER', Integer, 'Thread sleep time in seconds default 60') do |sleep|
        options[:sleep] = sleep
      end

      opts.on('-i', '--data_dir path/dir', String, 'Date directory to check for new files') do |data_dir|
        options[:data_dir] = data_dir
      end

      opts.on('-e', '--dir_date DATE_FORMAT', String, 'Date format for monthly dir name.  default: \'%Y%m\'') do |dir_date|
        options[:dir_date] = dir_date
      end

      opts.on('-f', '--filename_date DATE_FORMAT', String, 'Date format in daily filename.  default: \'%Y%m%d\'') do |filename_date|
        options[:filename_date] = filename_date
      end

      opts.on('-r', '--file_prefix PREFIX', String, 'filename prefix default: \'thehahns_1-\'') do |filename_prefix|
        options[:filename_prefix] = filename_prefix
      end

      opts.on('-x', '--filename_suffix SUFFIX', String, 'filename suffix default: \'.csv\'') do |filename_suffix|
        options[:filename_suffix] = filename_suffix
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

# Enviroment variables override arguments
options[:debug] = true if ENV['DEBUG']
options[:sleep] = ENV['SLEEP'].to_i if ENV['SLEEP']
options[:data_dir] = ENV['DATA_DIR'] if ENV['DATA_DIR']
options[:dir_date] = ENV['DIR_DATE'] if ENV['DIR_DATE']
options[:filename_date] = ENV['FILENAME_DATE'] if ENV['FILENAME_DATE']
options[:filename_prefix] = ENV['FILENAME_PREFIX'] if ENV['FILENAME_PREFIX']
options[:filename_suffix] = ENV['FILENAME_SUFFIX'] if ENV['FILENAME_SUFFIX']

options[:influx_url] = ENV['INFLUX_URL'] if ENV['INFLUX_URL']
options[:influx_port] = ENV['INFLUX_PORT'] if ENV['INFLUX_PORT']
options[:influx_db_monthly] = ENV['INFLUX_DB_MONTHLY'] if ENV['INFLUX_DB_MONTHLY']
options[:influx_db_daily] = ENV['INFLUX_DB_DAILY'] if ENV['INFLUX_DB_DAILY']
options[:influx_db_spot] = ENV['INFLUX_DB_SPOT'] if ENV['INFLUX_DB_SPOT']
options[:influx_precision] = ENV['INFLUX_PRECISION'] if ENV['INFLUX_PRECISION']

puts "options: #{options}" if options[:debug]

##
#
# Loop infinity
#
##

loop {
  threads = []

  date_format = '%Y%m%d'
  yyyymm = Time.now.strftime(options[:dir_date])
  yyyymmdd = Time.now.strftime(options[:filename_date])
  sleep_sec = options[:sleep].to_i

  puts "#{yyyymmdd} - Starting Threads"
  args = {
    sleep: options[:sleep],
    dateFormat: date_format,
    data_dir: options[:data_dir],
    influx_url: options[:influx_url],
    influx_port: options[:influx_port],
    influx_precision: options[:influx_precision],
    debug: options[:debug]
  }
  solar = BillsnetSolarData.new(args)
  sleep(0.1)

  # Watch for date to change and stop threads
  args = {
    dateFormat: date_format,
    sleep: options[:sleep] * 2,
    debug: options[:debug]
  }
  threads << solar.watchDateChange(args)
  sleep(0.1)

  # Read in data from the daily solar file
  args = {
    file: "#{options[:data_dir]}/#{yyyymm}/#{options[:filename_prefix]}#{yyyymmdd}#{options[:filename_suffix]}",
    file_type: 'daily',
    influx_db: options[:influx_db_daily],
    debug: options[:debug]
  }
  threads << solar.tailFile(args)
  sleep(0.1)

  # Read in data from the spot solar file
  args = {
    file: "#{options[:data_dir]}/#{yyyymm}/#{options[:filename_prefix]}Spot-#{yyyymmdd}#{options[:filename_suffix]}",
    file_type: 'spot',
    influx_db: options[:influx_db_spot],
    debug: options[:debug]
  }
  threads << solar.tailFile(args)
  sleep(0.1)

  # Read in data from the month solar file
  args = {
    file: "#{options[:data_dir]}/#{yyyymm}/#{options[:filename_prefix]}#{yyyymm}#{options[:filename_suffix]}",
    file_type: 'monthly',
    influx_db: options[:influx_db_monthly],
    debug: options[:debug]
  }
  threads << solar.tailFile(args)
  sleep(0.1)

  solar.print

  puts "#{yyyymmdd} - #{Thread.current}"
  threads.each { |thr| thr.join }
  puts "#{yyyymmdd} - Ending Threads - #{Thread.current}"
  sleep(sleep_sec * 5)
}
