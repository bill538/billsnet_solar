#!/usr/bin/env ruby

require_relative '../lib/billsnet_solar.rb'

require 'date'
require 'net/http'
require 'uri'
require 'thread'
require 'optparse'

filename = ARGV[0].split(%r{\/})
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

solar.read_file(file: ARGV[0],
                file_type: file_type, # spot|monthly|daily
                debug: debug)
