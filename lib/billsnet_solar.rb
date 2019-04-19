require_relative "../lib/billsnet_solar/version"

class BillsnetSolarData
  # Class to process solar data.

  def initialize(args = {})
    defaults = {
      sleep: 60,
      data_dir: '/var',
      influx_url: 'http://127.0.0.1',
      influx_port: '8086',
      influx_db: 'solar',
      influx_precision: 's',
      debug: false
    }
    args = defaults.merge(args)
    @lock = Mutex.new # For thread safety
    @config = args
    @state = true
    @sleep = args[:sleep].to_i
    @date = ''
    puts "initialize: date:#{@date}, config:#{@config}, args:#{args}" if args[:debug]
  end

  def getDataDetails(type = 'daily')
    data_details = {
      daily: {
        influx_db: 'solar-daily',
        dataType: 'daily',
        dateFormat: '%Y/%m/%d %H:%M:%S',
        dataRegex: %r{(^\d{4}\/\d\d\/\d\d \d\d:\d\d:\d\d);(.*)$},
        dataNames: %w[totalYield dayYield]
      },
      monthly: {
        influx_db: 'solar-monthly',
        dataType: 'monthly',
        dateFormat: '%Y/%m/%d',
        dataRegex: %r{(^\d{4}\/\d\d\/\d\d);(.*)$},
        dataNames: %w[totalYield dayYield]
      },
      spot: {
        influx_db: 'solar-spot',
        dataType: 'spot',
        dateFormat: '%Y/%m/%d %H:%M:%S',
        dataRegex: %r{(^\d{4}\/\d\d\/\d\d \d\d:\d\d:\d\d);(.*)$},
        dataNames: [
          'deviceName',
          'deviceType',
          'serial',
          'pdc1',
          'pdc2',
          'idc1',
          'idc2',
          'udc1',
          'udc2',
          'pac1',
          'pac2',
          'pac3',
          'iac1',
          'iac2',
          'iac3',
          'uac1',
          'uac2',
          'uac3',
          'pdcTot',
          'pacTot',
          'efficiency',
          'eToday',
          'eTotal',
          'frequency',
          'OperatingTime',
          'feedInTime',
          'btSignal',
          'condition',
          'gridRelay'
        ]
      }
    }
    data_details[type.to_sym]
  end

  def read_lines(args = {})
    defaults = {
      debug: false
    }
    defaults = defaults.merge!(@config) # Merge and overright defaults based on @config values
    args = defaults.merge(args)
    puts "read_lines: args: #{args}, defaults: #{defaults} @config: #{@config}" if args[:debug]

    args[:data] = '' # Blank out any old data
    bytes_read = 0
    args[:log].each_line do |line|
      puts "readline: line: #{line.chop}, line.bytesize: #{line.bytesize} bytes_to_skip: #{args[:bytes_to_skip]}" if args[:debug]
      bytes_read += line.bytesize

      line = line

      case line
      when /^sep=/
        args[:sep] = line.split('=')[-1].chomp
      when /^(;SN:\s.*).*/
        args[:deviceName] = line.split(/;/)[1].split(/:\s*/)[1].chomp
      when /^(;SB.*)/
        args[:deviceType] = line.split(/;/)[1].sub(/\s/, '_').chomp
      when /^(;\d+)/
        args[:deviceSerial] = line.split(';')[-1].chomp
        args[:deviceSerial] = args[:deviceName] if args[:deviceSerial] == ''
      when args[:dataRegex]
        args[:data] << line
      else
        puts "Skiping line: #{line} not normal pattern" if args[:debug]
      end
    end
    args[:bytes_to_skip] += bytes_read
    args
  end

  def formatInflux(args = {})
    defaults = {
      data: '',
      tz: DateTime.now.strftime('%Z'),
      debug: false
    }
    defaults = defaults.merge!(@config) # Merge and overright defaults based on @config values
    args = defaults.merge(args)
    puts "formatInflux: args: #{args}, defaults: #{defaults} @config: #{@config}" if args[:debug]


    postData = ''

    # Loop on each line
    puts "formatInflux: lenght:#{args[:data].split(/\n/).length}, data:#{args[:data].split('\n')}" if args[:debug]
    args[:data].split(/\n/).each do |line|
      m = args[:dataRegex].match(line)
      if m
        epoch = DateTime.strptime("#{m[1]} #{args[:tz]}", "#{args[:dateFormat]} %Z").to_time.to_i
        line_data = m[2].split(args[:sep])
        if line_data.length == 2
          line_data.each_with_index do |value, index|
            postData += "#{args[:dataNames][index]},deviceName=#{args[:deviceName]},deviceType=#{args[:deviceType]},deviceSerial=#{args[:deviceSerial]},dataType=#{args[:dataType]} value=#{value} #{epoch}\n"
          end
        else
          # Spot Data deviceName,deviceType,deviceSerial is on each line
          if line_data.length == 29
            args[:deviceName] = line_data[0].split(' ')[-1]
            line_data.shift
            args[:deviceType] = line_data[0].split(' ')[-1]
            line_data.shift
            args[:deviceSerial] = line_data[0]
            line_data.shift
          end

          line_data.each_with_index do |value, index|
            if args[:dataNames][index + 3] == 'gridRelay' || args[:dataNames][index + 3] == 'condition'
              case value
              when 'OK'
                value = '0'
              when '?'
                value = '1'
              else
                value = '99999'
              end
            end

            postData += "#{args[:dataNames][index+3]},deviceName=#{args[:deviceName]},deviceType=#{args[:deviceType]},deviceSerial=#{args[:deviceSerial]},dataType=#{args[:dataType]} value=#{value} #{epoch}\n"
          end
        end
      end
    end
    puts "formatInflux: args:#{args}, postData: #{postData}" if args[:debug]
    postData
  end

  def sendInflux(args = {})
    defaults = {
      influx_url: 'http://127.0.0.1',
      influx_port: '8086',
      influx_db: 'solar',
      influx_precision: 's',
      debug: false
    }
    defaults = defaults.merge!(@config) # Merge and overright defaults based on @config values
    args = defaults.merge(args)
    puts "sendInflux: args: #{args}, defaults: #{defaults} @config: #{@config}" if args[:debug]

    uri = URI.parse("#{args[:influx_url]}:#{args[:influx_port]}/write?db=#{args[:influx_db]}&precision=#{args[:influx_precision]}")
    request = Net::HTTP::Post.new(uri)
    request.body = args[:data]

    req_options = {
      use_ssl: uri.scheme == 'https'
    }

    response = Net::HTTP.start(uri.hostname, uri.port, req_options) do |http|
      http.request(request)
    end

    if response.code == 204
      puts "File Uploaded to influx #{response.code}"
    else
      puts "sendInflux: file: #{args[:file]}, uri: #{uri}, status code: #{response.code}, body: #{response.body}"
    end
  end

  def print()
    puts "print: date: #{@date}, @config: #{@config}, @state: #{@state}, @sleep: #{@sleep}, @lock: #{@lock}"
  end

  def get_state
    @state
  end

  def setState(state)
    @lock.synchronize {
      @state = state
    }
  end

  def get_date
    @date
  end

  def setDate(date)
    @lock.synchronize {
      @date = date
    }
  end

  # watch for date change
  def watchDateChange(args = {})
    defaults = {
      sleep: 60,
      dateFormat: '%Y%m%d',
      debug: false
    }
    defaults = defaults.merge!(@config) # Merge and overright defaults based on @config values
    args = defaults.merge(args)
    puts "watchDateChange: args: #{args}, defaults: #{defaults} @config: #{@config}" if args[:debug]

    thr = Thread.new {
      loop {
        current_date = Time.now.strftime(args[:dateFormat].to_s)

        setDate(current_date) if @date == ''

        if current_date != @date
          setState(false)
          setDate('EXIT')
          puts "watchDateChange: date change detected stop(#{@state}) threads: date:#{@date}, date_now:#{current_date}" if args[:debug]
          break
        end
        puts "watchDateChange: state: #{@state}, date:#{@date} != current_date:#{current_date}" if args[:debug]
        sleep(args[:sleep])
      }
    }
    thr
  end

  def tailFile(args = {})
    defaults = {
      file: "/opt/solar/#{Time.now.strftime('%Y%m')}/thehahns_1-#{Time.now.strftime('%Y%m%d')}.csv",
      file_type: 'daily', # spot|monthly|daily
      sleep: 60,
      debug: false
    }
    defaults = defaults.merge!(@config) # Merge and overright defaults based on @config values
    args = defaults.merge(args)
    puts "tailFile: Looping - @state: #{@state}, args: #{args}, defaults: #{defaults} @config: #{@config}" if args[:debug]

    Thread.new {
      dataDetails = getDataDetails(args[:file_type])
      dataDetails[:log] = ''
      dataDetails[:bytes_to_skip] = 0

      loop {
        if @state
          puts "tailFile: Looping - @state: #{@state}, args: #{args}, defaults: #{defaults} @config: #{@config}" if args[:debug]
          if File.file?(args[:file])
            puts "tailFile: found #{args[:file]}, log: #{dataDetails[:log]}" if args[:debug]
            if dataDetails[:log] == ''  # Open file handle if not open already
              dataDetails[:log] = File.open(args[:file])
              puts "tailFile: opening handle #{args[:file]} #{dataDetails[:log]}" if args[:debug]
            end

            # Check if lines need to be read
            log_file_size = dataDetails[:log].stat.size
            dataDetails[:bytes_to_skip] = 0 if log_file_size < dataDetails[:bytes_to_skip] # If the file shrink reset back to begining
            dataDetails[:log].seek(dataDetails[:bytes_to_skip], File::SEEK_SET)
            puts "tailFile: log_file_size: #{log_file_size}, bytes_to_skip: #{dataDetails[:bytes_to_skip]}" if args[:debug]
            dataDetails = read_lines(dataDetails)

            # Format the read in date for influx
            influx_data = formatInflux(sep: dataDetails[:sep],
                                       dataRegex:  dataDetails[:dataRegex],
                                       dateFormat: dataDetails[:dateFormat],
                                       deviceName: dataDetails[:deviceName],
                                       deviceType: dataDetails[:deviceType],
                                       deviceSerial: dataDetails[:deviceSerial],
                                       dataType: args[:file_type],
                                       dataNames: dataDetails[:dataNames],
                                       data: dataDetails[:data],
                                       debug: args[:debug])

            # Send the formated influx data to the influx server
            sendInflux(data: influx_data,
                       debug: args[:debug]) if influx_data != ''

          else
            puts "tailFile: Not found #{args[:file]}" if args[:debug]
          end
        else
          puts "tailFile: Exit loop @state: #{@state}" if args[:debug]
          Thread.exit
        end
        sleep(args[:sleep])
      }
    }
  end

  def read_file(args = {})
    defaults = {
      file: "/opt/solar/#{Time.now.strftime('%Y%m')}/thehahns_1-#{Time.now.strftime('%Y%m%d')}.csv",
      file_type: 'daily', # spot|monthly|daily
      debug: false
    }
    defaults = defaults.merge!(@config) # Merge and overright defaults based on @config values
    args = defaults.merge(args)
    puts "read_file: args: #{args}, defaults: #{defaults} @config: #{@config}" if args[:debug]

    dataDetails = getDataDetails(args[:file_type])

    if File.file?(args[:file])
      puts "read_file: found #{args[:file]}, dataDetails: #{dataDetails}" if args[:debug]
      dataDetails[:log] = File.open(args[:file])
      puts "read_file: opening handle #{args[:file]} #{dataDetails[:log]}" if args[:debug]

      # Check if lines need to be read
      dataDetails[:bytes_to_skip] = 0
      dataDetails[:log].seek(0, File::SEEK_SET)
      puts "read_file: before readline dataDetails: #{dataDetails}" if args[:debug]
      dataDetails = read_lines(dataDetails)
      puts "read_file: after readLine dataDetails: #{dataDetails}" if args[:debug]

      # Format the read in date for influx
      influx_data = formatInflux(sep: dataDetails[:sep],
                                 dataRegex:  dataDetails[:dataRegex],
                                 dateFormat: dataDetails[:dateFormat],
                                 deviceName: dataDetails[:deviceName],
                                 deviceType: dataDetails[:deviceType],
                                 deviceSerial: dataDetails[:deviceSerial],
                                 dataType: dataDetails[:dataType],
                                 dataNames: dataDetails[:dataNames],
                                 data: dataDetails[:data],
                                 debug: args[:debug])

      # Send the formated influx data to the influx server
      sendInflux(data: influx_data,
                 file: args[:file],
                 debug: args[:debug]) if influx_data != ''

    else
      puts "read_file: Not found #{args[:file]}" if args[:debug]
    end
  end

  def threadDetails
    thr = Thread.new {
      loop {
        if @state
          puts "\tthreadDetails: Current thread = #{Thread.current}"
          puts "\tthreadDetails: #{Thread.current}"
          puts "\tthreadDetails: #{@config}"
        else
          break
        end
        sleep(@sleep)
      }
    }
    thr
  end
end
