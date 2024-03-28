# frozen_string_literal: true

require 'json'
require 'csv'
require 'tiny_tds'
require 'logger'
require 'sequel'
require 'connection_pool'

# Class to access lead and job status databases.
class Database
  # Class-Instance var @classconfig -- this is to test only, not really used for anything except populating the
  # Instance variable upon initialization.
  @classconfig = nil
  class << self
    attr_accessor :classconfig
  end

  # Instance vars
  @logger = nil
  @dbconnection = nil
  @tdspool = nil
  @sequeldb = nil
  @config = nil
  attr_accessor :dbconnection, :logger, :sequeldb, :config, :tdspool

  def initialize(logger)
    progname = "#{Thread.current.object_id}|Database::initialize"
    LOGGER.info(progname) { 'Start' }
    @logger = logger
    @config = self.class.classconfig

    # Create a TDS pool of connections.
    @tdspool = ConnectionPool.new(size: 10, timeout: 10) do
      LOGGER.info(progname) { 'Tds connection pool invoked' }
      dbconnection = TinyTds::Client.new username: @config['sql_user'], password: @config['sql_password'],
                                         host: @config['sql_server'], port: @config['sql_port'],
                                         database: @config['sql_database'], azure: false
    rescue TinyTds::Error => e
      puts "Error #{e.inspect}"
      exit(-1)
    else
      dbconnection.execute('SET ANSI_NULLS ON').do
      dbconnection.execute('SET CURSOR_CLOSE_ON_COMMIT OFF').do
      dbconnection.execute('SET ANSI_NULL_DFLT_ON ON').do
      dbconnection.execute('SET IMPLICIT_TRANSACTIONS OFF').do
      dbconnection.execute('SET ANSI_PADDING ON').do
      dbconnection.execute('SET QUOTED_IDENTIFIER ON').do
      dbconnection.execute('SET ANSI_WARNINGS ON').do
      dbconnection.execute('SET CONCAT_NULL_YIELDS_NULL ON').do
      dbconnection
    end
    LOGGER.info(progname) { "Tds connection pool size: #{@tdspool.size}" }
    LOGGER.info(progname) { "Tds connection pool available: #{@tdspool.available}" }

    # TinyTDS connection. Not really needed once the above pool is used everywhere.
    begin
      @dbconnection = TinyTds::Client.new username: @config['sql_user'], password: @config['sql_password'],
                                          host: @config['sql_server'], port: @config['sql_port'],
                                          database: @config['sql_database'], azure: false
    rescue TinyTds::Error => e
      puts "Error #{e.inspect}"
      exit(-1)
    else
      @dbconnection.execute('SET ANSI_NULLS ON').do
      @dbconnection.execute('SET CURSOR_CLOSE_ON_COMMIT OFF').do
      @dbconnection.execute('SET ANSI_NULL_DFLT_ON ON').do
      @dbconnection.execute('SET IMPLICIT_TRANSACTIONS OFF').do
      @dbconnection.execute('SET ANSI_PADDING ON').do
      @dbconnection.execute('SET QUOTED_IDENTIFIER ON').do
      @dbconnection.execute('SET ANSI_WARNINGS ON').do
      @dbconnection.execute('SET CONCAT_NULL_YIELDS_NULL ON').do

      LOGGER.info(progname) { "sent?: #{@dbconnection.sqlsent?}" }
      LOGGER.info(progname) { "canceled?: #{@dbconnection.canceled?}" }
    end

    # Sequel connection -- alternative way to connect a database.
    begin
      db_connection_params = {
        adapter: 'tinytds',
        host: @config['sql_server'], # IP or hostname
        port: @config['sql_port'], # Required when using other that 1433 (default)
        database: @config['sql_database'],
        user: @config['sql_user'],
        password: @config['sql_password'],
        log_connection_info: true,
        sql_log_level: 'debug'
        #  logger: LOGGER
      }
      # Sequel connection
      @sequeldb = Sequel.connect(db_connection_params)
    rescue Sequel::Error => e
      puts "Error #{e.inspect}"
      exit(-1)
    end
    LOGGER.info(progname) { 'End' }
  end

  def test_access
    puts '***********START SQL CONNECT TEST***********'
    begin
      # Test generic SQL clause
      puts 'BEGIN TINYTDS TESTS'
      pp [:Q1, :sent, @dbconnection.sqlsent?, :canceled, @dbconnection.canceled?]
      results = @dbconnection.execute("USE [#{@config['sql_database']}]")
      pp [:RETCODE1, results.return_code]
      pp [:FIELDS1, results.fields]

      # Test how TinyTDS do works.
      pp [:Q2, :sent, @dbconnection.sqlsent?, :canceled, @dbconnection.canceled?]
      # With TinyTds, results chould be "do" or "cancel" to free the connection for new executions.
      pp [:DO2, results.do]
      pp [:Q2, :sent, @dbconnection.sqlsent?, :canceled, @dbconnection.canceled?]

      # Test real select
      results = @dbconnection.execute("SELECT TOP (3) * FROM #{@config['sql_statustable']}
      WHERE jobSource = 'ruby' ORDER BY createdAt")
      pp [:RETCODE3, results.return_code]
      p [:FIELDS3, results.fields]
      pp [:Q3, :sent, @dbconnection.sqlsent?, :canceled, @dbconnection.canceled?]
      results.each do |row|
        pp [:ROW3, row['exportId'], row['startDate']]
      end
      pp [:Q3, :sent, @dbconnection.sqlsent?, :canceled, @dbconnection.canceled?]

      # Test insert.
      t = Time.now
      curr_date = t.strftime('%Y-%m-%d %H:%M:%S.%L')
      rnd = (0...45).map { rand(65..90).chr }.join

      results = @dbconnection.execute("INSERT #{@config['sql_database']}.dbo.#{@config['sql_conntesttable']}
      (timestamp, id) OUTPUT INSERTED.id, INSERTED.timestamp VALUES ('#{curr_date}', '#{rnd}' )")
      pp [:RETURNCODE4, results.return_code]
      p [:FIELDS4, results.fields]
      pp [:Q4, :sent, @dbconnection.sqlsent?, :canceled, @dbconnection.canceled?]
    rescue TinyTds::Error => e
      puts "Error #{e.inspect}"
    else
      results.each do |row|
        p [:ROW4, row]
      end
      pp [:Q4, :sent, @dbconnection.sqlsent?, :canceled, @dbconnection.canceled?]
      # Cancel not really needed as the rows were done above.
      results.cancel
      pp [:Q4, :sent, @dbconnection.sqlsent?, :canceled, @dbconnection.canceled?]
    end

    # Test connection pool.
    puts 'BEGIN TINYTDS POOL TESTS'
    @tdspool.with do |connection|
      t = Time.now
      curr_date = t.strftime('%Y-%m-%d %H:%M:%S.%L')
      rnd = (0...45).map { rand(65..90).chr }.join

      pp connection.class.name
      pp "Tds connection pool size: #{@tdspool.size}"
      pp "Tds connection pool available: #{@tdspool.available}"
      results = connection.execute("INSERT #{@config['sql_database']}.dbo.#{@config['sql_conntesttable']}
      (timestamp, id) OUTPUT INSERTED.id, INSERTED.timestamp VALUES ('#{curr_date}', '#{rnd}' )")
      pp [:RETURNCODE5, results.return_code]
      p [:FIELDS5, results.fields]
      pp [:Q5, :sent, @dbconnection.sqlsent?, :canceled, @dbconnection.canceled?]
      results.each do |row|
        p [:ROW5, row]
      end
    end
    pp "Tds connection pool size: #{@tdspool.size}"
    pp "Tds connection pool available: #{@tdspool.available}"

    # Test the sequel way as well.
    puts 'BEGIN SEQUEL TESTS'
    curr_date = Time.now.strftime('%Y-%m-%d %H:%M:%S.%L')
    rnd = (0...45).map { rand(65..90).chr }.join
    begin
      dataset = @sequeldb[(@config['sql_conntesttable']).to_sym]
      results = dataset.all
      dataset.insert(timestamp: curr_date, id: rnd)
    rescue Sequel::Error, TinyTds::Error => e
      puts "Error #{e.inspect}"
    else
      results.take(3).each { |row| pp [:ROW5, row] }
    end

    puts '************END SQL CONNECT TEST************'
  end

  # Function to check database connection availability.
  def access_ok?
    progname = "#{Thread.current.object_id}|Database::access_ok?"
    LOGGER.debug(progname) { "Before: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
    @tdspool.with do |connection|
      if connection.sqlsent? == true
        LOGGER.error(progname) { 'TinyTDS Access NOK' }
        return false
      end
      LOGGER.debug(progname) { "During: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
    end
    LOGGER.debug(progname) { "After: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
    LOGGER.info(progname) { 'TinyTDS Access OK' }

    curr_date = Time.now.strftime('%Y-%m-%d %H:%M:%S.%L')
    rnd = (0...45).map { rand(65..90).chr }.join
    begin
      table = @sequeldb[(@config['sql_conntesttable']).to_sym]
      table.insert(timestamp: curr_date, id: rnd)
      return false unless table.where(id: rnd).count == 1
    rescue Sequel::Error, TinyTds::Error => e
      puts "Error #{e.inspect}"
      LOGGER.error(progname) { 'Sequel Access NOK' }
      return false
    else
      LOGGER.info(progname) { 'Sequel Access OK' }
    end
    true
  end

  # Get list of all jobs in DB.
  def jobs(type, printresponse)
    progname = "#{Thread.current.object_id}|Database::jobs"
    LOGGER.info(progname) { 'Start' }

    data = []
    query = case type
            when 'nextready'
              <<~SQL
                SELECT TOP (1)
                  exportId, createdAt, status, numberOfRecords, importStarted, importedAt, jobSource, jobType, monthDelta
                  FROM [MarketoLeads].[dbo].[JobStatus]
                  WHERE importStarted is null and jobType = 'fullsync' and status = 'completed'
                  ORDER BY createdAt
              SQL
            when 'noncleared'
              "SELECT * FROM \"JobStatus\" WHERE status != 'cleared' AND \"jobSource\" = 'ruby'"
            when 'all'
              'SELECT * FROM "JobStatus" WHERE "jobSource" = \'ruby\''
            else
              "SELECT * FROM \"JobStatus\" WHERE jobType = '#{type}' AND \"jobSource\" = 'ruby'"
            end
    LOGGER.debug(progname) { "query: #{query}" } if $debugmode

    begin
      LOGGER.debug(progname) { "Before: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
      @tdspool.with do |connection|
        results = connection.execute(query.to_s)
        LOGGER.debug(progname) { "During: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
        unless results
          LOGGER.error(progname) { 'TinyTds returned false' }
          raise StandardError, 'TinyTds returned false'
        end
        results.each do |row|
          data << row
          LOGGER.debug(progname) { "row: #{row}" } if $responsedebug
          # p [:jobs, :row, row] if printresponse
        end
        # Cancel is probably not necessary because all lines were processed above.
        results.cancel
      end
      LOGGER.debug(progname) { "After: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
    rescue TinyTds::Error => e
      LOGGER.error(progname) { 'TinyTds error' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    rescue StandardError => e
      LOGGER.error(progname) { 'Standard error' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    end

    if printresponse
      print 'export ID'.to_s.ljust(40, ' ')
      print 'startDate'.to_s.ljust(20, ' ')
      print 'endDate'.to_s.ljust(20, ' ')
      print 'status'.to_s.ljust(12, ' ')
      print 'importStarted'.to_s.ljust(20, ' ')
      print 'importedAt'.to_s.ljust(20, ' ')
      print 'jobSource'.to_s.ljust(13, ' ')
      print 'jobType'.to_s.ljust(13, ' ')
      print 'monthDelta'.to_s.ljust(15, ' ')
      print 'numberOfRecords'.to_s.ljust(20, ' ')
      print 'fileSize'.to_s.ljust(10, ' ')
      puts 'fullSyncComplete'.to_s.ljust(10, ' ')
      data.each do |line|
        print line['exportId'].to_s.ljust(40, ' ')
        if line['startDate'].instance_of?(Time)
          print line['startDate'].strftime('%y-%m-%d %H:%M').to_s.ljust(20, ' ')
        else
          print line['startDate'].to_s.ljust(20, ' ')
        end
        if line['endDate'].instance_of?(Time)
          print line['endDate'].strftime('%y-%m-%d %H:%M').ljust(20, ' ')
        else
          print line['endDate'].to_s.ljust(20, ' ')
        end
        print line['status'].to_s.ljust(12, ' ')
        if line['importStarted'].instance_of?(Time)
          print line['importStarted'].strftime('%y-%m-%d %H:%M').ljust(20, ' ')
        else
          print line['importStarted'].to_s.ljust(20, ' ')
        end
        if line['importedAt'].instance_of?(Time)
          print line['importedAt'].strftime('%y-%m-%d %H:%M').ljust(20, ' ')
        else
          print line['importedAt'].to_s.ljust(20, ' ')
        end
        print line['jobSource'].to_s.ljust(13, ' ')
        print line['jobType'].to_s.ljust(13, ' ')
        print line['monthDelta'].to_s.ljust(15, ' ')
        print line['numberOfRecords'].to_s.ljust(20, ' ')
        print line['filesize'].to_s.ljust(10, ' ')
        puts line['fullSyncComplete'].to_s.ljust(10, ' ')
      end
    end

    # Return array of rows.
    LOGGER.info(progname) { "Number of results: #{data.length}" }
    LOGGER.info(progname) { 'End' }
    data
  end

  # Helper function to get the last job of each full sync.
  def fullsyncsummary(indata)
    progname = "#{Thread.current.object_id}|Database::fullsyncsummary"
    LOGGER.info(progname) { 'Start' }

    # Get array of unique fullsyncids.
    fullsyncs = indata.filter_map { |r| r['fullSyncId'] if defined?(r['fullSyncId']) }
    fullsyncs.uniq!
    LOGGER.debug(progname) { "Full syncs: #{fullsyncs}" }

    outdata = []
    # Go through the data for each id, find the row with largest monthdelta.
    fullsyncs.each do |fs|
      # Choose the jobs of this specific full sync cycle.
      fsrows = indata.select { |row| row['fullSyncId'] == fs }
      # LOGGER.debug('Database::fullsyncsummary') { "fsrows: #{fsrows}" }
      # Find the latest job of this full sync cycle.
      latest = fsrows.max { |a, b| a['monthDelta'] <=> b['monthDelta'] }
      # For logging, select the hash key-values included in the given static list of interesting values.
      LOGGER.info(progname) do
        latest.select { |k| %w[exportId fullSyncId fullSyncComplete].include?(k) }
      end
      outdata << latest
    end
    LOGGER.info(progname) { 'End' }
    outdata
  end

  # Update job status database.
  def refresh_status_table(mkto)
    progname = "#{Thread.current.object_id}|Database::refresh_status_table"
    LOGGER.info(progname) { 'Start' }

    begin
      dbjobs = jobs('noncleared', false)
    rescue StandardError => e
      LOGGER.error(progname) { 'Error raised' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    end

    unless dbjobs
      LOGGER.error(progname) { 'Failed to get jobs from DB' }
      raise StandardError, 'Failed to get jobs from DB'
    end

    dbjobs.each do |job|
      LOGGER.info(progname) { "Start processing job from DB: #{job['exportId']}" }
      LOGGER.info(progname) { "Job from DB: #{job}" } if $debugmode
      begin
        job_details = mkto.bulk_job_status(job['exportId'])
      rescue StandardError => e
        LOGGER.error(progname) { 'Error raised by mkto.bulk_job_status' }
        LOGGER.error(progname) { "Error: #{e.inspect}" }
        raise
      end

      unless job_details
        LOGGER.error(progname) { 'Failed to get job details from Marketo' }
        raise StandardError, 'Failed to get job details from Marketo'
      end

      LOGGER.debug(progname) { "Job from Marketo: #{job_details['requestId']}" }
      LOGGER.debug(progname) { "Job from Marketo: #{job_details}" } if $debugmode

      # Before else, check we can access the status DB
      unless access_ok?
        LOGGER.error(progname) { 'Database not available' }
        raise StandardError, 'Database not available'
      end

      # First find out what kind of status we got from Marketo. Default 'none'. If errors, 'error'. If the special
      # case of not found error, 'notfound'. If we Martketo specifically reported success, 'success'.
      job_status = 'none'
      if job_details.key?('errors')
        job_status = 'error'
        LOGGER.debug(progname) { "Job errors: #{job_details['errors']}" }
        job_details['errors'].each do |error|
          if error.key?('message') && error['message'].include?('not found')
            LOGGER.debug(progname) { "Job not found in mkto: #{job['exportId']}" }
            job_status = 'notfound'
          end
        end
      elsif job_details['success'] == true
        job_status = 'success'
      end
      LOGGER.info(progname) { "Result of Marketo job query: #{job['exportId']}: #{job_status}" }

      # Succesfully found data from Marketo.
      LOGGER.debug(progname) { "Before: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
      @tdspool.with do |connection|
        case job_status
        when 'success'
          results = connection.execute("UPDATE MarketoLeads.dbo.JobStatus
                SET status = '#{job_details['result'][0]['status']}',
                numberOfRecords = '#{job_details['result'][0]['numberOfRecords']}',
                filesize = '#{job_details['result'][0]['fileSize']}'
                OUTPUT INSERTED.exportId, INSERTED.status, INSERTED.numberOfRecords, INSERTED.filesize
                WHERE exportId = '#{job['exportId']}'")
        when 'notfound'
          results = connection.execute("UPDATE MarketoLeads.dbo.JobStatus
                SET status = 'Cleared'
                OUTPUT INSERTED.exportId, INSERTED.status
                WHERE exportId = '#{job['exportId']}'")
        else
          next
        end
        LOGGER.debug(progname) { "During: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
      rescue TinyTds::Error => e
        LOGGER.error(progname) { 'TinyTds error' }
        LOGGER.error(progname) { "Error: #{e.inspect}" }
        raise
      rescue StandardError => e
        LOGGER.error(progname) { 'TinyTds standard error' }
        LOGGER.error(progname) { "Error: #{e.inspect}" }
        raise
      else
        results.each do |row|
          # p [:row, row]
          LOGGER.debug(progname) { "Inserted/updated to database: #{row}" }
        end
        # Check tinytds documentation -- is this correct way of completing update?
        results.do
      end
      LOGGER.debug(progname) { "After: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
    end
    true
  end

  # Insert new job data to the status DB
  def upsert_job(job, type)
    progname = "#{Thread.current.object_id}|Database::upsert_job"
    LOGGER.info(progname) { "Start #{type}" }
    # Check we can access the status DB
    unless access_ok?
      LOGGER.fatal('Database::upsert_job') { 'SQL connection not available!' }
      raise StandardError, 'SQL connection not available!'
    end

    LOGGER.debug(progname) { "Upsert data into DB: #{job}" } if $debugmode
    LOGGER.info(progname) { "Upsert job into DB, id: #{job[:exportId]}" }

    unless job.key?(:exportId)
      LOGGER.error(progname) { 'Did not get job exportId which is mandatory' }
      raise StandardError, 'Did not get job exportId which is mandatory'
    end

    # ~ below probably takes care of removing extra indent.
    case type
    when 'insert'
      query = <<~SQL
        INSERT MarketoLeads.dbo.JobStatus (#{job.keys.join(', ')}) \
        OUTPUT INSERTED.exportId VALUES ( #{job.values.map { |value| "'#{value}'" }.join(', ')} )
      SQL
    when 'update'
      # We do not want to update exportId but use it as selector instead. Reject it from data.
      data = job.reject { |k| k == :exportId }
      LOGGER.debug(progname) { "updated job: #{job[:exportId]} data: #{data}" }
      query = <<~SQL
        UPDATE MarketoLeads.dbo.JobStatus SET #{data.map { |k, v| "#{k}='#{v}'" }.join(', ')} \
        OUTPUT INSERTED.exportId \
        WHERE exportId = '#{job[:exportId]}'
      SQL
    else
      LOGGER.error(progname) { 'Database operation type incorrect' }
      raise StandardError, 'Database operation type incorrect'
    end
    LOGGER.debug(progname) { "SQL query: #{query}" } if $debugmode

    # Update bulk job status db and return the parsed response.
    begin
      results = nil
      LOGGER.debug(progname) { "Before: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
      @tdspool.with do |connection|
        LOGGER.debug(progname) { "During: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
        results = connection.execute(query.to_s)
        case type
        when 'insert'
          results.insert
        else
          results.do
        end
      end
      LOGGER.debug(progname) { "After: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
    rescue TinyTds::Error => e
      LOGGER.fatal('Database::upsert_job') { "SQL error: #{e.inspect}" }
      raise
    end
    LOGGER.debug(progname) { "SQL results: #{results}" }
    LOGGER.info(progname) { "SQL result rows: #{results.affected_rows}" }
    results.affected_rows
  end

  # Empty the temp table.
  def delete_temp_table
    progname = "#{Thread.current.object_id}|Database::delete_temp_table"
    LOGGER.info(progname) { 'Start' }
    unless access_ok?
      LOGGER.error(progname) { 'Database not available' }
      return false
    end

    begin
      @sequeldb[(@config['sql_temptable']).to_sym].delete
    rescue Sequel::Error => e
      puts "Error #{e.inspect}"
      exit(-1)
    end
    true
  end

  # Copy TSV contents to temp table.
  def tsv2db(export_id)
    progname = "#{Thread.current.object_id}|Database::tsv2db"
    LOGGER.info(progname) { "Start: file: c:/temp/#{export_id}.tsv" }
    # LOGGER.debug('Database::tsv2db') { "Encoding aliases: #{Encoding.aliases}" }

    unless File.file?("c:/temp/#{export_id}.tsv")
      LOGGER.error(progname) { 'File for this exportId not found' }
      raise StandardError, 'TSV file not found'
    end
    LOGGER.info(progname) { "File found: c:/temp/#{export_id}.tsv" }

    begin
      infile = File.open("c:/temp/#{export_id}.tsv", 'rb:BOM|UTF-8')
    rescue Errno::ENOENT
      LOGGER.error(progname) { 'File open error' }
      LOGGER.error(progname) { "Error #{e.inspect}" }
      raise
    end

    unless access_ok?
      LOGGER.error(progname) { 'Database not available' }
      raise StandardError, 'Database access not ok'
    end

    # It seems we do not need to specify encoding in this phase if file is opened with correct parameters above.
    # unless (rawdata = File.read(infile, encoding: 'BOM|UTF-8'))
    unless (rawdata = infile.read)
      LOGGER.error(progname) { 'Reading file failed' }
      raise StandardError, 'Reading file failed'
    end

    LOGGER.info(progname) { "Raw data encoding: #{rawdata.encoding}" }
    begin
      csvdata = CSV.parse(rawdata, { headers: true, col_sep: "\t", liberal_parsing: true })
    rescue StandardError => e
      LOGGER.error(progname) { 'CSV parse failed' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    end

    LOGGER.info(progname) { "Number of rows: #{csvdata.length}" }
    if csvdata.length.zero?
      LOGGER.info(progname) { 'Empty file, nothing to do, exiting' }
      return true
    end

    # Log and check the first line. Assume rest of the file is good. If we do not get at least 6 fields,
    # assume it was not parsed ok.
    LOGGER.debug(progname) { "Row 0 fields: #{csvdata[0].length}" }
    csvdata[0].each do |field|
      LOGGER.debug(progname) { "Header0: #{field[0]} -- Value0: #{field[1]}" }
    end
    if csvdata[0].length < 6
      LOGGER.error(progname) { 'CSV not parsed properly' }
      raise StandardError, 'CSV not parsed properly'
    end

    # Does this guarantee the order of headers and values is always the same?
    values = []
    count = 0
    # Marketo API returns string "null" for empty fields. Replace it with real nil.
    csvdata.each do |row|
      values << row.fields.map { |i| i == 'null' ? nil : i }
    end

    # How does this work? Apparently we take batches of 50 somehow.
    while (batch = values.shift(50))
      break unless batch.length.positive?

      count += batch.length
      LOGGER.debug(progname) { "#{export_id}: new batch. Total: #{count}" }
      begin
        @sequeldb.from(@config['sql_temptable']).import(csvdata.headers, batch)
      rescue Sequel::Error => e
        LOGGER.error(progname) { 'Database import failed' }
        LOGGER.error(progname) { "Error: #{e.inspect}" }
        raise
      end

      # Should we run merge only after the upload loop completed fully?
      unless merge_temp_table
        LOGGER.error(progname) { 'Database merge function failed' }
        raise StandardError, 'merge_temp_table call failed'
      end
    end
    true
  end

  # Merge temp tample into real table.
  def merge_temp_table
    progname = "#{Thread.current.object_id}|Database::merge_temp_table"
    LOGGER.info(progname) { 'Start' }

    # Update bulk job status db and return the parsed response.
    begin
      LOGGER.debug(progname) { "Before: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
      @tdspool.with do |connection|
        LOGGER.debug(progname) { "During: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
        results = connection.execute('EXEC [dbo].[merge_temp]')
        results.do
        unless connection.return_code.zero?
          LOGGER.error(progname) { 'Database merge stored procedure failed' }
          raise StandardError, 'Database merge stored procedure failed'
        end
      end
      LOGGER.debug(progname) { "After: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
    rescue TinyTds::Error => e
      LOGGER.error(progname) { 'TinyTds error' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    rescue StandardError => e
      LOGGER.error(progname) { 'TinyTds standard error' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    end
    LOGGER.info(progname) { 'Merged succesfully' }
    true
  end

  # Choose a completed full sync job, and upload the data to database.
  def upload_fullsync_data
    progname = "#{Thread.current.object_id}|Database::upload_fullsync_data"
    LOGGER.info(progname) { 'Start' }

    begin
      job = jobs('nextready', false)[0]
      unless job
        LOGGER.info(progname) { 'No jobs available for data transfer' }
        return true
      end
      LOGGER.info(progname) { "Next job ready to be uploaded: #{job['exportId']}" }
      LOGGER.debug(progname) { "Next job ready to be uploaded: #{job}" } if $debugmode
      selected_job = job['exportId']
      LOGGER.info(progname) { "Selected job id ready to be uploaded: #{selected_job}" }
    rescue StandardError => e
      LOGGER.error(progname) { 'jobs error' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    end

    begin
      LOGGER.info(progname) { 'Downloading TSV file' }
      MKTO.download_tsv(selected_job)
    rescue StandardError => e
      LOGGER.error(progname) { 'download_tsv error' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    end

    # Update importStarted timestamp into database.
    begin
      LOGGER.debug(progname) { "Before: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
      @tdspool.with do |connection|
        LOGGER.debug(progname) { "During: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
        results = connection.execute("UPDATE MarketoLeads.dbo.JobStatus
                    SET importStarted = '#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}'
                    OUTPUT INSERTED.exportId, INSERTED.status
                    WHERE exportId = '#{selected_job}'")
        results.do
      end
      LOGGER.debug(progname) { "After: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
    rescue TinyTds::Error => e
      LOGGER.error(progname) { 'TinyTds error 1' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    rescue StandardError => e
      LOGGER.error(progname) { 'TinyTds standard error 1' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    end

    begin
      tsv2db(selected_job)
    rescue StandardError => e
      LOGGER.error(progname) { 'tsv2db error' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    end

    # Update importedAt timestamp into database.
    begin
      LOGGER.debug(progname) { "Before: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
      @tdspool.with do |connection|
        LOGGER.debug(progname) { "During: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
        results = connection.execute("UPDATE MarketoLeads.dbo.JobStatus
                    SET importedAt = '#{Time.now.strftime('%Y-%m-%d %H:%M:%S')}'
                    OUTPUT INSERTED.exportId, INSERTED.status
                    WHERE exportId = '#{selected_job}'")
        results.do
      end
      LOGGER.debug(progname) { "After: Pool size: #{@tdspool.size} Pool available: #{@tdspool.available}" }
    rescue TinyTds::Error => e
      LOGGER.error(progname) { 'TinyTds error 2' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    rescue StandardError => e
      LOGGER.error(progname) { 'TinyTds standard error 2' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    end

    true
  end
end
