# frozen_string_literal: true

require 'json'
require 'httparty'
require 'rufus-scheduler'
require 'logger'
require_relative 'config_loader'
require_relative 'mkto'
require_relative 'sql'

progname = "#{Thread.current.object_id}|Main"

logfile = File.open('mktosync.log', 'ab:UTF-8')
logfile.sync = true
LOGGER = Logger.new(logfile)
LOGGER.level = Logger::DEBUG
$debugmode = false
$responsedebug = false

LOGGER.info(progname) { "run dir: #{__dir__}" }
LOGGER.info(progname) { "run file: #{__FILE__}" }
LOGGER.info(progname) { "debugmode: #{$debugmode}" }

CONFIG = ConfigLoader.new.config_all
LOGGER.debug(progname) { JSON.pretty_generate(CONFIG).to_s } if $responsedebug

Mkto.config = CONFIG
begin
  MKTO = Mkto.new(LOGGER)
rescue StandardError => e
  puts "Error initializing Marketo object! Exiting. #{e.inspect}"
  raise
end
LOGGER.info(progname) { "mkto_token: #{MKTO.mkto_token}" }

Database.classconfig = CONFIG
begin
  DATABASE = Database.new(LOGGER)
rescue StandardError => e
  puts "Error initializing Database object! Exiting. #{e.inspect}"
  raise
end
LOGGER.info(progname) { "dbconnection.active?: #{DATABASE.dbconnection.active?}" }

scheduler = Rufus::Scheduler.new

str = '
h: List options
---- VALIDATIONS & TESTS ----
0: Get new Marketo token
1: Get lead
2: Get lead details
3: Test SQL Access
4: DB Access OK?
---- BULK JOB TASKS ----
10: List bulk jobs
11: Create bulk job
12: Enqueue bulk job
13: Cancel bulk job
14: Get bulk job status
15: Download TSV
---- DB TASKS ----
20: Load job export data to DB
21: Refresh status DB
22: Delete temp table
23: Merge temp table
---- SCHEDULERS ----
30: Schedule DB refresh
31: Schedule job stats
---- FULL SYNC TASKS ----
40: List full syncs
41: Start new full sync
42: Continue existing full syncs
43: Stop a full sync
44: Schedule full sync continue
45: Schedule full sync data upload
---- OTHER ----
99: Exit'

# Checking for errors returned should be added.
loop do
  print 'Choose action (h for help):'
  choice = gets.chomp
  case choice
  when 'h'
    puts str

  when '0'
    begin
      MKTO.refresh_token(CONFIG, true)
    rescue StandardError => e
      puts "Error! #{e.inspect}"
    else
      puts 'Success!'
    end

  when '1'
    print 'Enter email address:'
    MKTO.lead_by_email(gets.chomp) ? (puts 'Success!') : (puts 'Failed!')
  when '2'
    print 'Enter email address:'
    MKTO.lead_by_email_details(gets.chomp) ? (puts 'Success!') : (puts 'Failed!')
  when '3'
    DATABASE.test_access
  when '4'
    DATABASE.access_ok? ? (puts 'Success!') : (puts 'Failed!')
  when '10'
    print 'Enter type (db, api):'
    case gets.chomp
    when 'db'
      begin
        DATABASE.jobs('all', true)
      rescue StandardError => e
        puts "Error: #{e.inspect}"
      end
    when 'api'
      MKTO.bulk_jobs(true)
    else
      puts 'Invalid value (api or db).'
    end
  when '11'
    puts 'Enter start date (YYYY-MM-DD): '
    startd = gets.chomp
    puts 'Enter end date (YYYY-MM-DD): '
    endd = gets.chomp
    MKTO.create_bulk_job(startd, endd, 'manual', DATABASE) ? (puts 'Success!') : (puts 'Failed!')
  when '12'
    print 'Enter job id:'
    MKTO.enqueue_bulk_job(gets.chomp) ? (puts 'Success!') : (puts 'Failed!')
  when '13'
    print 'Enter job id:'
    MKTO.cancel_bulk_job(gets.chomp) ? (puts 'Success!') : (puts 'Failed!')
  when '14'
    print 'Enter job id:'
    (ret = MKTO.bulk_job_status(gets.chomp)) ? (puts JSON.pretty_generate(ret).to_s) : (puts 'Failed!')

  when '15'
    print 'Enter job id:'
    begin
      MKTO.download_tsv(gets.chomp)
    rescue StandardError => e
      puts "Error: #{e.inspect}"
    else
      puts 'Success!'
    end

  when '20'
    print 'Enter job id:'
    DATABASE.tsv2db(gets.chomp) ? (puts 'Success!') : (puts 'Failed!')
  when '21'
    DATABASE.refresh_status_table(MKTO) ? (puts 'Success!') : (puts 'Failed!')
  when '22'
    DATABASE.delete_temp_table ? (puts 'Success!') : (puts 'Failed!')
  when '23'
    DATABASE.merge_temp_table ? (puts 'Success!') : (puts 'Failed!')

  when '30'
    LOGGER.info(progname) { 'Creating scheduler DBREFRESH' }
    dbrefreshjob = scheduler.schedule_interval '10m', name: 'DBREFRESH', overlap: false, first_at: Time.now + 30 do
      LOGGER.info(progname) { 'Schedule DBREFRESH: start run' }
      MKTO.refresh_token(CONFIG, false)
      DATABASE.refresh_status_table(MKTO)
    rescue StandardError => e
      LOGGER.error(progname) { 'Schedule DBREFRESH: run failed' }
      LOGGER.error(progname) { "Schedule DBREFRESH: Error #{e.inspect}" }
      puts Time.now
      puts "Error in DBREFRESH: #{e.inspect}"
      puts "Backtrace in DBREFRESH: #{e.backtrace}"
    else
      LOGGER.info(progname) { 'Schedule DBREFRESH: run ok' }
    end
    unless dbrefreshjob.instance_of? Rufus::Scheduler::IntervalJob
      LOGGER.error(progname) { 'Failed creating scheduler DBREFRESH' }
      raise StandardError, 'Failed creating scheduler DBREFRESH'
    end
    LOGGER.info(progname) { 'Created scheduler DBREFRESH' }

  when '31'
    LOGGER.info(progname) { 'Creating scheduler JOBSTATS' }
    scheduler.schedule_interval '5m', name: 'JOBSTATS', overlap: false, first_at: Time.now + 120 do
      LOGGER.info(progname) do
        <<~LOGLINE.gsub(/\n/, '')
          JOBSTATS: uptime: #{scheduler.uptime_s}, freq: #{scheduler.frequency},
          paused?: #{scheduler.paused?}, up?: #{scheduler.up?}
        LOGLINE
      end

      # LOGGER.debug('main') { scheduler.jobs } if $debugmode
      scheduler.jobs.each do |job|
        LOGGER.info(
          <<~LOGLINE.gsub(/\n/, ' ')
            main JOBSTATS: jobname: #{job.name}, count #{job.count},
            interval: #{job.interval}, original: #{job.original}
          LOGLINE
        )
      end
      LOGGER.info(progname) { "TDSPool available connections: #{DATABASE.tdspool.available}" }
    end
    LOGGER.info(progname) { 'Created scheduler JOBSTATS' }

  when '40'
    LOGGER.info('main') { 'List full sync cycles summary' }
    if (data = DATABASE.jobs('fullsync', false)) && (fullsyncs = DATABASE.fullsyncsummary(data))
      fullsyncs.each do |fs|
        # fs is a hash of different properties of a fullsync. In a %w array, list the keys we are intereseted in,
        # and use .include to select only those hash items.
        p(fs.select { |key| %w[exportId monthDelta fullSyncId fullSyncComplete].include?(key) })
      end
    else
      puts 'Failed!'
    end

  when '41'
    LOGGER.info(progname) { 'Create new full sync cycle' }
    begin
      MKTO.fullsync(DATABASE, 'new')
    rescue StandardError => e
      LOGGER.error(progname) { 'Creating new full sync failed!' }
      LOGGER.error(progname) { "Error #{e.inspect}" }
      puts Time.now
      puts "Error in UPLOADDATA: #{e.inspect}"
      puts "Backtrace in UPLOADDATA: #{e.backtrace}"
    else
      puts 'Success!'
    end

  when '42'
    LOGGER.info(progname) { 'Add next jobs for all ongoing full sync cycles' }
    begin
      fullsyncs = MKTO.fullsync(DATABASE, 'cont')
    rescue StandardError => e
      LOGGER.error(progname) { 'Creating new full sync failed!' }
      LOGGER.error(progname) { "Error #{e.inspect}" }
      puts Time.now
      puts "Error in UPLOADDATA: #{e.inspect}"
      puts "Backtrace in UPLOADDATA: #{e.backtrace}"
    else
      if fullsyncs
        fullsyncs.each do |fs|
          p(fs.select { |k| %w[fullSyncId].include?(k) })
        end
        LOGGER.info(progname) { 'Added new full sync jobs' }
      else
        LOGGER.info(progname) { 'No new jobs added' }
        puts 'No new jobs added'
      end
    end

  when '43'
    LOGGER.info('main') { 'Stop an ongoing full sync cycle' }
    print 'Enter job id:'
    MKTO.stop_fullsync(gets.chomp, DATABASE) ? (puts 'Success!') : (puts 'Failed!')

  when '44'
    LOGGER.info(progname) { 'Creating scheduler ADDFULLSYNCJOB' }
    fullsjob = scheduler.schedule_interval '2h', name: 'ADDFULLSYNCJOB', overlap: false, first_at: Time.now + 10 do
      LOGGER.info(progname) { 'Schedule ADDFULLSYNCJOB: Start run' }
      MKTO.refresh_token(CONFIG, false)
      fullsyncs = MKTO.fullsync(DATABASE, 'cont')
      if fullsyncs
        fullsyncs.each do |fs|
          LOGGER.info(progname) do
            "Schedule ADDFULLSYNCJOB: Processed: #{fs.select { |key| %w[fullSyncId].include?(key) }}"
          end
        end
      else
        LOGGER.info(progname) { 'Schedule ADDFULLSYNCJOB: No jobs processed' }
      end
    rescue StandardError => e
      LOGGER.error(progname) { 'Schedule ADDFULLSYNCJOB: Run failed' }
      LOGGER.error(progname) { "Schedule ADDFULLSYNCJOB: Error #{e.inspect}" }
      puts Time.now
      puts "Error in ADDFULLSYNCJOB: #{e.inspect}"
      puts "Backtrace in ADDFULLSYNCJOB: #{e.backtrace}"
    else
      LOGGER.info(progname) { 'Schedule ADDFULLSYNCJOB: Run ok' }
    end
    unless fullsjob.instance_of? Rufus::Scheduler::IntervalJob
      LOGGER.error(progname) { 'Failed creating scheduler ADDFULLSYNCJOB' }
      raise StandardError, 'Failed creating scheduler ADDFULLSYNCJOB'
    end
    LOGGER.info(progname) { 'Created scheduler ADDFULLSYNCJOB' }

  when '45'
    LOGGER.info(progname) { 'Creating scheduler UPLOADDATA' }
    uploaddatajob = scheduler.schedule_interval '30m', name: 'UPLOADDATA', overlap: false, first_at: Time.now + 5 do
      LOGGER.info(progname) { 'Schedule UPLOADDATA: Start run' }
      DATABASE.upload_fullsync_data
    rescue StandardError => e
      LOGGER.error(progname) { 'Schedule UPLOADDATA: Run failed' }
      LOGGER.error(progname) { "Schedule UPLOADDATA: Error #{e.inspect}" }
      puts Time.now
      puts "Error in UPLOADDATA: #{e.inspect}"
      puts "Backtrace in UPLOADDATA: #{e.backtrace}"
    else
      LOGGER.info(progname) { 'Schedule UPLOADDATA: Run ok' }
    end
    unless uploaddatajob.instance_of? Rufus::Scheduler::IntervalJob
      LOGGER.error(progname) { 'Failed creating scheduler UPLOADDATA' }
      raise StandardError, 'Failed creating scheduler UPLOADDATA'
    end
    LOGGER.info(progname) { 'Created scheduler UPLOADDATA' }

  when '99'
    LOGGER.info('Exiting')
    scheduler.shutdown
    exit 0
  else
    puts 'No such choice'
  end
end
