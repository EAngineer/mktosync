# frozen_string_literal: true

require 'json'
require 'csv'
require 'httparty'
require 'logger'

# Class to access Mkto.
class Mkto
  # Class-Instance variable for Mkto config.
  # Class-Instance variables are good because the behave like class var, but are not shared with sub-classes.
  @config = nil
  class << self
    attr_accessor :config
  end

  # Instance variables.
  @mkto_token = nil
  @logger = nil
  attr_accessor :mkto_token, :logger

  def initialize(logger)
    progname = "#{Thread.current.object_id}|Mkto::initialize"

    LOGGER.info(progname) { 'Start' }
    @logger = logger
    begin
      @mkto_token = refresh_token(self.class.config, false)
    rescue StandardError => e
      LOGGER.error(progname) { 'Mkto token refresh failed' }
      LOGGER.error(progname) { "Error #{e.inspect}" }
      raise
    end
    LOGGER.info(progname) { 'End' }
  end

  # Get access token.
  def refresh_token(config, printresponse)
    progname = "#{Thread.current.object_id}|Mkto::refresh_token"
    LOGGER.info(progname) { 'Start' }
    if $debugmode
      LOGGER.debug(progname) do
        # gsub used to remove the linefeed at the end of string
        <<~HEREDOC.gsub(/\n/, '')
          GET: #{config['marketo_url']}/identity/oauth/token?grant_type=client_credentials&client_id=\
          #{config['client_id']}&client_secret=#{config['client_secret']}
        HEREDOC
      end
    end

    # Make request.
    begin
      response = HTTParty.get("#{config['marketo_url']}/identity/oauth/token?grant_type=client_credentials&\
client_id=#{config['client_id']}&client_secret=#{config['client_secret']}")
    rescue StandardError => e
      LOGGER.error(progname) { 'Mkto API request failed' }
      LOGGER.error(progname) { "Error #{e.inspect}" }
      raise
    end

    # Check API response
    unless check_api_response(response)
      LOGGER.error(progname) { 'Mkto API resonse validation failed' }
      raise StandardError, 'Mkto API response validation failed'
    end

    # Parse textual response into hash.
    begin
      parsed_response = JSON.parse(response.body)
    rescue StandardError => e
      LOGGER.error(progname) { 'Mkto API resonse JSON parse failed' }
      LOGGER.error(progname) { "Error #{e.inspect}" }
      raise
    end

    puts "RESPONSE: #{JSON.pretty_generate(parsed_response)}" if printresponse
    LOGGER.debug(progname) { "Response: #{JSON.pretty_generate(parsed_response)}" } if $responsedebug
    LOGGER.info(progname) { 'End' }

    # Update the token & return it.
    @mkto_token = parsed_response['access_token']
    parsed_response['access_token']
  end

  # Get a lead by email address.
  def lead_by_email(email)
    LOGGER.info('Mkto::lead_by_email') { "Start: email: #{email}" }
    LOGGER.debug('Mkto::lead_by_email') do
      "GET: #{self.class.config['marketo_url']}/rest/v1/leads.json?access_token=#{@mkto_token}&\
filterType=email&filterValues=#{email}"
    end

    response = HTTParty.get("#{self.class.config['marketo_url']}/rest/v1/leads.json?\
access_token=#{@mkto_token}&filterType=email&filterValues=#{email}")
    unless check_api_response(response)
      LOGGER.error('Mkto::lead_by_email') { 'Mkto API request failed' }
      return false
    end

    parsed_response = JSON.parse(response.body)
    puts "RESPONSE: #{JSON.pretty_generate(parsed_response)}"
    LOGGER.debug('Mkto::lead_by_email') { "RESPONSE: #{JSON.pretty_generate(parsed_response)}" } if $responsedebug
    parsed_response
  end

  # Other version of getting a lead record by email address.
  def lead_by_email_details(email)
    LOGGER.info('Mkto::lead_by_email_details') { "Start: email: #{email}" }

    fields = "RecordTypeId,id,email,SFDCType,sfdcLeadId,sfdcContactId,company,\
contactCompany,inferredCompany,externalCompanyId,sfdcAccountId,Account__c,\
LFBN__Duplicate_Account__c,AccountName__c,Account_Name__c,bizible2__Account__c"

    LOGGER.debug('Mkto::lead_by_email_details') do
      "GET: #{self.class.config['marketo_url']}/rest/v1/leads.json?access_token=#{@mkto_token}&\
filterType=email&filterValues=#{email}&fields=#{fields}"
    end

    response = HTTParty.get("#{self.class.config['marketo_url']}/rest/v1/leads.json?\
access_token=#{@mkto_token}&filterType=email&filterValues=#{email}&fields=#{fields}")
    unless check_api_response(response)
      LOGGER.error('Mkto::lead_by_email_details') { 'Mkto API request failed' }
      return false
    end

    parsed_response = JSON.parse(response.body)
    if $responsedebug
      LOGGER.debug('Mkto::lead_by_email_details') { "RESPONSE: #{JSON.pretty_generate(parsed_response)}" }
    end
    puts "RESPONSE: #{JSON.pretty_generate(parsed_response)}"
    parsed_response
  end

  # Get bulk jobs via Marketo API.
  def bulk_jobs(printresponse)
    LOGGER.info('Mkto::bulk_jobs') { 'Start' }
    LOGGER.debug('Mkto::bulk_jobs') do
      "GET: #{self.class.config['marketo_url']}/bulk/v1/leads/export.json?access_token=#{@mkto_token}"
    end
    # Get bulk jobs via Marketo API
    response = HTTParty.get("#{self.class.config['marketo_url']}/bulk/v1/leads/export.json?\
access_token=#{@mkto_token}")
    parsed_response = JSON.parse(response.body)
    unless check_api_response(response)
      LOGGER.error('Mkto::bulk_jobs') { 'Mkto API request failed' }
      return false
    end

    # Print if needed
    if printresponse == true
      print 'export ID'.to_s.ljust(40, ' ')
      print 'status'.to_s.ljust(13, ' ')
      print 'createdAt'.to_s.ljust(25, ' ')
      print 'queuedAt'.to_s.ljust(25, ' ')
      print 'numberOfRecords'.to_s.ljust(20, ' ')
      puts 'fileSize'.to_s.ljust(10, ' ')
      parsed_response['result'].each do |job|
        print job['exportId'].to_s.ljust(40, ' ')
        print job['status'].to_s.ljust(13, ' ')
        print job['createdAt'].to_s.ljust(25, ' ')
        print job['queuedAt'].to_s.ljust(25, ' ')
        print job['numberOfRecords'].to_s.ljust(20, ' ')
        puts job['fileSize'].to_s.ljust(10, ' ')
        # p [:job, job]
      end
      LOGGER.debug('Mkto::bulk_jobs') { "RESPONSE: #{JSON.pretty_generate(parsed_response)}" } if $responsedebug
    end
    # Return parsed response.
    parsed_response
  end

  # Create bulk job with given start and end dates.
  # Return the parsed API response. Update status DB, unless nil database given.
  def create_bulk_job(startd, endd, jobtype, database)
    progname = "#{Thread.current.object_id}|Marketo::create_bulk_job"
    LOGGER.info(progname) { 'Start' }
    LOGGER.info(progname) { "startd '#{startd}', endd '#{endd}', jobtype '#{jobtype}'" }

    # If database=nil, we are not expected to updated database. So, nil is ok. But if database connection is
    # provided, access to is must be ok.
    unless database.nil? || database.access_ok?
      LOGGER.error(progname) { 'SQL connection not available' }
      raise StandardError, 'SQL connection not available'
    end

    if $debugmode
      LOGGER.debug(progname) do
        "POST: #{self.class.config['marketo_url']}/bulk/v1/leads/export/create.json?access_token=#{@mkto_token}"
      end
    end

    # Prepare request.
    req_body = { fields: self.class.config['fields'],
                 format: 'TSV',
                 columnHeaderNames: self.class.config['columnHeaderNames'],
                 filter: {
                   createdAt: {
                     startAt: "#{startd}T00:00:00Z",
                     endAt: "#{endd}T00:00:00Z"
                   }
                 } }
    req_headers = { 'Content-Type': 'application/json' }
    # Body needs to be transformed to json txt, because HTTParty is not supposed to parse it, but instead,
    # just forward it. Headers are parsed.
    req_params = { body: req_body.to_json, headers: req_headers }
    LOGGER.debug(progname) { "req BODY: #{JSON.pretty_generate(req_body)}" } if $responsedebug
    LOGGER.debug(progname) { "req HEADERS: #{JSON.pretty_generate(req_headers)}" } if $responsedebug

    # Call the API.
    LOGGER.info(progname) { 'Making Marketo API call' }
    response = HTTParty.post("#{self.class.config['marketo_url']}/bulk/v1/leads/export/\
create.json?access_token=#{@mkto_token}", req_params)
    unless check_api_response(response)
      LOGGER.error(progname) { 'Mkto API request failed' }
      raise StandardError, 'Mkto API request failed'
    end

    # Parse response.
    parsed_response = JSON.parse(response.body)
    LOGGER.debug(progname) { "RESPONSE: #{JSON.pretty_generate(parsed_response)}" } if $responsedebug
    # Assume we get only one resultset ('0').
    LOGGER.info(progname) { "exportId: #{parsed_response['result'][0]['exportId']}" }
    LOGGER.info(progname) { "createdAt: #{parsed_response['result'][0]['createdAt']}" }

    # If status database was given, update it. Otherwise, skip database update.
    unless database.nil?
      LOGGER.info(progname) { 'Status database was given, upserting job data' }
      if jobtype.nil?
        LOGGER.error(progname) { 'Database given, but jobtype not provided, cannot update databse'}
        raise StandardError, 'Dabase giveb, jobtype missing'
      end
      # Prepare dataset to be updated to DB.
      jobvalues = { exportId: parsed_response['result'][0]['exportId'],
                    startDate: startd,
                    endDate: endd,
                    createdAt: parsed_response['result'][0]['createdAt'],
                    status: parsed_response['result'][0]['status'],
                    jobSource: 'ruby',
                    jobType: jobtype }

      # Update status DB.
      begin
        database.upsert_job(jobvalues, 'insert')
      rescue StandardError => e
        LOGGER.error(progname) { "upsert_job failed: #{e.inspect}" }
        raise
      end
    end
    parsed_response
  end

  # Start or continue full sync cycle. Return true if success.
  def fullsync(database, type)
    progname = "#{Thread.current.object_id}|Marketo::fullsync"
    LOGGER.info(progname) { 'Start' }

    # First get raw data from satus db, then get the latest full sync jobs using the helper function.
    begin
      data = database.jobs('fullsync', false)
      fullsyncs = database.fullsyncsummary(data)
    rescue StandardError => e
      LOGGER.error(progname) { "Failed to get existing job data, error: #{e.inspect}" }
      raise
    end

    LOGGER.info(progname) { "all: #{fullsyncs.length}" }
    LOGGER.info(progname) { "complete_f: #{fullsyncs.filter { |r| r['fullSyncComplete'] == false }.length}" }
    LOGGER.info(progname) { "complete_notf: #{fullsyncs.filter { |r| r['fullSyncComplete'] != false }.length}" }
    LOGGER.info(progname) { "complete_t: #{fullsyncs.filter { |r| r['fullSyncComplete'] == true }.length}" }
    LOGGER.info(progname) { "complete_nott: #{fullsyncs.filter { |r| r['fullSyncComplete'] != true }.length}" }

    case type
    when 'cont'
      # Get number of incomplete full syncs. Unless we have any, abort (nothing to continue).
      unless fullsyncs.reject { |x| x['fullSyncComplete'] == true }.length.positive?
        LOGGER.info(progname) { 'No full syncs running, cannot continue' }
        return false
      end
      LOGGER.info(progname) { 'Continue running full syncs' }
      # Go through all the incomplete full syncs. Increase month delta and call add_fullsync_job to add the next job.
      # Last, enqueue it.
      fullsyncs.reject { |x| x['fullSyncComplete'] == true }.each do |fs|
        LOGGER.info(progname) { "Add next job to: #{fs['fullSyncId']} #{fs['monthDelta']}" }
        delta = fs['monthDelta'] + 1
        jobid = add_fullsync_job(delta, fs['fullSyncId'], database)
        unless jobid
          LOGGER.error(progname) { 'Creating next job of full sync failed' }
          raise StandardError, 'Creating next job of full sync failed'
        end
        unless enqueue_bulk_job(jobid)
          LOGGER.error(progname) { 'Job enqueue failed' }
          raise StandardError, 'Job enqueue failed'
        end
      end
      # Return an array of updated full syncs.
      fullsyncs.reject { |x| x['fullSyncComplete'] == true }

    when 'new'
      # Get number of incomplete full syncs. If there are any, abort (we don't want multiple running).
      if fullsyncs.reject { |x| x['fullSyncComplete'] == true }.length.positive?
        LOGGER.error(progname) { 'Full syncs already running, cannot continue' }
        return false
      end
      LOGGER.info(progname) { 'Create new full sync' }
      jobid = add_fullsync_job(0, nil, database)
      unless jobid
        LOGGER.error(progname) { 'Creating first job of full sync failed' }
        raise StandardError, 'Creating first job of full sync failed'
      end
      unless enqueue_bulk_job(jobid)
        LOGGER.error(progname) { 'Job enqueue failed' }
        return false
      end
      # Return the id of the new full sync.
      jobid
    else
      false
    end
  end

  # Helper function to resolve parameters for the new Mkto bulk job to be created. Creates the job, and updated the DB.
  # Return the id of the job created.
  def add_fullsync_job(delta, fullsyncid, database)
    progname = "#{Thread.current.object_id}|Marketo::add_fullsync_job"
    LOGGER.info(progname) { "Start: delta: '#{delta}' fullsyncid: '#{fullsyncid}'" }

    mkto_epoch_date = Date.parse self.class.config['marketo_epoch']
    job_start_date = mkto_epoch_date >> delta
    job_end_date = job_start_date >> 1

    # Some troubleshoot logging
    LOGGER.debug(progname) { "epoch: #{mkto_epoch_date}" }
    LOGGER.debug(progname) { "epoch: #{mkto_epoch_date.strftime('%b %d %Y')}" }
    LOGGER.debug(progname)  { "delta #{delta}" }
    LOGGER.debug(progname)  { "epochday #{mkto_epoch_date.mday}" }
    LOGGER.debug(progname)  { "epochmonth #{mkto_epoch_date.mon}" }
    LOGGER.debug(progname)  { "epochyear #{mkto_epoch_date.year}" }
    LOGGER.debug(progname)  { "startdate #{job_start_date.strftime('%b %d %Y')}" }
    LOGGER.debug(progname)  { "startdate #{job_end_date.strftime('%b %d %Y')}" }
    LOGGER.info(progname) { "Adding job to fullsync: '#{fullsyncid}'" }

    # Crete job into Mkto. Return false if job creation fails.
    begin
      parsed_response = create_bulk_job(job_start_date.strftime('%Y-%m-%d'), job_end_date.strftime('%Y-%m-%d'),
                                        nil, nil)
    rescue StandardError => e
      LOGGER.error(progname) { "create_bulk_job failed: #{e.inspect}" }
      raise
    else
      unless parsed_response
        LOGGER.error(progname) { 'create_bulk_job returned non-true' }
        raise StandardError, 'create_bulk_job returned non-true'
      end
    end

    LOGGER.debug(progname) { "apiresponse: #{JSON.pretty_generate(parsed_response)}" } if $responsedebug

    # Set the full sync id to be updated into DB:
    # - If we got it with function call (fullsync already running) use the given value.
    # - If not got with function call, this is a new fullsync -> use the one given in Mkto API response.
    fsid = if fullsyncid.nil?
             parsed_response['result'][0]['exportId']
           else
             fullsyncid
           end

    # Prepare dataset to be updated to DB.
    jobvalues = { exportId: parsed_response['result'][0]['exportId'],
                  startDate: job_start_date,
                  endDate: job_end_date,
                  createdAt: parsed_response['result'][0]['createdAt'],
                  status: parsed_response['result'][0]['status'],
                  jobSource: 'ruby',
                  jobType: 'fullsync',
                  monthDelta: delta,
                  fullSyncId: fsid }

    # Update DB.
    begin
      database.upsert_job(jobvalues, 'insert')
    rescue StandardError => e
      LOGGER.error(progname) { "Bulk job could not be inserted into database, error: #{e.inspect}" }
      raise
    end
    # Return the ID of the job created.
    parsed_response['result'][0]['exportId']
  end

  # Cancel all jobs of a full sync cycle. Runs cancel operation to Marketo and updates the database.
  def stop_fullsync(fullsync_id, database)
    LOGGER.info('Mkto::stop_fullsync') { 'Start' }
    LOGGER.info('Mkto::stop_fullsync') { "Stopping fullsync with id: #{fullsync_id}" }
    return false if fullsync_id.to_s.strip.empty?

    jobdata = DATABASE.jobs('fullsync', false)
    return false unless jobdata
    return false unless jobdata.instance_of?(Array)

    LOGGER.info('Mkto::stop_fullsync') { 'Got job data from DB' }

    # Get job id's of the specified full sync.
    jobs = jobdata.filter_map { |r| r['exportId'] if r['fullSyncId'] == fullsync_id }
    LOGGER.debug('Mkto::stop_fullsync') { "Jobs to be stopped: #{jobs}" }
    LOGGER.info('Mkto::stop_fullsync') { "#{jobs.length} jobs to be stopped" }

    # Cancel each job. Continue upon failure, because the job may have been removed by Marketo itself already.
    jobs.each do |job|
      LOGGER.debug('Mkto::stop_fullsync') { "Stopping job #{job}" }
      # Example of flow control using lazines.
      cancel_bulk_job(job) || LOGGER.warn('Mkto::stop_fullsync') { 'Task cancellation failed' }

      jobvalues = { exportId: job,
                    fullSyncComplete: 1 }

      unless database.upsert_job(jobvalues, 'update')
        LOGGER.error('Mkto::stop_fullsync') { 'Status database update failed' }
        return false
      end
    end

    true
  end

  # Cancel a bulk job with given id.
  def cancel_bulk_job(job_id)
    LOGGER.info('Mkto::cancel_bulk_job') { "Start: job_id=#{job_id}" }
    LOGGER.debug('Mkto::cancel_bulk_job') do
      "POST: #{self.class.config['marketo_url']}/bulk/v1/leads/export/#{job_id}/cancel.json?access_token=#{@mkto_token}"
    end

    response = HTTParty.post("#{self.class.config['marketo_url']}/bulk/v1/leads/export/#{job_id}/cancel.json\
?access_token=#{@mkto_token}")
    unless check_api_response(response)
      LOGGER.warn('Mkto::cancel_bulk_job') { 'Mkto API request failed' }
      return false
    end

    parsed_response = JSON.parse(response.body)
    LOGGER.debug('Mkto::cancel_bulk_job') { "RESPONSE: #{JSON.pretty_generate(parsed_response)}" } if $responsedebug
    parsed_response
  end

  # Run bulk job.
  def enqueue_bulk_job(job_id)
    progname = "#{Thread.current.object_id}|Marketo::enqueue_bulk_job"
    LOGGER.info(progname) { 'Start' }
    if $debugmode
      LOGGER.debug(progname) do
        "POST: #{self.class.config['marketo_url']}/bulk/v1/leads/export/#{job_id}\
/enqueue.json?access_token=#{@mkto_token}"
      end
    end

    response = HTTParty.post("#{self.class.config['marketo_url']}/bulk/v1/leads/export/#{job_id}/enqueue.json\
?access_token=#{@mkto_token}")
    unless check_api_response(response)
      LOGGER.error(progname) { 'Mkto API request failed' }
      return false
    end

    LOGGER.info(progname) { "Enqueued job: #{job_id}" }
    parsed_response = JSON.parse(response.body)
    LOGGER.debug(progname) { "RESPONSE: #{JSON.pretty_generate(parsed_response)}" } if $responsedebug
    parsed_response
  end

  # Get specific bulk job's status.
  def bulk_job_status(job_id)
    progname = "#{Thread.current.object_id}|Mkto::bulk_job_status"
    LOGGER.info(progname) { 'Start' }

    if $debugmode
      LOGGER.debug(progname) do
        <<~HEREDOC.gsub(/\n/, '')
          GET: #{self.class.config['marketo_url']}/bulk/v1/leads//export/#{job_id}/status.json\
          ?access_token=#{@mkto_token}"
        HEREDOC
      end
    end

    begin
      response = HTTParty.get("#{self.class.config['marketo_url']}/bulk/v1/leads//export/#{job_id}/status.json?\
access_token=#{@mkto_token}")
    rescue StandardError => e
      LOGGER.error(progname) { "HTTParty error: #{e.inspect}" }
      return false
    end

    unless check_api_response(response)
      LOGGER.error(progname) { 'Mkto API request failed' }
      return false
    end

    parsed_response = JSON.parse(response.body)
    LOGGER.debug(progname) { "RESPONSE: #{JSON.pretty_generate(parsed_response)}" } if $responsedebug
    LOGGER.info(progname) { 'End' }
    parsed_response
  end

  # Download bulk job results.
  def download_tsv(job_id)
    progname = "#{Thread.current.object_id}|Mkto::download_tsv"

    LOGGER.info(progname) { "Start: job_id: #{job_id}" }

    if $debugmode
      LOGGER.debug(progname) do
        <<~HEREDOC.gsub(/\n/, '')
          GET: #{self.class.config['marketo_url']}/bulk/v1/leads/export/#{job_id}/file.json?access_token=#{@mkto_token}
        HEREDOC
      end
    end

    begin
      response = HTTParty.get("#{self.class.config['marketo_url']}/bulk/v1/leads/export/#{job_id}/file.json?\
access_token=#{@mkto_token}")
      LOGGER.info(progname) { "HTTParty response: #{response.code}, #{response.message}" }
      unless response.code == 200
        LOGGER.error(progname) { 'API response not 200 OK' }
        raise StandardError, 'API response not 200 OK'
      end
    rescue URI::InvalidURIError => e
      LOGGER.error(progname) { 'HTTParty error' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    rescue StandardError => e
      LOGGER.error(progname) { 'HTTParty standard error' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    end

    LOGGER.debug(progname) { "Response class: #{response.class.name}" }
    LOGGER.debug(progname) { "Response body class: #{response.body.class.name}" }
    # The body is likely in UTF-8 in reality, because that's how the resulting file looks like when
    # written out in binary mode. For some reason, Mkto/HTTParty sets it to ASCII-8BIT, though.
    LOGGER.debug(progname) { "Response body encoding: #{response.body.encoding}" }

    # FYI good example of using a hash to convey parameters.
    # Parse only for logging purposes.
    begin
      parsed_response = CSV.parse(response.body, { headers: true, col_sep: '\t', liberal_parsing: true })
    rescue StandardError => e
      LOGGER.error(progname) { 'CSV.parse error' }
      LOGGER.error(progname) { "Error: #{e.inspect}" }
      raise
    else
      unless parsed_response
        LOGGER.error(progname) { 'Parsing TSV failed' }
        raise
      end
    end

    if $responsedebug
      LOGGER.debug(progname) do
        "RESPONSE: #{parsed_response.length} lines, #{response.body.length} bytes, #{response.body.class}"
      end
    end

    # Write raw data into file.
    outfile = File.open("c:/temp/_#{job_id}.tsv", 'wb:ASCII-8BIT:UTF-8')
    # It seems this is not needed with the correct File.open parameters being used. Instead, we can just write.
    # outfile.write response.body.encode(response.body.encoding, universal_newline: true)
    outfile.write(response.body)
    outfile.close
    LOGGER.info(progname) { "TSV file c:/temp/_#{job_id}.tsv written" }
    File.rename("c:/temp/_#{job_id}.tsv", "c:/temp/#{job_id}.tsv")
    LOGGER.info(progname) { "TSV file renamed to c:/temp/#{job_id}.tsv" }

    parsed_response
  end

  # Check Marketo API response is valid and succesful.
  def check_api_response(response)
    progname = "#{Thread.current.object_id}|Mkto::check_api_response"
    LOGGER.info(progname) { 'Start' }
    LOGGER.info(progname) { "HTTParty response: #{response.code}, #{response.message}" }

    unless response
      LOGGER.info(progname) { 'No response data provided, check failed' }
      return false
    end

    unless response.instance_of?(HTTParty::Response)
      LOGGER.info(progname) { 'Response if of not class HTTParty::Response, check failed' }
      return false
    end

    unless response.code == 200
      LOGGER.error(progname) { 'API response not 200 OK' }
      return false
    end

    begin
      parsed_response = JSON.parse(response.body)
    rescue StandardError => e
      LOGGER.info(progname) { 'Parsing failed, check failed' }
      LOGGER.info(progname) { "Error #{e.inspect}" }
      return false
    else
      LOGGER.debug(progname) { "Resp: #{JSON.pretty_generate(parsed_response)}" } if $responsedebug
      LOGGER.debug(progname) { "access_token: #{parsed_response['access_token']}" }
      LOGGER.debug(progname) { "success: #{parsed_response['success']}" }
    end

    unless parsed_response.key?('access_token') ||
           parsed_response.key?('requestId')
      LOGGER.warn(progname) { 'Mkto API request check failed' }
      LOGGER.warn(progname) { "Response: #{JSON.pretty_generate(parsed_response)}" }
      return false
    end
    LOGGER.info(progname) { 'Mkto API request check passed' }
    LOGGER.info(progname) { 'End' }
    true
  end
end
