# frozen_string_literal: true

require 'json'

# Class to load config file
class ConfigLoader
  def initialize
    load_config_data
  end

  def config_all
    @config_data
  end

  def config_param(param)
    @config_data[param]
  end

  private

  def load_config_data
    config_file_path = 'config.json'
    begin
      config_file_contents = File.read(config_file_path)
    rescue Errno::ENOENT
      warn 'Missing config file'
      raise
    end
    @config_data = JSON.parse config_file_contents
  end
end
