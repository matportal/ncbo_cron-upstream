require 'logger'
require 'json'
require 'benchmark'
require 'google/analytics/data'
require 'google/apis/analytics_v3'
require 'google/api_client/auth/key_utils'

require_relative 'matomo_connector'
require_relative 'matomo_ontology_visits_analytics'
require_relative 'matomo_user_visits_analytics'
require_relative 'matomo_page_visits_analytics'
require_relative 'ontology_visits_analytics'
require_relative 'user_visits_analytics'
require_relative 'page_visits_analytics'

module NcboCron
  module Models
    class ObjectAnalyticsJob
      def initialize(logger)
        @redis_host = LinkedData.settings.ontology_analytics_redis_host
        @redis_port = LinkedData.settings.ontology_analytics_redis_port

        @data_file =  NcboCron.settings.analytics_path_to_ga_data_file



        @logger = logger
        if matomo_configured?
          @logger.info "Authenticating with the Matomo Reporting API..."
          @analytics_conn = MatomoConnector.new(
            url: NcboCron.settings.matomo_url,
            token: NcboCron.settings.matomo_api_token,
            site_id: NcboCron.settings.matomo_site_id,
            logger: @logger,
            insecure: NcboCron.settings.matomo_insecure
          )
          @analytics_objects = [
            NcboCron::Models::MatomoOntologyVisitsAnalytics,
            NcboCron::Models::MatomoUsersVisitsAnalytics,
            NcboCron::Models::MatomoPageVisitsAnalytics
          ]
        else
          @logger.info "Authenticating with the Google Analytics Endpoint..."
          @analytics_conn = GoogleAnalyticsConnector.new
          @analytics_objects = [
            NcboCron::Models::OntologyVisitsAnalytics,
            NcboCron::Models::UsersVisitsAnalytics,
            NcboCron::Models::PageVisitsAnalytics
          ]
        end
      end

      def run
        redis = Redis.new(:host => @redis_host, :port => @redis_port)
        @logger.info "Starting analytics refresh..."
        time = Benchmark.realtime do
          @logger.info "Fetching saved analytics data..."
          save = {}
          @old_data = read_old_data
          @analytics_objects.each do |analytic_object|
            analytic_object = analytic_object.new(start_date: detect_latest_date, old_data: @old_data)
            @logger.info "Start fetching new #{analytic_object.redis_field}  data..."
            new_data = analytic_object.full_data(@logger, @analytics_conn)
            save[analytic_object.redis_field] = new_data
            redis.set(analytic_object.redis_field, Marshal.dump(new_data))
            @logger.info "Completed fetching #{analytic_object.redis_field}  data..."
          end
          save_data_in_file(save)
        end
        @logger.info "Completed analytics refresh in #{(time / 60).round(1)} minutes."
        @logger.flush
      end

      private
      def read_old_data
        return {} unless File.exists?(@data_file) && !File.zero?(@data_file)
        JSON.parse(File.read(@data_file))
      end

      def detect_latest_date
        begin
          input_date = Date.parse(@old_data['latest_date_save']).prev_month(6)
          start_of_month = Date.new(input_date.year, input_date.month, 1)
          start_of_month.to_s
        rescue
          nil
        end

      end

      def matomo_configured?
        NcboCron.settings.respond_to?(:matomo_url) &&
          NcboCron.settings.respond_to?(:matomo_site_id) &&
          NcboCron.settings.respond_to?(:matomo_api_token) &&
          NcboCron.settings.matomo_url.to_s.strip != "" &&
          NcboCron.settings.matomo_site_id.to_s.strip != "" &&
          NcboCron.settings.matomo_api_token.to_s.strip != ""
      end

      def save_data_in_file(new_data, saved_date =  Date.today.to_s, data_file =  @data_file)
        new_data["latest_date_save"] =  saved_date
        # Ensure the directory exists before creating the file
        FileUtils.mkdir_p(File.dirname(data_file))
        # Open the file with 'w+' mode to create if not exist and write
        File.open(data_file, 'w+') do |f|
          f.write(new_data.to_json)
        end
      end
    end

  end
end
