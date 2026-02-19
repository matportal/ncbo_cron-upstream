require 'date'
require_relative 'object_analytics'

module NcboCron
  module Models
    class MatomoPageVisitsAnalytics < ObjectAnalytics
      def initialize(start_date: Date.today.prev_month.to_s, old_data: {})
        super(redis_field: 'pages_analytics', start_date: Date.today.prev_month.to_s, old_data: {})
      end

      def fetch_object_analytics(logger, matomo_conn)
        @logger = logger
        @matomo_conn = matomo_conn

        aggregated_results = {}
        month = Date.today.prev_month
        response = @matomo_conn.api_get(
          method: 'Actions.getPageUrls',
          period: 'month',
          date: month.to_s,
          params: {
            flat: 1,
            filter_limit: -1,
            showColumns: 'nb_hits'
          }
        )

        rows = normalize_rows(response)
        rows.each do |row|
          page_path = row_value(row, 'label')
          next if page_path.nil?

          page_count = row_value(row, 'nb_hits').to_i
          page_path = page_path.chop if page_path.end_with?('/') && !page_path.eql?('/')
          next if page_count < 10

          old_page_count = aggregated_results[page_path] || 0
          aggregated_results[page_path] = old_page_count + page_count
        end

        { "all_pages" => aggregated_results }
      end

      private

      def normalize_rows(response)
        return response if response.is_a?(Array)
        return [] unless response.is_a?(Hash)
        return [] if response.empty?
        if response.values.length == 1 && response.values.first.is_a?(Array)
          response.values.first
        else
          []
        end
      end

      def row_value(row, key)
        row[key] || row[key.to_sym]
      end

      # We don't want to fill missing data for page analytics.
      def fill_missing_data(ga_data)
        ga_data
      end
    end
  end
end
