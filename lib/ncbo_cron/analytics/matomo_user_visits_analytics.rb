require 'date'
require_relative 'object_analytics'

module NcboCron
  module Models
    class MatomoUsersVisitsAnalytics < ObjectAnalytics
      def initialize(start_date:, old_data: {})
        super(redis_field: 'user_analytics', start_date: start_date, old_data: old_data)
      end

      def fetch_object_analytics(logger, matomo_conn)
        @logger = logger
        @matomo_conn = matomo_conn

        aggregated_results = {}
        month = Date.new(@start_date.year, @start_date.month, 1)
        last = Date.new(Date.today.year, Date.today.month, 1)

        while month <= last
          @logger.info "Fetching Matomo user analytics for #{month}..."
          @logger.flush
          response = @matomo_conn.api_get(
            method: 'VisitsSummary.get',
            period: 'month',
            date: month.to_s,
            params: {}
          )

          count = response_value(response, %w[nb_new_visitors nb_users nb_uniq_visitors nb_visits])
          results = [[-1, month.year.to_s, month.month.to_s, count.to_i]]
          aggregate_results(aggregated_results, results)
          month = month.next_month
        end

        { "all_users" => aggregated_results }
      end

      private

      def response_value(response, keys)
        return 0 unless response.is_a?(Hash)
        keys.each do |key|
          return response[key].to_i if response.key?(key)
        end
        0
      end
    end
  end
end
