require 'date'
require_relative 'object_analytics'

module NcboCron
  module Models
    class MatomoOntologyVisitsAnalytics < ObjectAnalytics
      ONTOLOGY_ANALYTICS_REDIS_FIELD = 'ontology_analytics'

      def initialize(start_date:, old_data: {})
        super(redis_field: ONTOLOGY_ANALYTICS_REDIS_FIELD, start_date: start_date, old_data: old_data)
        @ont_acronyms = LinkedData::Models::Ontology.where.include(:acronym).all.map { |o| o.acronym }
        @matomo_site_url = NcboCron.settings.respond_to?(:matomo_site_url) ? NcboCron.settings.matomo_site_url.to_s.strip : ""
        @matomo_site_url = @matomo_site_url.chomp("/") unless @matomo_site_url.empty?
      end

      def fetch_object_analytics(logger, matomo_conn)
        @logger = logger
        @matomo_conn = matomo_conn

        aggregated_results = {}
        @ont_acronyms.each do |acronym|
          @logger.info "Fetching Matomo ontology analytics for #{acronym}..."
          @logger.flush
          aggregated_results[acronym] ||= {}
          monthly_counts = fetch_monthly_page_hits("/ontologies/#{acronym}", acronym)
          results = monthly_counts.map do |month_date, count|
            [acronym, month_date.year.to_s, month_date.month.to_s, count.to_i]
          end
          aggregate_results(aggregated_results[acronym], results)
        end
        aggregated_results
      end

      private

      def fetch_monthly_page_hits(path_pattern, acronym)
        data = {}
        month = Date.new(@start_date.year, @start_date.month, 1)
        last = Date.new(Date.today.year, Date.today.month, 1)

        while month <= last
          data[month] = fetch_page_hits_for_month(month, path_pattern, acronym)
          month = month.next_month
        end
        data
      end

      def fetch_page_hits_for_month(month, path_pattern, acronym)
        if @matomo_site_url.empty?
          @logger.warn "Matomo site URL is not configured; falling back to label filter for #{acronym}"
          return fetch_page_hits_by_label(month, path_pattern)
        end

        response = @matomo_conn.api_get(
          method: 'Actions.getPageUrls',
          period: 'month',
          date: month.to_s,
          params: {
            flat: 1,
            filter_limit: -1,
            showColumns: 'nb_hits',
            segment: "pageUrl==#{@matomo_site_url}/ontologies/#{acronym}"
          }
        )

        rows = normalize_rows(response)
        rows.sum do |row|
          label = row_value(row, 'label').to_s
          label.start_with?("/ontologies/#{acronym}") ? row_value(row, 'nb_hits').to_i : 0
        end
      end

      def fetch_page_hits_by_label(month, path_pattern)
        params = {
          flat: 1,
          filter_limit: -1,
          filter_column: 'label',
          filter_pattern: path_pattern,
          filter_column_recursive: 'label',
          filter_pattern_recursive: path_pattern,
          showColumns: 'nb_hits'
        }

        response = @matomo_conn.api_get(
          method: 'Actions.getPageUrls',
          period: 'month',
          date: month.to_s,
          params: params
        )

        rows = normalize_rows(response)
        rows.sum { |row| row_value(row, 'nb_hits').to_i }
      end

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
    end
  end
end
