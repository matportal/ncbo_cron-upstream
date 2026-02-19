require 'json'
require 'net/http'
require 'openssl'
require 'uri'

module NcboCron
  module Models
    class MatomoConnector
      DEFAULT_TIMEOUT = 60

      def initialize(url:, token:, site_id:, logger: nil, timeout: DEFAULT_TIMEOUT, insecure: false)
        @base_url = url.to_s.strip.chomp("/")
        @token = token.to_s.strip
        @site_id = site_id.to_s.strip
        @logger = logger
        @timeout = timeout
        @insecure = insecure
      end

      def api_get(method:, period:, date:, params: {})
        raise "Matomo URL is not configured" if @base_url.empty?
        raise "Matomo site_id is not configured" if @site_id.empty?
        raise "Matomo token_auth is not configured" if @token.empty?

        query = {
          module: 'API',
          method: method,
          idSite: @site_id,
          period: period,
          date: date,
          format: 'JSON',
          token_auth: @token
        }.merge(params)

        uri = URI(@base_url + "/")
        uri.query = URI.encode_www_form(query)
        request(uri)
      end

      private

      def request(uri)
        http = Net::HTTP.new(uri.host, uri.port)
        http.use_ssl = (uri.scheme == 'https')
        if http.use_ssl? && @insecure
          http.verify_mode = OpenSSL::SSL::VERIFY_NONE
        end
        http.read_timeout = @timeout
        http.open_timeout = @timeout

        response = http.get(uri.request_uri)
        unless response.is_a?(Net::HTTPSuccess)
          raise "Matomo API request failed (#{response.code}): #{response.body}"
        end

        data = JSON.parse(response.body)
        if data.is_a?(Hash) && data['result'] == 'error'
          raise "Matomo API error: #{data['message']}"
        end
        data
      end
    end
  end
end
