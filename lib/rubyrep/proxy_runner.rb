$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'optparse'
require 'drb'
require 'drb/ssl'

module RR
  # This class implements the functionality of the rrproxy.rb command.
  class ProxyRunner

    CommandRunner.register 'proxy' => {
      :command => self,
      :description => 'Proxies connections from rubyrep commands to the database'
    }
    
    # Default options to start a DatabaseProxy server
    DEFAULT_OPTIONS = {
      :port => DatabaseProxy::DEFAULT_PORT,
      :host => '',
    }
    
    # Parses the given command line parameter array.
    # Returns 
    #   * the options hash or nil if command line parsing failed
    #   * status (as per UNIX conventions: 1 if parameters were invalid, 0 otherwise)
    def get_options(args)
      options = DEFAULT_OPTIONS
      status = 0

      parser = OptionParser.new do |opts|
        opts.banner = "Usage: #{$0} proxy [options]"
        opts.separator ""
        opts.separator "Specific options:"

        opts.on("-h", "--host", "=IP_ADDRESS", "IP address to listen on. Default: binds to all IP addresses of the computer") do |arg|
          options[:host] = arg
        end

        opts.on("-p", "--port", "=PORT_NUMBER", Integer, "TCP port to listen on. Default port: #{DatabaseProxy::DEFAULT_PORT}") do |arg|
          options[:port] = arg
        end

        opts.on("-k", "--key", "=FILE", "File containing the SSL private key") do |arg|
          options[:key] = arg
        end

        opts.on("-c", "--certificate", "=FILE", "File containing the SSL certificate") do |arg|
          options[:certificate] = arg
        end
        
        opts.on_tail("--help", "Show this message") do
          $stderr.puts opts
          options = nil
        end
      end

      begin
        parser.parse!(args)
      rescue Exception => e
        $stderr.puts "Command line parsing failed: #{e}"
        $stderr.puts parser.help
        options = nil
        status = 1
      end
  
      return options, status
    end

    # Builds the druby URL from the given options and returns it
    def build_url(options)
      protocol = options.include?(:certificate) ? 'drbssl' : 'druby'
      "#{protocol}://#{options[:host]}:#{options[:port]}"
    end

    # Builds the druby config from the given options and returns it
    def build_config(options)
      if options.include?(:certificate)
        {:SSLPrivateKey  => OpenSSL::PKey::RSA.new(File.read(options[:key])),
         :SSLCertificate => OpenSSL::X509::Certificate.new(File.read(options[:certificate])),
        }
      else
        nil
      end
    end

    # Starts a proxy server under the given druby URL
    def start_server(options)
      url = build_url(options)
      config = build_config(options)

      proxy = DatabaseProxy.new

      # DRb service fails on Exception, so restart it
      begin
        DRb.start_service(url, proxy, config)
        DRb.thread.join
        done = true
      rescue Errno::ECONNABORTED, Errno::ECONNRESET, Errno::EHOSTUNREACH, Errno::EPIPE, Errno::ETIMEDOUT, OpenSSL::SSL::SSLError => e
        STDERR.puts "#{e.class.name}: #{e}"
        done = false
      rescue Exception => e
        STDERR.puts "#{e.class.name}: #{e}"
        done = true
      end until done
    end
    
    # Runs the ProxyRunner (processing of command line & starting of server)
    # args: the array of command line options with which to start the server
    def self.run(args)
      runner = ProxyRunner.new
      
      options, status = runner.get_options(args)
      if options
        runner.start_server(options)
      end
      status
    end

  end
end


