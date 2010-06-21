$LOAD_PATH.unshift File.dirname(__FILE__) + '/..'

require 'drb'

require 'rubyrep'

module RR
  # The proxy to a remote database connection
  class DatabaseProxy
    
    # Ensure that the proxy object always stays on server side and only remote
    # references are returned to the client.
    include DRbUndumped 
    
    # Default tcp port to listen on
    DEFAULT_PORT = 9876
    
    # A simple Hash to hold Session object
    # Purpose: preventing them from being garbage collected when they are only referenced through Drb
    attr_accessor :session_register    
    
    def initialize
      self.session_register = {}
    end
    
    # Create a ProxyConnection according to provided configuration Hash.
    # +config+ is a hash as described by ActiveRecord::Base#establish_connection
    def create_session(config)
      if config.include?(:proxy_host)
        # Need to circumvent possible NAT issues
        
        protocol = config.include?(:proxy_SSLCACertificateFile) ? 'drbssl' : 'druby'

        # HACK. Ugly and hacky way to overcome NAT problems.
        #       Not Thread Safe!
        #       Will cause problems when different clients refer to the proxy
        #       by a different NAT name.
        # TODO. Find a better way to do this...
        Kernel.eval %Q{
          def DRb.uri
            "#{protocol}://#{config[:proxy_host]}:#{config[:proxy_port]}"
          end
        }
      end
      
      session = ProxyConnection.new config
      self.session_register[session] = session
      session
    end
    
    # Destroys the given session from the session register
    def destroy_session(session)
      session.destroy
      session_register.delete session
    end
    
    # Returns 'pong'. Used to verify that a working proxy is running.
    def ping
      'pong'
    end
    
    # Terminates this proxy
    def terminate!
      # AL: The only way I could find to kill the main thread from a sub thread
      Thread.main.raise SystemExit
    end
  end
end

