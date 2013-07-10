module Sidekiq
  class Throttler
    module Storage
      ##
      # Stores job executions in a Hash of Arrays
      class Memory
        def initialize
          @hash = Hash.new { |hash, key| hash[key] = [] }
        end

        ##
        # Number of executions for +key+.
        #
        # @param [String]
        #   Key to fetch count for
        #
        # @return [Fixnum]
        #   Execution count
        def count(key)
          @hash[key].length
        end

        ##
        # Remove entries older than +cutoff+.
        #
        # @param [String] key
        #   The key to prune
        #
        # @param [Time] cutoff
        #   Oldest allowable time
        def prune(key, cutoff)
          @hash[key].select! { |time| time > cutoff }
        end

        ##
        # Add a new entry to the hash
        #
        # @param [String] key
        #   The key to append to
        #
        # @param [Time]
        #   The time to insert
        def append(key, time)
          @hash[key] << time
        end
      end
    end # Storage
  end # Throttler
end # Sidekiq
