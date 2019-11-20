require 'sidekiq'
require 'active_support/core_ext/numeric/time'
require 'singleton'

require 'sidekiq/throttler/version'
require 'sidekiq/throttler/rate_limit'

require 'sidekiq/throttler/storage/memory'
require 'sidekiq/throttler/storage/redis'

module Sidekiq
  ##
  # Sidekiq server middleware. Throttles jobs when they exceed limits specified
  # on the worker. Jobs that exceed the limit are requeued with a delay.
  class Throttler
    def initialize(options = {})
      @options = options.dup
    end

    ##
    # Passes the worker, arguments, and queue to {RateLimit} and either yields
    # or requeues the job depending on whether the worker is throttled.
    #
    # @param [Sidekiq::Worker] worker
    #   The worker the job belongs to.
    #
    # @param [Hash] msg
    #   The job message.
    #
    # @param [String] queue
    #   The current queue.
    def call(worker, msg, queue)
      rate_limit = RateLimit.new(worker, msg['args'], queue, @options)

      rate_limit.within_bounds do
        yield
      end

      # We now allow for explicitly setting the exceeded behavior.
      # By default (or with a :retry value), the exceeded behavior follows the
      # previous behavior and retries the job.
      #
      # Otherwise, it attempts to use the behavior specified in the class options
      # The specified behavior should be a a proc that takes up to 4 arguments:
      # 1: the period of the rate limiter
      # 2: the worker for the job
      # 3: the message payload
      # 4: the queue the job was pulled from
      # NB: The exceeded behavior passed *MUST* be a proc if you are using less
      # then 4 arguments. Because the rate limiter itself always passes four
      # arguments it doesn't work with a lambda.

      if rate_limit.options['exceeded'].nil? || rate_limit.options['exceeded'] == :retry
        rate_limit.exceeded do |delay|
          # Sidekiq is going to run it at the given time
          # partially copied from Sidekiq::Worker#perform_in https://github.com/mperham/sidekiq/blob/4eb54965dc0acf1920d2d0eb8c678b1f77efd0c9/lib/sidekiq/worker.rb#L55
          interval = delay.to_f
          now = Time.now.to_f
          ts = (interval < 1_000_000_000 ? now + interval : interval)
          msg['at'] = ts unless ts <= now

          # Using .client_push instead of .perform_in to send all the data we have about the worker (like queue, retry, throttle), not only the args
          # By doing so the worker doesn't need to ask for the sidekiq options again by calling Sidekiq::Client.get_sidekiq_options
          # Before it was: worker.class.perform_in(delay, *msg['args'])
          worker.class.client_push(msg)
        end
      else
        rate_limit.exceeded &rate_limit.options['exceeded']
      end

      rate_limit.execute
    end

  end # Throttler
end # Sidekiq
