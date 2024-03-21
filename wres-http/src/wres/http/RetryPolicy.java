package wres.http;


import java.time.Duration;
import java.time.Instant;

/**
 * An object to define the retry policy of a WebClient
 */

public class RetryPolicy
{

    /**
     * The max amount of time spent on retrying and calls before stopping
     * Stops making calls even if there are still retry attempts
     * Default = 0 unlimited time
     */
    private final Duration maxRetryTime;

    /**
     * The total number of retries attempted before stopping
     * Default = 0 no retry attempts
     */
    private final int maxRetryCount;



    /**
     * A RetryPolicy that follows a builder pattern
     * @param builder the builder to create a retryPolicy
     */
    private RetryPolicy( Builder builder )
    {
        this.maxRetryTime = builder.maxRetryTime;
        this.maxRetryCount = builder.maxRetryCount;
    }

    /**
     * gets max retry count
     * @return int of max retry count
     */
    public int getMaxRetryCount()
    {
        return this.maxRetryCount;
    }

    /**
     * gets max retry duration
     * @return a Duration of how long to try attempts
     */
    public Duration getMaxRetryTime()
    {
        return this.maxRetryTime;
    }

    /**
     * Method to determine if a retry should be attempted in the WebClient
     * The first stopper encountered causes this to fail
     * @param start Start time of calling
     * @param now Current time when checking retry
     * @param attemptCount The amount of attempts a call has been made
     * @return boolean if retries should be attempted
     */
    public boolean shouldRetry( Instant start, Instant now, int attemptCount )
    {
        boolean shouldRetry = true;
        // Check that MaxRetryTime is not unlimited (ZERO)
        if ( !this.getMaxRetryTime().isZero() )
        {
            // if MaxRetryTime is greater than currentTime, continue retries
            shouldRetry = start.plus( this.getMaxRetryTime() )
                               .isAfter( now );
        }

        // If we already have reached a failure point, no reason to check attempt count
        if ( shouldRetry )
        {
            shouldRetry = this.getMaxRetryCount() > attemptCount;
        }

        return shouldRetry;
    }

    /**
     * Builds an instance.
     */
    public static class Builder
    {
        private Duration maxRetryTime = Duration.ZERO;

        private int maxRetryCount = 0;

        /**
         * Sets max retry time
         * @param maxRetryTime the value to set
         * @return a builder
         */
        public Builder maxRetryTime( Duration maxRetryTime )
        {
            this.maxRetryTime = maxRetryTime;
            return this;
        }

        /**
         * Sets max retry count
         * @param retryCount value to set
         * @return a builder
         */
        public Builder maxRetryCount( int retryCount )
        {
            this.maxRetryCount = retryCount;
            return this;
        }

        /**
         * The default builder has 0 retries
         * @return a RetryPolicy
         */
        public RetryPolicy build()
        {
            return new RetryPolicy( this );
        }
    }
}
