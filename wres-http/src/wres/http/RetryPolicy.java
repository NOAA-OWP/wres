package wres.http;


import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiPredicate;

import lombok.Builder;
import lombok.Getter;

/**
 * <p>A retry policy that is invoked when connections to a web server fail because:
 * <ol>
 * <li>The http error code corresponds to a code that should be retried; or</li>
 * <li>The connection fails with an exception or underlying cause (nested exception) that should be retried.</li>
 * </ol>
 *
 * <p>The retries themselves may be constrained in one of two ways, namely:
 * <ol>
 * <li>A maximum duration or time elapsed since the first retry; and</li>
 * <li>A maximum number of retries.</li>
 * </ol>
 *
 * <p>In summary, there are policy triggers (first list) and policy constraints (second list). A retry is only attempted
 * when one of the policy triggers occurs and that trigger leads to a determination that all policy constraints are met.
 */

@Getter
@Builder
public class RetryPolicy
{

    /**The maximum possible duration. */
    private static final Duration MAXIMUM_DURATION = Duration.ofSeconds( Long.MAX_VALUE, 999_999_999 );

    /**
     * The max amount of time spent on retrying and calls before stopping. Stops making calls even if there are still
     * retry attempts. Default is effectively unlimited time.
     */
    @Builder.Default
    private final Duration maxRetryTime = MAXIMUM_DURATION;

    /**
     * The total number of retries attempted before stopping. Default is no retries.
     */
    @Builder.Default
    private final int maxRetryCount = 0;

    /**
     * The HTTP error codes that should be retried.
     */
    @Builder.Default
    private final Set<Integer> errorCodes = Set.of( 500,
                                                    502,
                                                    503,
                                                    504,
                                                    523,
                                                    524 );

    /**
     * The particular conditions under which an {@link IOException} should be retried, conditionally on a retry count.
     * No default.
     */
    @Builder.Default
    private final BiPredicate<Exception, Integer> exceptionPolicy = RetryPolicy.getDefaultExceptionPolicy();

    /**
     * Determines whether a retry should be attempted for the supplied time constraints and retry count.
     *
     * @param start the start time of calling
     * @param now the current time when checking a retry
     * @param retryCount the current retry count
     * @return whether a further retry should be attempted
     */
    public boolean shouldRetry( Instant start, Instant now, int retryCount )
    {
        return this.getMaxRetryCount() > retryCount
               && ( this.getMaxRetryTime() == MAXIMUM_DURATION ||
                    Duration.between( start, now )
                            .compareTo( this.getMaxRetryTime() ) < 0 );
    }

    /**
     * Determines whether a retry should be attempted for the supplied exception.
     *
     * @param exception the exception
     * @param retryCount the current retry count
     * @return whether a further retry should be attempted
     * @throws NullPointerException if the exception is null
     */
    public boolean shouldRetry( Exception exception, int retryCount )
    {
        Objects.requireNonNull( exception );

        return this.exceptionPolicy.test( exception, retryCount );
    }

    /**
     * Determines whether a retry should be attempted for the supplied HTTP error code.
     *
     * @param errorCode the error code
     * @return whether a further retry should be attempted
     */
    public boolean shouldRetry( int errorCode )
    {
        return this.errorCodes.contains( errorCode );
    }

    /**
     * @return a default policy for performing retries when an exception is encountered
     */

    private static BiPredicate<Exception, Integer> getDefaultExceptionPolicy()
    {
        return ( ioe, retryCount ) ->
        {
            if ( RetryPolicy.shouldRetryIndividualThrowable( ioe, retryCount ) )
            {
                return true;
            }

            Throwable cause = ioe.getCause();

            while ( !Objects.isNull( cause ) )
            {
                if ( RetryPolicy.shouldRetryIndividualThrowable( cause, retryCount ) )
                {
                    return true;
                }

                cause = cause.getCause();
            }

            return false;
        };
    }

    /**
     * Look at an individual throwable and determines whether it should be retried.
     *
     * @param t The throwable
     * @param retryCount The number of times the request with exception T has been retried
     * @return true when the exception can be safely retried, false otherwise.
     */
    private static boolean shouldRetryIndividualThrowable( Throwable t, int retryCount )
    {
        if ( t instanceof HttpTimeoutException )
        {
            return true;
        }

        if ( t instanceof ConnectException )
        {
            return true;
        }

        if ( t instanceof SocketException )
        {
            return true;
        }

        if ( t instanceof SocketTimeoutException )
        {
            return true;
        }

        if ( t instanceof UnknownHostException
             && retryCount > 0 )
        {
            return true;
        }

        if ( t instanceof EOFException )
        {
            return true;
        }

        if ( t instanceof InterruptedIOException
             && Objects.nonNull( t.getMessage() )
             && t.getMessage()
                 .toLowerCase()
                 .contains( "timeout" ) )
        {
            return true;
        }

        return t instanceof IOException
               && Objects.nonNull( t.getMessage() )
               && t.getMessage()
                   .toLowerCase()
                   .contains( "connection reset" );
    }
}
