package wres.io.utilities;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import javax.ws.rs.WebApplicationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.util.functional.ExceptionalFunction;

public class WebRetryStrategy implements Cloneable
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WebRetryStrategy.class );
    private int attemptsLeft;
    private Duration waitDuration;
    private boolean hasAsked;

    public WebRetryStrategy( final int attempts, final Duration waitDuration)
    {
        this.attemptsLeft = attempts;
        this.waitDuration = waitDuration;
    }

    public WebRetryStrategy( final int attempts)
    {
        this.attemptsLeft = attempts;
        this.waitDuration = Duration.of( 500, ChronoUnit.MILLIS);
    }

    public WebRetryStrategy()
    {
        this.attemptsLeft = 5;
        this.waitDuration = Duration.of( 500, ChronoUnit.MILLIS);
    }

    public <P, R, E extends Exception> R execute( ExceptionalFunction<P, R, E> function, P arguments) throws E, OutOfAttemptsException
    {
        R result = null;

        while (this.shouldTry())
        {
            try
            {
                result = function.call( arguments );
                break;
            }
            catch ( WebApplicationException exception )
            {
                this.manageError( exception );
            }
        }

        return result;
    }

    public boolean shouldTry()
    {
        // If we've never tried, yes, we want to try
        if (!hasAsked)
        {
            this.hasAsked = true;
            return true;
        }

        return this.attemptsLeft > 0;
    }

    public void manageError(WebApplicationException exception) throws OutOfAttemptsException
    {
        if (400 >= exception.getResponse().getStatus() && exception.getResponse().getStatus() < 500)
        {
            throw exception;
        }

        this.attemptsLeft--;

        if (!this.shouldTry())
        {
            throw new OutOfAttemptsException("Ran out of number of allowable attempts.", exception);
        }

        LOGGER.trace("A call failed but trying again...");
        this.waitUntilNextTry();
    }

    private void waitUntilNextTry()
    {
        try
        {
            Thread.sleep( this.waitDuration.toMillis() );
        }
        catch ( InterruptedException interruption )
        {
            LOGGER.warn( "Interrupted while pausing before retry.",
                         interruption );
            Thread.currentThread().interrupt();
        }
    }
}
