package wres.io.utilities;

import java.io.IOException;
import java.lang.annotation.Annotation;
import java.net.URI;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.EntityTag;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.util.functional.ExceptionalFunction;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*") // thanks https://stackoverflow.com/questions/16520699/mockito-powermock-linkageerror-while-mocking-system-class#21268013
public class WebRetryStrategyTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( WebRetryStrategyTest.class);

    @Test
    public void helperFunctionThrowsTheGivenExceptions()
    {
        List<Throwable> exceptions = new ArrayList<>(  );
        exceptions.add(new IOException( "This is the first exception" ));
        exceptions.add(new NullPointerException( "This is the second exception" ));
        exceptions.add(new WebApplicationException( "This is the third exception" ));

        ExceptionalFunction<String, String, Throwable> flip = this.getResultFlipper( 3, exceptions );
        int failCount = 0;

        try
        {
            flip.call( "First Exception" );
        }
        catch (Throwable t)
        {
            if (!t.equals( exceptions.get(0) ))
            {
                Assert.fail( "The helper function did not throw the first exception" );
            }

            failCount++;
        }

        try
        {
            flip.call("Second Exception");
        }
        catch (Throwable t)
        {
            if (!t.equals( exceptions.get(1) ))
            {
                Assert.fail("The helper function did not throw the second exception");
            }

            failCount++;
        }

        try
        {
            flip.call("Third Exception");
        }
        catch (Throwable t)
        {
            if (!t.equals(exceptions.get(2)))
            {
                Assert.fail("The helper function did not throw the third exception");
            }

            failCount++;
        }

        Assert.assertEquals( "All three exceptions should have been thrown by the helper function.",3, failCount );
    }

    @Test
    public void helperFunctionHitSuccess()
    {
        List<Throwable> exceptions = new ArrayList<>(  );
        exceptions.add(new IOException( "This is an exception" ));

        ExceptionalFunction<String, String, Throwable> flip = this.getResultFlipper( 1, exceptions );
        int failCount = 0;
        int successCount = 0;

        for (int i = 0; i < 5; ++i)
        {
            try
            {
                flip.call( "Whatever" );
                successCount++;
            }
            catch (Throwable t)
            {
                failCount++;
            }
        }

        Assert.assertEquals( "The helper function should have only thrown one exception", 1, failCount );
        Assert.assertEquals( 4, successCount );
    }

    @Test(timeout = 2000)
    /*
     * Tests to make sure that the strategy retries until it encounters a successful run
     */
    public void failThenSucceed()
    {
        List<Throwable> exceptions = new ArrayList<>();

        exceptions.add(this.getWebApplicationException( 500 ));
        ExceptionalFunction<String, String, Throwable> flip = this.getResultFlipper( 2, exceptions );

        WebRetryStrategy strategy = new WebRetryStrategy(  );
        boolean failHit = false;
        boolean successHit = false;

        try
        {
            while (strategy.shouldTry())
            {
                try
                {
                    flip.call( "Here goes nothing..." );
                    successHit = true;
                    break;
                }
                catch (WebApplicationException exception)
                {
                    failHit = true;
                    strategy.manageError( exception );
                }
            }
        }
        catch (OutOfAttemptsException oae)
        {
            LOGGER.error( oae.toString() );
            Assert.fail("The strategy decided to raise the issue rather than trying again.");
        }
        catch (Throwable exception)
        {
            LOGGER.error( exception.toString() );
            Assert.fail( "The process of testing ran into an error." );
        }

        Assert.assertTrue( "The strategy did not handle a failure.", failHit );
        Assert.assertTrue("The function never ran successfully.", successHit);
    }

    @Test(timeout = 2000)
    /*
     * Tests to make sure that non-WebApplicationExceptions are not eaten and are thrown
     */
    public void nonWebExceptionsAreThrown()
    {
        WebRetryStrategy strategy = new WebRetryStrategy(  );
        List<IOException> exceptionsToThrow = new ArrayList<>(  );
        exceptionsToThrow.add(new IOException("This is just a non-web application exception"));
        ExceptionalFunction<String, String, IOException> flipper = getResultFlipper( 3, exceptionsToThrow );

        try
        {
            while (strategy.shouldTry())
            {
                try
                {
                    flipper.call( "Here goes nothing..." );
                    break;
                }
                catch (WebApplicationException exception)
                {
                    strategy.manageError( exception );
                }
            }
        }
        catch ( OutOfAttemptsException e )
        {
            Assert.fail( "The execution ran out of attempts to execute the "
                         + "function rather than stopping on the first error" );
        }
        catch ( IOException e )
        {
            return;
        }

        Assert.fail( "The execution should have stopped due to a non-web exception" );
    }

    @Test(timeout = 2000)
    /*
     * Tests to make sure that the strategy still fails on http 400 exceptions
     */
    public void failOn400Exception()
    {
        List<Throwable> exceptions = new ArrayList<>();

        exceptions.add(this.getWebApplicationException( 500 ));
        exceptions.add(this.getWebApplicationException( 400 ));
        ExceptionalFunction<String, String, Throwable> flip = this.getResultFlipper( 10, exceptions );

        WebRetryStrategy strategy = new WebRetryStrategy(  );

        try
        {
            while (strategy.shouldTry())
            {
                try
                {
                    flip.call( "Here goes nothing..." );
                    break;
                }
                catch (WebApplicationException exception)
                {
                    strategy.manageError( exception );
                }
            }
        }
        catch (WebApplicationException wae)
        {
            return;
        }
        catch (Throwable exception)
        {
            Assert.fail( "The process of testing ran into an error." );
        }

        Assert.fail("The web strategy failed to raise the http 400 error");
    }

    @Test(timeout = 2000)
    /*
     * Tests to make sure that the strategy quits if it tries too many times
     */
    public void failOnTooManyAttempts()
    {
        List<Throwable> exceptions = new ArrayList<>();

        exceptions.add(this.getWebApplicationException( 500 ));
        ExceptionalFunction<String, String, Throwable> flip = this.getResultFlipper( 10, exceptions );

        WebRetryStrategy strategy = new WebRetryStrategy( 3 );
        int failCount = 0;

        try
        {
            while (strategy.shouldTry())
            {
                try
                {
                    flip.call( "Here goes nothing..." );
                    break;
                }
                catch (WebApplicationException exception)
                {
                    failCount++;
                    strategy.manageError( exception );
                }
            }
        }
        catch (OutOfAttemptsException ooe)
        {
            Assert.assertEquals( "The strategy should have tried three times before quiting.", 3, failCount );
            return;
        }
        catch (Throwable exception)
        {
            Assert.fail( "An error was thrown before the strategy could run out of attempts." );
        }

        Assert.fail( "The execution should have stopped due to a non-web exception" );
    }

    @Test(timeout = 2000)
    /*
     * Tests to make sure that the strategy retries until it encounters a successful run
     */
    public void executeHandlesSuccess()
    {
        WebRetryStrategy strategy = new WebRetryStrategy(  );
        List<WebApplicationException> exceptionsToThrow = new ArrayList<>();
        exceptionsToThrow.add(this.getWebApplicationException( 500 ));
        ExceptionalFunction<String, String, WebApplicationException> flipper = getResultFlipper( 3, exceptionsToThrow );

        String executeResult = null;

        try
        {
            executeResult = strategy.execute( flipper, "Whatever the string doesn't matter" );
        }
        catch ( OutOfAttemptsException e )
        {
            Assert.fail("The execute function did not respond to a successful call");
        }

        Assert.assertNotNull( "Execute did not return a result", executeResult );
    }

    @Test(timeout = 2000)
    /*
     * Tests to make sure that non-WebApplicationExceptions are not eaten and are thrown
     */
    public void executeStopsOnNonWebException()
    {
        WebRetryStrategy strategy = new WebRetryStrategy(  );
        List<IOException> exceptionsToThrow = new ArrayList<>(  );
        exceptionsToThrow.add(new IOException("This is just a non-web application exception"));
        ExceptionalFunction<String, String, IOException> flipper = getResultFlipper( 3, exceptionsToThrow );

        try
        {
            strategy.execute( flipper, "Whatever" );
        }
        catch ( OutOfAttemptsException e )
        {
            Assert.fail( "The execution ran out of attempts to execute the "
                         + "function rather than stopping on the first error" );
        }
        catch ( Throwable e )
        {
            return;
        }

        Assert.fail( "The execution should have stopped due to a non-web exception" );
    }

    @Test(timeout = 2000)
    /*
     * Tests to make sure that the execution helper on the strategy still fails on http 400 exceptions
     */
    public void executeStopsOn400()
    {
        List<WebApplicationException> exceptions = new ArrayList<>();

        exceptions.add(this.getWebApplicationException( 500 ));
        exceptions.add(this.getWebApplicationException( 400 ));
        ExceptionalFunction<String, String, WebApplicationException> flip = this.getResultFlipper( 10, exceptions );

        WebRetryStrategy strategy = new WebRetryStrategy(  );

        try
        {
            strategy.execute( flip, "Whatever" );
        }
        catch (WebApplicationException wae)
        {
            return;
        }
        catch (Throwable exception)
        {
            Assert.fail( "The process of testing ran into an error." );
        }

        Assert.fail("The web strategy failed to raise the http 400 error");
    }

    @Test(timeout = 2000)
    /*
     * Tests to make sure that the execution helper in the strategy quits if it tries too many times
     */
    public void executeStopsOnTooManyExceptions()
    {
        List<Throwable> exceptions = new ArrayList<>();

        exceptions.add(this.getWebApplicationException( 500 ));
        ExceptionalFunction<String, String, Throwable> flip = this.getResultFlipper( 10, exceptions );

        WebRetryStrategy strategy = new WebRetryStrategy( 3 );
        int failCount = 0;

        try
        {
            while (strategy.shouldTry())
            {
                try
                {
                    flip.call( "Here goes nothing..." );
                }
                catch (WebApplicationException exception)
                {
                    failCount++;
                    strategy.manageError( exception );
                }
            }
        }
        catch (OutOfAttemptsException ooe)
        {
            Assert.assertEquals( "The strategy should have tried three times before quiting.", 3, failCount );
            return;
        }
        catch (Throwable exception)
        {
            Assert.fail( "An error was thrown before the strategy could run out of attempts." );
        }

        Assert.fail("The web strategy failed to throw an error when it ran out of attempts");
    }

    /**
     * Creates a WebApplicationException with the give HTTP status code
     * @param status An HTTP status code
     * @return A WebApplicationException with the given HTTP code
     */
    private WebApplicationException getWebApplicationException( final int status  )
    {
        final Response.Status.Family errorFamily;

        if (status >= 500)
        {
            errorFamily = Response.Status.Family.SERVER_ERROR;
        }
        else if (status >= 400)
        {
            errorFamily = Response.Status.Family.CLIENT_ERROR;
        }
        else
        {
            errorFamily = Response.Status.Family.OTHER;
        }

        Response response = new Response() {
            @Override
            public int getStatus()
            {

                return status;
            }

            @Override
            public StatusType getStatusInfo()
            {
                return new StatusType() {
                    @Override
                    public int getStatusCode()
                    {
                        return status;
                    }

                    @Override
                    public Status.Family getFamily()
                    {
                        return errorFamily;
                    }

                    @Override
                    public String getReasonPhrase()
                    {
                        return "Failed for testing reasons";
                    }
                };
            }

            @Override
            public Object getEntity()
            {
                return null;
            }

            @Override
            public <T> T readEntity( Class<T> aClass )
            {
                return null;
            }

            @Override
            public <T> T readEntity( GenericType<T> genericType )
            {
                return null;
            }

            @Override
            public <T> T readEntity( Class<T> aClass, Annotation[] annotations )
            {
                return null;
            }

            @Override
            public <T> T readEntity( GenericType<T> genericType, Annotation[] annotations )
            {
                return null;
            }

            @Override
            public boolean hasEntity()
            {
                return false;
            }

            @Override
            public boolean bufferEntity()
            {
                return false;
            }

            @Override
            public void close()
            {

            }

            @Override
            public MediaType getMediaType()
            {
                return null;
            }

            @Override
            public Locale getLanguage()
            {
                return null;
            }

            @Override
            public int getLength()
            {
                return 0;
            }

            @Override
            public Set<String> getAllowedMethods()
            {
                return null;
            }

            @Override
            public Map<String, NewCookie> getCookies()
            {
                return null;
            }

            @Override
            public EntityTag getEntityTag()
            {
                return null;
            }

            @Override
            public Date getDate()
            {
                return null;
            }

            @Override
            public Date getLastModified()
            {
                return null;
            }

            @Override
            public URI getLocation()
            {
                return null;
            }

            @Override
            public Set<Link> getLinks()
            {
                return null;
            }

            @Override
            public boolean hasLink( String s )
            {
                return false;
            }

            @Override
            public Link getLink( String s )
            {
                return null;
            }

            @Override
            public Link.Builder getLinkBuilder( String s )
            {
                return null;
            }

            @Override
            public MultivaluedMap<String, Object> getMetadata()
            {
                return null;
            }

            @Override
            public MultivaluedMap<String, String> getStringHeaders()
            {
                return null;
            }

            @Override
            public String getHeaderString( String s )
            {
                return null;
            }
        };
        return new WebApplicationException( response );
    }


    /**
     * Creates a function the will throw a certain number of exceptions prior to succeeding
     * @param failCount The number of times to throw an exception
     * @param exceptionsToThrow A collection of errors that may be thrown
     * @param <E> Some type of exception that the function will throw
     * @return A function that will throw a certain number of exceptions before successfully completing
     */
    private <E extends Throwable> ExceptionalFunction<String, String, E> getResultFlipper( final int failCount, final List<E> exceptionsToThrow )
    {
        Objects.requireNonNull(exceptionsToThrow, "Exceptions to throw must be passed in order to get a result flipper function.");
        if (failCount < 1)
        {
            throw new InvalidParameterException( "The result flipper must be allowed to flip at least once" );
        }
        if (exceptionsToThrow.size() == 0)
        {
            throw new InvalidParameterException( "At least one exception should be thrown by the result flipper" );
        }

        return new ExceptionalFunction<String, String, E>()
        {
            private final List<E> exceptions = exceptionsToThrow;
            private final int failLimit = failCount;
            private int soFar = 0;


            @Override
            public String call( String s ) throws E
            {
                if (this.soFar >= failLimit)
                {
                    return "This was a successful call.";
                }

                int exceptionIndex = this.soFar % this.exceptions.size();
                this.soFar++;
                throw this.exceptions.get(exceptionIndex);
            }
        };
    }
}
