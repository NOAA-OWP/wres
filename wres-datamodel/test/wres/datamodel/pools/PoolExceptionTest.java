package wres.datamodel.pools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

/**
 * Tests the {@link PoolException}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class PoolExceptionTest
{

    private static final String TEST_EXCEPTION_MESSAGE = "Test exception.";

    /**
     * Constructs and tests a {@link PoolException}.
     */

    @Test
    public void testException()
    {       
        PoolException e = new PoolException();
        
        assertNotNull( new PoolException() );
        
        PoolException f = new PoolException( TEST_EXCEPTION_MESSAGE );
        
        assertNotNull( f );
        
        assertEquals( TEST_EXCEPTION_MESSAGE, f.getMessage() );
        
        PoolException g = new PoolException( f.getMessage(), e );        
        
        assertNotNull( g );
        
        assertEquals( TEST_EXCEPTION_MESSAGE, g.getMessage() );

        assertEquals( e, g.getCause() );
    }
}
