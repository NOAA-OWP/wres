package wres.datamodel.sampledata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.junit.Test;

/**
 * Tests the {@link SampleDataException}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class SampleDataExceptionTest
{

    private static final String TEST_EXCEPTION_MESSAGE = "Test exception.";

    /**
     * Constructs and tests a {@link SampleDataException}.
     */

    @Test
    public void testException()
    {       
        SampleDataException e = new SampleDataException();
        
        assertNotNull( new SampleDataException() );
        
        SampleDataException f = new SampleDataException( TEST_EXCEPTION_MESSAGE );
        
        assertNotNull( f );
        
        assertEquals( TEST_EXCEPTION_MESSAGE, f.getMessage() );
        
        SampleDataException g = new SampleDataException( f.getMessage(), e );        
        
        assertNotNull( g );
        
        assertEquals( TEST_EXCEPTION_MESSAGE, g.getMessage() );

        assertEquals( e, g.getCause() );
    }
}
