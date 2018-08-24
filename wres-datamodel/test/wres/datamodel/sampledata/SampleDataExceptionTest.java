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

    /**
     * Constructs and tests a {@link SampleDataException}.
     */

    @Test
    public void testException()
    {       
        SampleDataException e = new SampleDataException();
        
        assertNotNull( new SampleDataException() );
        
        SampleDataException f = new SampleDataException( "Test exception." );
        
        assertNotNull( f );
        
        assertEquals( f.getMessage(), "Test exception." );
        
        SampleDataException g = new SampleDataException( f.getMessage(), e );        
        
        assertNotNull( g );
        
        assertEquals( g.getMessage(), "Test exception." );

        assertEquals( e, g.getCause() );
    }
}
