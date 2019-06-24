package wres.io.reading.wrds;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import wres.datamodel.scale.TimeScale;

/**
 * Tests the {@link TimeScaleFromParameterCodes}.
 * 
 * @author james.brown@hydrosolved.com
 */
public final class TimeScaleFromParameterCodesTest
{

    @Rule
    public final ExpectedException exception = ExpectedException.none();

    /**
     * Confirms that the {@link TimeScaleFromParameterCodes#getTimeScale(String)} returns 
     * an expected result.
     */

    @Test
    public void testGetTimeScale()
    {
        ParameterCodes codes = new ParameterCodes();
        codes.setDuration( "I" );

        // Instantaneous parameter code, case insensitive        
        assertEquals( TimeScaleFromParameterCodes.getTimeScale( codes, null ), TimeScale.of() );

        codes.setDuration( "i" );

        assertEquals( TimeScaleFromParameterCodes.getTimeScale( codes, null ), TimeScale.of() );
    }

    /**
     * Confirms that the {@link TimeScaleFromParameterCodes#getTimeScale(String)} returns 
     * a null result with input that cannot be translated.
     */

    @Test
    public void testGetTimeScaleReturnsNull()
    {
        ParameterCodes codes = new ParameterCodes();
        codes.setDuration( "this_will_never_be_a_time_scale" );
        
        // Instantaneous parameter code, case insensitive        
        assertNull( TimeScaleFromParameterCodes.getTimeScale( codes, null ) );
    }    
    
    /**
     * Tests that the {@link TimeScaleFromParameterCodes#getTimeScale(String)} throws 
     * an expected exception when the input is null.
     */
    @Test
    public void testGetTimeScaleThrowsExceptionWithNullInput()
    {
        exception.expect( NullPointerException.class );
        exception.expectMessage( "Specify non-null parameter codes alongside the WRDS source 'null'" );

        TimeScaleFromParameterCodes.getTimeScale( null, null );
    }

}
