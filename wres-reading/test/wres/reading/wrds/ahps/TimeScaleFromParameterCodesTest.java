package wres.reading.wrds.ahps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.time.Duration;

import org.junit.Test;

import wres.datamodel.scale.TimeScaleOuter;

import wres.reading.wrds.ahps.ParameterCodes;
import wres.reading.wrds.ahps.TimeScaleFromParameterCodes;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * Tests the {@link TimeScaleFromParameterCodes}.
 * 
 * @author James Brown
 */
public final class TimeScaleFromParameterCodesTest
{

    @Test
    public void testGetTimeScaleIsInstantaneous()
    {
        ParameterCodes codes = new ParameterCodes();
        codes.setDuration( "I" );

        // Instantaneous parameter code, case insensitive        
        assertEquals( TimeScaleOuter.of(), TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "i" );

        assertEquals( TimeScaleOuter.of(), TimeScaleFromParameterCodes.getTimeScale( codes, null ) );
    }

    @Test
    public void testGetTimeScaleIsNotInstantaneous()
    {
        ParameterCodes codes = new ParameterCodes();
        codes.setPhysicalElement( "QR" );

        TimeScaleFunction expectedFunction = TimeScaleFunction.MEAN;

        codes.setDuration( "U" );
        assertEquals( TimeScaleOuter.of( Duration.ofMinutes( 1 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "E" );
        assertEquals( TimeScaleOuter.of( Duration.ofMinutes( 5 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "G" );
        assertEquals( TimeScaleOuter.of( Duration.ofMinutes( 10 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "C" );
        assertEquals( TimeScaleOuter.of( Duration.ofMinutes( 15 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "J" );
        assertEquals( TimeScaleOuter.of( Duration.ofMinutes( 30 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "H" );
        assertEquals( TimeScaleOuter.of( Duration.ofHours( 1 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "B" );
        assertEquals( TimeScaleOuter.of( Duration.ofHours( 2 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "T" );
        assertEquals( TimeScaleOuter.of( Duration.ofHours( 3 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "F" );
        assertEquals( TimeScaleOuter.of( Duration.ofHours( 4 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "Q" );
        assertEquals( TimeScaleOuter.of( Duration.ofHours( 6 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "A" );
        assertEquals( TimeScaleOuter.of( Duration.ofHours( 8 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "K" );
        assertEquals( TimeScaleOuter.of( Duration.ofHours( 12 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "L" );
        assertEquals( TimeScaleOuter.of( Duration.ofHours( 18 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

        codes.setDuration( "D" );
        assertEquals( TimeScaleOuter.of( Duration.ofDays( 1 ), expectedFunction ),
                      TimeScaleFromParameterCodes.getTimeScale( codes, null ) );

    }

    @Test
    public void testGetTimeScaleThrowsExpectedExceptionWhenPhysicalElementIsUnexpected()
    {
        ParameterCodes codes = new ParameterCodes();
        codes.setPhysicalElement( "this_will_never_be_a_physical_element" );

        assertThrows( UnsupportedOperationException.class,
                      () -> TimeScaleFromParameterCodes.getTimeScale( codes, null ) );
    }

    @Test
    public void testGetTimeScaleThrowsExpectedExceptionWhenDurationIsUnexpected()
    {
        ParameterCodes codes = new ParameterCodes();
        codes.setPhysicalElement( "QR" );
        codes.setDuration( "this_will_never_be_a_time_scale" );

        assertThrows( UnsupportedOperationException.class,
                      () -> TimeScaleFromParameterCodes.getTimeScale( codes, null ) );
    }

    @Test
    public void testGetTimeScaleThrowsExceptionWithNullInput()
    {
        NullPointerException expectedException = assertThrows( NullPointerException.class,
                                                               () -> TimeScaleFromParameterCodes.getTimeScale( null,
                                                                                                               null ) );

        assertEquals( "Specify non-null parameter codes alongside the WRDS source 'null'.",
                      expectedException.getMessage() );
    }

}
