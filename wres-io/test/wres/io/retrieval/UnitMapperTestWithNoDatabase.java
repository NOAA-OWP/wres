package wres.io.retrieval;

import java.util.Locale;
import java.util.function.DoubleUnaryOperator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.apache.commons.math3.util.Precision.EPSILON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import wres.io.data.caching.MeasurementUnits;

/**
 * The UnitMapper doesn't need a database (db) to test conversions. See also
 * the original/companion class UnitMapperTest for tests of db integration.
 */

public class UnitMapperTestWithNoDatabase
{
    @Mock private MeasurementUnits measurementUnitsCache;

    @BeforeEach
    public void setup()
    {
        MockitoAnnotations.openMocks( this );
    }


    @Test
    public void testIdentityConversionOfDashUnit()
    {
        String dashUnit = "-";
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache, dashUnit );
        DoubleUnaryOperator namedConverter = mapper.getUnitMapper( dashUnit );
        assertEquals( 2579.0, namedConverter.applyAsDouble( 2579.0 ), EPSILON );
    }


    @Test
    public void testIdentityConversionOfNoneUnit()
    {
        String dummyNoneUnit = "NONE";
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache, dummyNoneUnit );
        DoubleUnaryOperator converter = mapper.getUnitMapper( dummyNoneUnit );
        assertEquals( 2591.0, converter.applyAsDouble( 2591.0 ), EPSILON );
    }


    @Test
    public void caseSensitiveUnequalFakeUnitsFailToConvert()
    {
        String dummyUnitUpperCase = "BOOGAFLICKLE";
        String dummyUnitLowerCase = dummyUnitUpperCase.toLowerCase( Locale.US );
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache, dummyUnitUpperCase );
        assertThrows( RuntimeException.class,
                      () -> mapper.getUnitMapper( dummyUnitLowerCase ) );
    }


    @Test
    public void convertFromCelsiusToFahrenheit()
    {
        // Assumes a default alias map of "C" to degrees celsius, likewise for F
        String fromUnit = "C";
        String toUnit = "F";
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache, toUnit );
        DoubleUnaryOperator converter = mapper.getUnitMapper( fromUnit );
        assertEquals( 62.6, converter.applyAsDouble( 17.0 ), EPSILON );
    }


    @Test
    public void convertFromFahrenheitToCelsius()
    {
        // Assumes a default alias map of "C" to degrees celsius, likewise for F
        String fromUnit = "F";
        String toUnit = "C";
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache, toUnit );
        DoubleUnaryOperator converter = mapper.getUnitMapper( fromUnit );
        assertEquals( -30.5555555555555555, converter.applyAsDouble( -23.0 ), EPSILON );
    }

    /**
     * This test ensures that SI units and US Customary units work.
     */

    @Test
    public void convertFromMetersPerSecondToFeetPerMinute()
    {
        String fromUnit = "m/s";
        String toUnit = "ft/min";
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache, toUnit );
        DoubleUnaryOperator converter = mapper.getUnitMapper( fromUnit );
        assertEquals( 510039.370, converter.applyAsDouble( 2591.0 ), 0.001 );
    }


    /**
     * A common-enough conversion with an odd representation in indriya.
     */
    @Test
    public void convertFromThousandsOfCubicFeetPerSecondToCubicMetersPerSecond()
    {
        String fromUnit = "ft^3*1000/s";
        String toUnit = "m^3/s";
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache, toUnit );
        DoubleUnaryOperator converter = mapper.getUnitMapper( fromUnit );
        assertEquals( 73425.583, converter.applyAsDouble( 2593.0 ), 0.001 );
    }
}
