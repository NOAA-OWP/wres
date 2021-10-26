package wres.io.retrieval;

import java.util.List;
import java.util.Locale;
import java.util.function.DoubleUnaryOperator;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.apache.commons.math3.util.Precision.EPSILON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import wres.config.ProjectConfigException;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.UnitAlias;
import wres.io.data.caching.MeasurementUnits;

/**
 * The UnitMapper doesn't need a database (db) to test conversions. See also
 * the original/companion class UnitMapperTest for tests of db integration.
 * See also the tests in wres.datamodel.UnitsTest
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
        String toUnit = "[ft_i]/min";
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
        String fromUnit = "1000.[ft_i]3/s";
        String toUnit = "m3/s";
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache, toUnit );
        DoubleUnaryOperator converter = mapper.getUnitMapper( fromUnit );
        assertEquals( 73425.583, converter.applyAsDouble( 2593.0 ), 0.001 );
    }


    /**
     * Going from dimensions x/y to z should fail: NoSuchUnitConversionException
     */

    @Test
    public void convertFromDistancePerTimeToLuminousFluxFails()
    {
        // Light years (distance dimension) per hour (time dimension)
        String fromUnit = "[ly]/h";

        // Lumen (luminous flux dimension)
        String toUnit = "lm";
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache, toUnit );
        assertThrows( NoSuchUnitConversionException.class,
                      () -> mapper.getUnitMapper( fromUnit ) );
    }


    /**
     * Alias "boogaflickle" to unit "m/s" should be respected as "from" unit.
     */

    @Test
    public void oneAliasWorksAsSourceUnit()
    {
        final String BOOGAFLICKLE = "boogaflickle";
        String fromUnit = BOOGAFLICKLE;
        String toUnit = "[ft_i]/min";

        // Here we declare that boogaflickle should be interpreted as "m/s"
        UnitAlias boogaflickleMeansMetersPerSecond = new UnitAlias( BOOGAFLICKLE, "m/s" );
        ProjectConfig declaration = this.getProjectDeclarationWith( List.of( boogaflickleMeansMetersPerSecond ) );
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache,
                                           toUnit,
                                           declaration );
        DoubleUnaryOperator converter = mapper.getUnitMapper( fromUnit );
        assertEquals( 510039.370, converter.applyAsDouble( 2591.0 ), 0.001 );
    }

    /**
     * Alias "boogaflickle" to unit "ft/min" should be respected as "to" unit.
     */

    @Test
    public void oneAliasWorksAsTargetUnit()
    {
        final String BOOGAFLICKLE = "boogaflickle";
        String fromUnit = "m/s";
        String toUnit = BOOGAFLICKLE;

        // Here declare that boogaflickle should be interpreted as "[ft_i]/min"
        UnitAlias boogaflickleMeansFeetPerMinute = new UnitAlias( BOOGAFLICKLE, "[ft_i]/min" );
        ProjectConfig declaration = this.getProjectDeclarationWith( List.of( boogaflickleMeansFeetPerMinute ) );
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache,
                                           toUnit,
                                           declaration );
        DoubleUnaryOperator converter = mapper.getUnitMapper( fromUnit );
        assertEquals( 510039.370, converter.applyAsDouble( 2591.0 ), 0.001 );
    }


    /**
     * Alias "C" to unit "C" should override internal celsius map entry when
     * converting from "C".
     */

    @Test
    public void aliasCoulombMeansCoulombCannotConvertToFahrenheit()
    {
        String fromUnit = "C";
        String toUnit = "F";

        // Here we declare that "C" should be interpreted as "C" or Coulomb
        // rather than the WRES convenience alias for Celsius
        UnitAlias coulombMeansCoulomb = new UnitAlias( "C", "C" );
        ProjectConfig declaration = this.getProjectDeclarationWith( List.of( coulombMeansCoulomb ) );
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache,
                                           toUnit,
                                           declaration );
        assertThrows( NoSuchUnitConversionException.class,
                      () -> mapper.getUnitMapper( fromUnit ) );
    }


    /**
     * Alias "F" to unit "F" should override internal celsius map entry when
     * converting to "F".
     */

    @Test
    public void aliasFaradMeansFaradCannotConvertFromCelsius()
    {
        String fromUnit = "C";
        String toUnit = "F";

        // Here we declare that "F" should be interpreted as "F" or Farad
        // rather than the WRES convenience alias for Fahrenheit.
        UnitAlias faradMeansFarad = new UnitAlias( "F", "F" );
        ProjectConfig declaration = this.getProjectDeclarationWith( List.of( faradMeansFarad ) );
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache,
                                           toUnit,
                                           declaration );
        assertThrows( NoSuchUnitConversionException.class,
                      () -> mapper.getUnitMapper( fromUnit ) );
    }


    /**
     * Supplying duplicates of one alias should fail with ProjectConfigException
     */

    @Test
    public void duplicatesOfAnAliasFailsWhenConstructingMapper()
    {
        String toUnit = "s";
        UnitAlias aliasOne = new UnitAlias( "F", "far out" );
        UnitAlias aliasTwo = new UnitAlias( "F", "nearby" );
        ProjectConfig declaration = this.getProjectDeclarationWith( List.of( aliasOne, aliasTwo ) );
        assertThrows( ProjectConfigException.class,
                      () -> UnitMapper.of( this.measurementUnitsCache,
                                           toUnit,
                                           declaration ) );
    }


    /**
     * Supplying more than one alias works when converting to and from each.
     */

    @Test
    public void multipleAliasesWork()
    {
        final String FUNKY_MINUTES = "MiNuTeS oOoOo";
        final String FUNKY_SECONDS = "sEcOnDs WhOa";
        String fromUnit = FUNKY_MINUTES;
        String toUnit = FUNKY_SECONDS;

        UnitAlias aliasOne = new UnitAlias( FUNKY_MINUTES, "min" );
        UnitAlias aliasTwo = new UnitAlias( FUNKY_SECONDS, "s" );
        ProjectConfig declaration = this.getProjectDeclarationWith( List.of( aliasOne, aliasTwo ) );
        UnitMapper mapper = UnitMapper.of( this.measurementUnitsCache,
                                           toUnit,
                                           declaration );
        DoubleUnaryOperator converter = mapper.getUnitMapper( fromUnit );
        assertEquals( 156540.0, converter.applyAsDouble( 2609.0 ), EPSILON );
    }

    private ProjectConfig getProjectDeclarationWith( List<UnitAlias> aliases )
    {
        return new ProjectConfig( null,
                                  new PairConfig( null,
                                                  aliases,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  null ),
                                  null,
                                  null,
                                  null,
                                  null );
    }
}
