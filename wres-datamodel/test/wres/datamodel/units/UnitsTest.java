package wres.datamodel.units;

import java.time.Duration;
import java.util.function.UnaryOperator;

import javax.measure.Quantity;
import javax.measure.Unit;
import javax.measure.UnitConverter;
import javax.measure.format.UnitFormat;
import javax.measure.quantity.Time;
import javax.measure.quantity.Volume;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import si.uom.quantity.VolumetricFlowRate;
import systems.uom.ucum.format.UCUMFormat;
import tech.units.indriya.quantity.Quantities;
import tech.units.indriya.unit.ProductUnit;

import wres.datamodel.scale.TimeScaleOuter;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static systems.uom.ucum.UCUM.FOOT_INTERNATIONAL;
import static systems.uom.ucum.UCUM.GALLON_US;
import static systems.uom.ucum.format.UCUMFormat.Variant.CASE_SENSITIVE;
import static tech.units.indriya.unit.Units.CUBIC_METRE;
import static tech.units.indriya.unit.Units.DAY;
import static tech.units.indriya.unit.Units.SECOND;

/**
 * Tests {@link Units}.
 */

class UnitsTest
{
    private static final String M3 = "m3";
    private static final String KG = "kg";
    private static final String KG_H = "kg/h";
    private static final String KM = "km";
    private static final String KM_H = "km/h";
    private static final String CFS = "CFS";
    private static final String CMS = "CMS";
    static final Unit<VolumetricFlowRate> CUBIC_METRE_PER_SECOND =
            new ProductUnit<>( CUBIC_METRE.divide( SECOND ) );
    static final Unit<VolumetricFlowRate> CUBIC_FOOT_PER_SECOND =
            new ProductUnit<>( FOOT_INTERNATIONAL.pow( 3 ).divide( SECOND ) );
    private static final Logger LOGGER =
            LoggerFactory.getLogger( UnitsTest.class );

    @Test
    void testConvertFlow()
    {
        double someCubicMetersPerSecondNoUnits = 947.0;
        Quantity<VolumetricFlowRate> someCubicMetersPerSecond =
                Quantities.getQuantity( someCubicMetersPerSecondNoUnits,
                                        CUBIC_METRE_PER_SECOND );
        LOGGER.debug( "Quantity of CMS flow: {}", someCubicMetersPerSecond );

        UnitConverter converter = someCubicMetersPerSecond.getUnit()
                                                          .getConverterTo( CUBIC_FOOT_PER_SECOND );
        double someCubicFeetPerSecondNoUnits = converter.convert( someCubicMetersPerSecond.getValue() )
                                                        .doubleValue();

        LOGGER.debug( "Converted-to-CFS raw value: {}",
                      someCubicFeetPerSecondNoUnits );
        assertTrue( someCubicFeetPerSecondNoUnits > someCubicMetersPerSecondNoUnits,
                    "Expected " + someCubicFeetPerSecondNoUnits
                    + " to be greater than "
                    + someCubicMetersPerSecondNoUnits );
    }

    @Test
    void getCfsUnit()
    {
        Unit<?> cfs = Units.getUnit( CFS );
        assertNotNull( cfs );
    }

    @Test
    void getCmsUnit()
    {
        Unit<?> cfs = Units.getUnit( CMS );
        assertNotNull( cfs );
    }

    @Test
    void getPairsPerParsecUnitThrowsException()
    {
        assertThrows( Units.UnrecognizedUnitException.class,
                      () -> Units.getUnit( "pears/parsec" ) );
    }

    /**
     * Verifies that the values in the convenience unit map (not the keys) can
     * be parsed directly by the UCUM parser. This does not guarantee that these
     * mean what we or the callers intend them to mean, because that is handled
     * by explicit logging of the unit found and the use of unit aliases when
     * the caller disagrees with the convenience aliases for a given evaluation.
     */

    @Test
    void testAllConvenienceUnitsCanBeParsed()
    {
        UnitFormat unitFormat = UCUMFormat.getInstance( CASE_SENSITIVE );

        for ( String stringToParse : Units.getAllConvenienceUnits() )
        {
            LOGGER.debug( "About to parse unit from string {}", stringToParse );
            Unit<?> unit = unitFormat.parse( stringToParse );
            LOGGER.debug( "Successfully parsed unit {} from string {}",
                          unit,
                          stringToParse );
            assertNotNull( unit );
        }
    }

    @Test
    void testIsConvertingFromVolumetricFlowToVolume()
    {
        Unit<?> flowUnit = Units.getUnit( CMS );
        Unit<?> volumeUnit = Units.getUnit( M3 );

        assertTrue( Units.isSupportedTimeIntegralConversion( flowUnit, volumeUnit ) );
    }

    @Test
    void testIsConvertingFromSpeedToDistance()
    {
        Unit<?> speedUnit = Units.getUnit( KM_H );
        Unit<?> distanceUnit = Units.getUnit( KM );

        assertTrue( Units.isSupportedTimeIntegralConversion( speedUnit, distanceUnit ) );
    }

    @Test
    void testIsConvertingFromMassFlowToMass()
    {
        Unit<?> speedUnit = Units.getUnit( KG_H );
        Unit<?> distanceUnit = Units.getUnit( KG );

        assertTrue( Units.isSupportedTimeIntegralConversion( speedUnit, distanceUnit ) );
    }

    @Test
    void testGetVolumetricFlowToVolumeConverter()
    {
        Unit<?> flowUnit = Units.getUnit( CMS );
        Unit<?> volumeUnit = Units.getUnit( M3 );

        Duration sixHours = Duration.ofHours( 6 );
        TimeScaleOuter timeScale = TimeScaleOuter.of( sixHours, TimeScaleFunction.MEAN );
        UnaryOperator<Double> converter = Units.getTimeIntegralConverter( timeScale, flowUnit, volumeUnit );

        assertEquals( 21_600_000.0, converter.apply( 1000.0 ) );
    }

    @Test
    void testGetSpeedToDistanceConverter()
    {
        Unit<?> speedUnit = Units.getUnit( KM_H );
        Unit<?> distanceUnit = Units.getUnit( KM );

        Duration sixHours = Duration.ofHours( 6 );
        TimeScaleOuter timeScale = TimeScaleOuter.of( sixHours, TimeScaleFunction.MEAN );
        UnaryOperator<Double> converter = Units.getTimeIntegralConverter( timeScale, speedUnit, distanceUnit );

        assertEquals( 60.0, converter.apply( 10.0 ) );
    }

    @Test
    void testGetMassFlowToMassConverter()
    {
        Unit<?> massFlowUnit = Units.getUnit( KG_H );
        Unit<?> massUnit = Units.getUnit( KG );

        Duration sixHours = Duration.ofHours( 6 );
        TimeScaleOuter timeScale = TimeScaleOuter.of( sixHours, TimeScaleFunction.MEAN );
        UnaryOperator<Double> converter = Units.getTimeIntegralConverter( timeScale, massFlowUnit, massUnit );

        assertEquals( 72.0, converter.apply( 12.0 ) );
    }

    @Test
    void testConvertFlowToVolumeExploratory()
    {
        double someCubicMetersPerSecondNoUnits = 953.0;
        Quantity<VolumetricFlowRate> someCubicMetersPerSecond =
                Quantities.getQuantity( someCubicMetersPerSecondNoUnits,
                                        CUBIC_METRE_PER_SECOND );
        LOGGER.debug( "Quantity of CMS flow: {}", someCubicMetersPerSecond );
        Quantity<Time> someDays = Quantities.getQuantity( 3, DAY );
        Quantity<?> someVolume = someCubicMetersPerSecond.multiply( someDays );
        LOGGER.debug( "Converted-to-volume: {}", someVolume );
        LOGGER.debug( "Converted-to-volume dimension: {}",
                      someVolume.getUnit()
                                .getDimension() );
        LOGGER.debug( "Converted-to-volume as system unit: {}",
                      someVolume.toSystemUnit() );
        @SuppressWarnings( "unchecked" )
        UnitConverter unitConverter =
                ( ( Unit<Volume> ) someVolume.getUnit() )
                        .getConverterTo( GALLON_US );
        double someGallonsDouble = unitConverter.convert( someVolume.getValue()
                                                                    .doubleValue() );

        assertEquals( 6.525514636058416E10, someGallonsDouble, 5 );

        LOGGER.debug( "Converted to gallons (double): {}", someGallonsDouble );
        Number someGallonsNumber =
                unitConverter.convert( someVolume.getValue() );
        LOGGER.debug( "Converted to gallons (Number): {}", someGallonsNumber );
        LOGGER.debug( "Class of the Number: {}", someGallonsNumber.getClass() );

        assertEquals( 6.525514636058416E10, someGallonsNumber.doubleValue(), 5 );

        // More straightforward than using a converter:
        @SuppressWarnings( "unchecked" )
        Quantity<Volume> gallons =
                ( ( Quantity<Volume> ) someVolume ).to( GALLON_US );
        LOGGER.debug( "Converted to gallons again: {}", gallons );

        assertEquals( 6.525514636058416E10, gallons.getValue()
                                                   .doubleValue(), 5 );

        // What about cubic meters?
        @SuppressWarnings( "unchecked" )
        Quantity<?> cubicMeters =
                ( ( Quantity<Volume> ) someVolume ).to( CUBIC_METRE );

        assertEquals( "247017600 m³", cubicMeters.toString() );

        LOGGER.debug( "Converted to cubicMeters: {}", cubicMeters );
    }
}
