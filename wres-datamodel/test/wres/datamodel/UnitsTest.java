package wres.datamodel;

import java.nio.charset.StandardCharsets;
import java.util.List;
import javax.measure.Quantity;
import javax.measure.Unit;
import javax.measure.UnitConverter;
import javax.measure.format.MeasurementParseException;
import javax.measure.format.UnitFormat;
import javax.measure.quantity.Time;
import javax.measure.quantity.Volume;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import si.uom.quantity.VolumetricFlowRate;
import systems.uom.ucum.format.UCUMFormat;
import systems.uom.ucum.internal.format.TokenException;

import systems.uom.ucum.internal.format.TokenMgrError;
import tech.units.indriya.AbstractUnit;
import tech.units.indriya.quantity.Quantities;
import tech.units.indriya.unit.ProductUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static systems.uom.ucum.UCUM.CUBIC_FOOT_INTERNATIONAL;
import static systems.uom.ucum.UCUM.GALLON_US;
import static systems.uom.ucum.format.UCUMFormat.Variant.CASE_SENSITIVE;
import static tech.units.indriya.unit.Units.CUBIC_METRE;
import static tech.units.indriya.unit.Units.DAY;
import static tech.units.indriya.unit.Units.SECOND;

/**
 * The first test and the "explore" tests are exploratory. The remainder are
 * critical tests to verify unit parsing and unit conversion functionality.
 * See more related tests in wres.io.retrieval.UnitMapperTest as well as in
 * wres.io.retrieval.UnitMapperTestWithNoDatabase
 */

public class UnitsTest
{
    static final Unit<VolumetricFlowRate> CUBIC_METRE_PER_SECOND =
            new ProductUnit<>( CUBIC_METRE.divide( SECOND ) );
    static final Unit<VolumetricFlowRate> CUBIC_FOOT_PER_SECOND =
            new ProductUnit<>( CUBIC_FOOT_INTERNATIONAL.divide( SECOND ) );
    private static final Logger LOGGER =
            LoggerFactory.getLogger( UnitsTest.class );

    private static final List<String> UNITS_TO_PARSE =
            List.of( "g/l", "m", "m/s", "metre per second", "square foot",
                     "gallon", "gal", "sec", "m3", "litre", "kilograms", "kg",
                     "metre", "metre/second", "second", "m³", "m³/s", "CFS",
                     "CMS", "cubic metre per second", "m^2", "m^3/s", "meter",
                     "feet", "foot", "inches", "inch", "ft", "in", "FT", "IN",
                     "cms", "cfs", "m3 s-1", "m3/s", "ft^3/s", "kilo ft^3/s",
                     "cubic foot / second", "Foot^3/Second", "kft", "KFoot",
                     "Foot", "ft^3*1000/s", "K", "degc", "C", "F", "c", "f",
                     "°C", "°F", "MM", "CM", "mm", "cm", "celsius",
                     "fahrenheit", "\u2103", "\u2109", "\u00b0F", "GALLON",
                     "gi.us", "METRE", "fm", "fth_us", "pt_br", "DEGF",
                     "gal/min", "ac-ft", "ac", "ac ft", "gal*1000000/d",
                     "mm s^-1", "kg/m^2/h", "1000*ft^3/s",
                     "|0|",
                     "℃",
                     "|1|",
                     new String( "℃".getBytes(),
                                 StandardCharsets.UTF_8 ),
                     "|2|",
                     new String( "℃".getBytes( StandardCharsets.UTF_8 ),
                                 StandardCharsets.UTF_8 ),
                     "|3|",
                     new String( "℃".getBytes( StandardCharsets.UTF_16 ),
                                 StandardCharsets.UTF_8 ),
                     "|4|",
                     "cfs_i", "[cfs_i]", "[cfs_i]/s", "degF", "[degF]", "Cel",
                     "K/[ft_i]", "ft3/s", "kft3/s", "k(ft3)/s", "k[ft3]/s",
                     "k[cft_i]/s", "m3/s", "ft_i", "ft_i3", "kft_i3", "[ft_i]",
                     "[ft_i]3", "k[ft_i]", "k[ft_i]3/s", "cft_i", "[cft_i]",
                     "[cft_i]/s", "k[cft_i]/s", "m[gal_us]/d" );

    @Test
    public void testConvertFlow()
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
        assertTrue(
                someCubicFeetPerSecondNoUnits > someCubicMetersPerSecondNoUnits,
                "Expected " + someCubicFeetPerSecondNoUnits
                + " to be greater than " + someCubicMetersPerSecondNoUnits );
    }


    @Test
    public void exploreConvertFlowToVolume()
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
        UnitConverter unitConverter =
                ( ( Unit<Volume> ) someVolume.getUnit() ).getConverterTo(
                        GALLON_US );
        double someGallonsDouble = unitConverter.convert( someVolume.getValue()
                                                                    .doubleValue() );
        LOGGER.debug( "Converted to gallons (double): {}", someGallonsDouble );
        Number someGallonsNumber =
                unitConverter.convert( someVolume.getValue() );
        LOGGER.debug( "Converted to gallons (Number): {}", someGallonsNumber );
        LOGGER.debug( "Class of the Number: {}", someGallonsNumber.getClass() );

        // More straightforward than using a converter:
        Quantity<Volume> gallons =
                ( ( Quantity<Volume> ) someVolume ).to( GALLON_US );
        LOGGER.debug( "Converted to gallons again: {}", gallons );

        // What about cubic meters?
        Quantity<?> cubicMeters =
                ( ( Quantity<Volume> ) someVolume ).to( CUBIC_METRE );
        LOGGER.debug( "Converted to cubicMeters: {}", cubicMeters );
    }


    @Test
    public void exploreUnitNamesAndSymbols()
    {
        LOGGER.debug( "Name of tech.units.indriya.unit.Units.METRE: {}",
                      tech.units.indriya.unit.Units.METRE.getName() );
        LOGGER.debug( "Symbol of tech.units.indriya.unit.Units.METRE: {}",
                      tech.units.indriya.unit.Units.METRE.getSymbol() );
        LOGGER.debug( "Name of tech.units.indriya.unit.Units.CUBIC_METRE: {}",
                      tech.units.indriya.unit.Units.CUBIC_METRE.getName() );
        LOGGER.debug( "Symbol of tech.units.indriya.unit.Units.CUBIC_METRE: {}",
                      tech.units.indriya.unit.Units.CUBIC_METRE.getSymbol() );
        LOGGER.debug( "Name of si.uom.SI.CUBIC_METRE: {}",
                      si.uom.SI.CUBIC_METRE.getName() );
        LOGGER.debug( "Symbol of si.uom.SI.CUBIC_METRE: {}",
                      si.uom.SI.CUBIC_METRE.getSymbol() );
        LOGGER.debug( "Name of CUBIC_METRE_PER_SECOND: {}",
                      CUBIC_METRE_PER_SECOND.getName() );
        LOGGER.debug( "Symbol of CUBIC_METRE_PER_SECOND: {}",
                      CUBIC_METRE_PER_SECOND.getSymbol() );
    }

    @Test
    public void exploreIndriyaUnitParsing()
    {
        // When using USCustomary instead of UCUM, uncomment, import this:
        //Unit<Length> foot = FOOT;

        for ( String stringToParse : UNITS_TO_PARSE )
        {
            try
            {
                Unit<?> unit = AbstractUnit.parse( stringToParse );
                LOGGER.debug(
                        "Indriya parsed '{}' into '{}', name='{}', symbol='{}', dimension='{}'",
                        stringToParse,
                        unit,
                        unit.getName(),
                        unit.getSymbol(),
                        unit.getDimension() );
                System.out.println( "Indriya parsed '" + stringToParse
                                    + "' into '" + unit + "' name='" +
                                    unit.getName() + "'" );
            }
            catch ( MeasurementParseException | TokenMgrError mpe )
            {
                LOGGER.debug( "Exception while parsing '{}': {}",
                              stringToParse, mpe.getMessage() );
            }
        }
    }

    @Test
    public void exploreUCUMUnitParsing()
    {
        UnitFormat unitFormat = UCUMFormat.getInstance( CASE_SENSITIVE );

        for ( String stringToParse : UNITS_TO_PARSE )
        {
            try
            {
                // Apparently identical to the implementation gotten from SPI.
                Unit<?> unit = unitFormat.parse( stringToParse );
                LOGGER.debug(
                        "Indriya/UCUM parsed '{}' into '{}', name='{}', symbol='{}', dimension='{}'",
                        stringToParse,
                        unit,
                        unit.getName(),
                        unit.getSymbol(),
                        unit.getDimension() );
            }
            catch ( MeasurementParseException | TokenMgrError | TokenException | IllegalArgumentException e )
            {
                LOGGER.debug( "Exception while parsing '{}': {}",
                              stringToParse, e.getMessage() );
            }
        }
    }

    @Test
    public void getCfsUnit()
    {
        Unit<?> cfs = Units.getUnit( "CFS" );
        assertNotNull( cfs );
    }

    @Test
    public void getCmsUnit()
    {
        Unit<?> cfs = Units.getUnit( "CMS" );
        assertNotNull( cfs );
    }

    @Test
    public void getPairsPerParsecUnitThrowsException()
    {
        assertThrows( Units.UnsupportedUnitException.class,
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
    public void verifyAllConvenienceUnitsCanBeParsed()
    {
        UnitFormat unitFormat = UCUMFormat.getInstance( CASE_SENSITIVE );

        for ( String stringToParse : Units.getAllConvenienceUnits() )
        {
            LOGGER.debug( "About to parse unit from string {}", stringToParse );
            Unit<?> unit = unitFormat.parse( stringToParse );
            LOGGER.debug( "Successfully parsed unit {} from string {}",
                          unit, stringToParse );
        }
    }
}
