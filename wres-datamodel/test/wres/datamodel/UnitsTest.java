package wres.datamodel;

import java.util.List;
import javax.measure.Quantity;
import javax.measure.Unit;
import javax.measure.UnitConverter;
import javax.measure.format.MeasurementParseException;
import javax.measure.format.UnitFormat;
import javax.measure.quantity.Length;
import javax.measure.quantity.Time;
import javax.measure.quantity.Volume;
import javax.measure.spi.ServiceProvider;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import si.uom.quantity.VolumetricFlowRate;
import tech.units.indriya.AbstractUnit;
import tech.units.indriya.format.EBNFUnitFormat;
import tech.units.indriya.quantity.Quantities;
import tech.units.indriya.unit.ProductUnit;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static systems.uom.common.USCustomary.FOOT;
import static systems.uom.common.USCustomary.GALLON_LIQUID;
import static systems.uom.common.USCustomary.INCH;
import static systems.uom.common.USCustomary.CUBIC_FOOT;
import static tech.units.indriya.unit.Units.CUBIC_METRE;
import static tech.units.indriya.unit.Units.DAY;
import static tech.units.indriya.unit.Units.SECOND;

/**
 * Experimental/exploratory as of 2021-02-17 commits
 */

public class UnitsTest
{
    static final Unit<VolumetricFlowRate> CUBIC_METRE_PER_SECOND =
            new ProductUnit<>( CUBIC_METRE.divide( SECOND ) );
    static final Unit<VolumetricFlowRate> CUBIC_FOOT_PER_SECOND =
            new ProductUnit<>( CUBIC_FOOT.divide( SECOND ) );
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
                     "mm s^-1", "kg/m^2/h" );

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
                        GALLON_LIQUID );
        double someGallonsDouble = unitConverter.convert( someVolume.getValue()
                                                                    .doubleValue() );
        LOGGER.debug( "Converted to gallons (double): {}", someGallonsDouble );
        Number someGallonsNumber =
                unitConverter.convert( someVolume.getValue() );
        LOGGER.debug( "Converted to gallons (Number): {}", someGallonsNumber );
        LOGGER.debug( "Class of the Number: {}", someGallonsNumber.getClass() );

        // More straightforward than using a converter:
        Quantity<Volume> gallons =
                ( ( Quantity<Volume> ) someVolume ).to( GALLON_LIQUID );
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
    public void exploreServiceProviderUnitParsing()
    {
        UnitFormat unitFormat = ServiceProvider.current()
                                               .getFormatService()
                                               .getUnitFormat();
        for ( String stringToParse : UNITS_TO_PARSE )
        {
            try
            {
                Unit<?> unit = unitFormat.parse( stringToParse );
                LOGGER.debug(
                        "SPI parsed '{}' into '{}', name='{}', symbol='{}', dimension='{}'",
                        stringToParse,
                        unit,
                        unit.getName(),
                        unit.getSymbol(),
                        unit.getDimension() );
            }
            catch ( MeasurementParseException mpe )
            {
                LOGGER.debug( "Exception while parsing '{}': {}",
                              stringToParse, mpe.getMessage() );
            }
        }
    }

    @Test
    public void exploreIndriyaUnitParsing()
    {
        Unit<Length> foot = FOOT;

        for ( String stringToParse : UNITS_TO_PARSE )
        {
            try
            {
                // Apparently identical to the implementation gotten from SPI.
                Unit<?> unit = AbstractUnit.parse( stringToParse );
                LOGGER.debug(
                        "Indriya parsed '{}' into '{}', name='{}', symbol='{}', dimension='{}'",
                        stringToParse,
                        unit,
                        unit.getName(),
                        unit.getSymbol(),
                        unit.getDimension() );
            }
            catch ( MeasurementParseException mpe )
            {
                LOGGER.debug( "Exception while parsing '{}': {}",
                              stringToParse, mpe.getMessage() );
            }
        }
    }

    @Test
    public void exploreIndriyaKiloCubicFeetPerSecond()
    {
        // Even though "inch" is unused, it causes the USCustomary units to load
        Unit<Length> inch = INCH;
        String stringToParse = "ft^3*1000/s";
        Unit<VolumetricFlowRate> unit = (Unit<VolumetricFlowRate>) AbstractUnit.parse( stringToParse );
        LOGGER.debug( "Parsed '{}' into '{}', name='{}', symbol='{}', dimension='{}'",
                      stringToParse,
                      unit,
                      unit.getName(),
                      unit.getSymbol(),
                      unit.getDimension() );
        Quantity<VolumetricFlowRate> someKcfs = Quantities.getQuantity( 50.0, unit );
        LOGGER.debug( someKcfs.to( CUBIC_FOOT_PER_SECOND ).toString() );
    }

    @Test
    public void exploreEBNFUnitParsing()
    {
        UnitFormat unitFormat = EBNFUnitFormat.getInstance();

        for ( String stringToParse : UNITS_TO_PARSE )
        {
            try
            {
                // Apparently identical to the implementation gotten from SPI.
                Unit<?> unit = unitFormat.parse( stringToParse );
                LOGGER.debug(
                        "Indriya parsed '{}' into '{}', name='{}', symbol='{}', dimension='{}'",
                        stringToParse,
                        unit,
                        unit.getName(),
                        unit.getSymbol(),
                        unit.getDimension() );
            }
            catch ( MeasurementParseException | IllegalArgumentException e )
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

    @Test
    public void verifyAllConvenienceUnitsCanBeParsed()
    {
        for ( String stringToParse : Units.getAllConvenienceUnits() )
        {
            Unit<?> unit = AbstractUnit.parse( stringToParse );
            LOGGER.debug( "Successfully parsed unit {} from string {}",
                          unit, stringToParse );
        }
    }
}
