package wres.datamodel;

import javax.measure.Quantity;
import javax.measure.Unit;
import javax.measure.UnitConverter;
import javax.measure.quantity.Time;
import javax.measure.quantity.Volume;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import si.uom.quantity.VolumetricFlowRate;
import tech.units.indriya.quantity.Quantities;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static systems.uom.common.USCustomary.GALLON_LIQUID;
import static tech.units.indriya.unit.Units.CUBIC_METRE;
import static tech.units.indriya.unit.Units.DAY;
import static wres.datamodel.Units.CUBIC_FOOT_PER_SECOND;
import static wres.datamodel.Units.CUBIC_METRE_PER_SECOND;

/**
 * Experimental/exploratory as of 2021-02-17 commits
 */

public class UnitsTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( UnitsTest.class );

    @Test
    public void testConvertFlow()
    {
        double someCubicMetersPerSecondNoUnits = 947.0;
        Quantity<VolumetricFlowRate> someCubicMetersPerSecond =
                Quantities.getQuantity( someCubicMetersPerSecondNoUnits,
                                        CUBIC_METRE_PER_SECOND );
        LOGGER.debug( "Quantity of CMS flow: {}", someCubicMetersPerSecond );
        double someCubicFeetPerSecondNoUnits =
                Units.convertFlow( someCubicMetersPerSecond, CUBIC_FOOT_PER_SECOND );
        LOGGER.debug( "Converted-to-CFS raw value: {}", someCubicFeetPerSecondNoUnits );
        assertTrue( someCubicFeetPerSecondNoUnits > someCubicMetersPerSecondNoUnits,
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
                ( ( Unit<Volume> ) someVolume.getUnit() ).getConverterTo( GALLON_LIQUID );
        double someGallonsDouble = unitConverter.convert( someVolume.getValue()
                                                                    .doubleValue() );
        LOGGER.debug( "Converted to gallons (double): {}", someGallonsDouble );
        Number someGallonsNumber = unitConverter.convert( someVolume.getValue() );
        LOGGER.debug( "Converted to gallons (Number): {}", someGallonsNumber );
        LOGGER.debug( "Class of the Number: {}", someGallonsNumber.getClass() );

        // More straightforward than using a converter:
        Quantity<Volume> gallons = ( ( Quantity<Volume> ) someVolume).to( GALLON_LIQUID );
        LOGGER.debug( "Converted to gallons again: {}", gallons );

        // What about cubic meters?
        Quantity<?> cubicMeters = ( ( Quantity<Volume> ) someVolume).to( CUBIC_METRE );
        LOGGER.debug( "Converted to cubicMeters: {}", cubicMeters );
    }
}
