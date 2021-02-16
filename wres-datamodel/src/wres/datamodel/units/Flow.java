package wres.datamodel.units;

import javax.measure.Unit;

import si.uom.quantity.VolumetricFlowRate;
import tech.units.indriya.unit.ProductUnit;

import static tech.units.indriya.unit.Units.CUBIC_METRE;
import static tech.units.indriya.unit.Units.SECOND;

/**
 * Experimental class to build flow units using javax.measure. When it is used,
 */

class Flow
{
    static final Unit<VolumetricFlowRate> CUBIC_METRE_PER_SECOND =
            new ProductUnit<VolumetricFlowRate>( CUBIC_METRE.divide( SECOND ) );
}
