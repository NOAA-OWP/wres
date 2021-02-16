package wres.datamodel.units;

import javax.measure.Unit;

import si.uom.quantity.VolumetricFlowRate;
import tech.units.indriya.unit.ProductUnit;

import static tech.units.indriya.unit.Units.CUBIC_METRE;
import static tech.units.indriya.unit.Units.SECOND;
import static systems.uom.common.USCustomary.CUBIC_FOOT;

/**
 * Experimental class to build flow units using javax.measure. When it is used,
 * visibility will likely need to be public and the library declarations updated
 * in the build.gradle to be "api" instead of "implementation."
 */

class Flow
{
    static final Unit<VolumetricFlowRate> CUBIC_METRE_PER_SECOND =
            new ProductUnit<VolumetricFlowRate>( CUBIC_METRE.divide( SECOND ) );
    static final Unit<VolumetricFlowRate> CUBIC_FOOT_PER_SECOND =
            new ProductUnit<VolumetricFlowRate>( CUBIC_FOOT.divide( SECOND ) );
}
