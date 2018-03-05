package wres.io.writing.netcdf;

import java.util.function.Consumer;

import wres.datamodel.outputs.MetricOutput;
import wres.datamodel.outputs.MetricOutputMapByTimeAndThreshold;
import wres.datamodel.outputs.MetricOutputMultiMapByTimeAndThreshold;

/**
 * Manages netcdf outputs for a project. Consumes a {@link MetricOutputMapByTimeAndThreshold}. Writers of concrete 
 * types should extend this class.
 */

interface NetcdfWriter<T extends MetricOutput<?>> extends Consumer<MetricOutputMultiMapByTimeAndThreshold<T>>
{
}
