package wres.io.writing.netcdf;

import java.util.function.Consumer;

import wres.datamodel.statistics.ListOfMetricOutput;
import wres.datamodel.statistics.MetricOutput;

/**
 * Manages netcdf outputs for a project. Consumes a {@link ListOfMetricOutput}. Writers of concrete 
 * types should extend this class.
 */

interface NetcdfWriter<T extends MetricOutput<?>> extends Consumer<ListOfMetricOutput<T>>
{
}
