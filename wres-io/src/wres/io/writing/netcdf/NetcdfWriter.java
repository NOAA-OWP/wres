package wres.io.writing.netcdf;

import java.util.List;
import java.util.function.Consumer;
import wres.datamodel.statistics.Statistic;

/**
 * Manages netcdf outputs for a project. Consumes a list of statistics. Writers of concrete 
 * types should extend this class.
 */

interface NetcdfWriter<T extends Statistic<?>> extends Consumer<List<T>>
{
}
