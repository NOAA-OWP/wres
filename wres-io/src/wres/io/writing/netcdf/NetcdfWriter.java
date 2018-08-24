package wres.io.writing.netcdf;

import java.util.function.Consumer;

import wres.datamodel.statistics.ListOfStatistics;
import wres.datamodel.statistics.Statistic;

/**
 * Manages netcdf outputs for a project. Consumes a {@link ListOfStatistics}. Writers of concrete 
 * types should extend this class.
 */

interface NetcdfWriter<T extends Statistic<?>> extends Consumer<ListOfStatistics<T>>
{
}
