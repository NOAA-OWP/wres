package wres.io.writing.netcdf;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import wres.datamodel.statistics.Statistic;

/**
 * Manages netcdf outputs for a project. Consumes a list of statistics. Writers of concrete 
 * types should extend this class.
 */

interface NetcdfWriter<T extends Statistic<?>> extends Function<List<T>,Set<Path>>
{
}
