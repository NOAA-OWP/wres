package wres.datamodel.baselines;

import java.util.function.Function;

import wres.datamodel.time.TimeSeries;

/**
 * Generates a baseline prediction from a template time-series.
 * @param <T> the type of baseline data generated
 * @author James Brown
 */
public interface BaselineGenerator<T> extends Function<TimeSeries<?>,TimeSeries<T>>
{
}
