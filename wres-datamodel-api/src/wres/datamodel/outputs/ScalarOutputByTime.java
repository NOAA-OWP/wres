package wres.datamodel.outputs;

import wres.datamodel.time.TimeSeries;

/**
 * A scalar output for each of several times.
 * 
 * @author james.brown@hydrosolved.com
 * @version 0.1
 * @since 0.3
 */

public interface ScalarOutputByTime extends MetricOutput<Double>, TimeSeries<Double>
{
}
