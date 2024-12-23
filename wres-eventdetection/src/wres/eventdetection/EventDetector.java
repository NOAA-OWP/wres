package wres.eventdetection;

import java.util.Set;

import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;

/**
 * A protocol for detecting "events" in time-series data. AN "event" in this context is described by a
 * {@link TimeWindowOuter} with an earliest and latest valid datetime.
 *
 * @author James Brown
 */
public interface EventDetector
{

    /**
     * Performs event detection.
     *
     * @param timeSeries the time-series data
     * @return the detected events
     * @throws NullPointerException if the input is null
     * @throws EventDetectionException if the event detection fails for any other reason
     */
    Set<TimeWindowOuter> detect( TimeSeries<Double> timeSeries );

}
