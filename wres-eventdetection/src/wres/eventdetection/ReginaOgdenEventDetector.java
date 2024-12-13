package wres.eventdetection;

import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.EventDetection;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.time.TimeWindowOuter;

/**
 * An implementation of the method described in <a href="https://onlinelibrary.wiley.com/doi/10.1002/hyp.14405">
 * Regina and Ogden (2021)</a>. Ported from the original Python code, available here:
 * <a href="https://github.com/NOAA-OWP/hydrotools/blob/main/python/events/src/hydrotools/events/event_detection/decomposition.py">HydroTools</a>.
 *
 * @author James Brown
 */
public class ReginaOgdenEventDetector implements EventDetector
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( ReginaOgdenEventDetector.class );

    @Override
    public Set<TimeWindowOuter> detect( TimeSeries<Double> timeSeries, EventDetection parameters )
    {
        Objects.requireNonNull( timeSeries );
        Objects.requireNonNull( parameters );

        LOGGER.debug( "Performing event detection using the Regina-Ogden method, which is described here: "
                      + "https://doi.org/10.1002/hyp.14405" );

        // Unbounded time window, placeholder
        return Set.of( TimeWindowOuter.of( wres.statistics.MessageFactory.getTimeWindow() ) );
    }

    /**
     * Creates an instance.
     * @return a detector instance
     */
    static ReginaOgdenEventDetector of()
    {
        return new ReginaOgdenEventDetector();
    }

    /**
     * Hidden constructor.
     */

    private ReginaOgdenEventDetector()
    {
    }
}
