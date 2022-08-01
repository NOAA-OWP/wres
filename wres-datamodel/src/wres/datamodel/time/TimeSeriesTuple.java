package wres.datamodel.time;

import java.util.Objects;

import wres.datamodel.Ensemble;

/**
 * A small value class that stores a tuple of time series, one of each type.
 * 
 * @author James Brown
 */

public class TimeSeriesTuple
{
    /** A single-valued time-series. */
    private final TimeSeries<Double> singleValued;

    /** An ensemble time-series. */
    private final TimeSeries<Ensemble> ensemble;

    /**
     * Creates an instance with a single-valued time-series and an ensemble time-series.
     * @param singleValued the single-valued time-series
     * @param ensemble the ensemble time-series
     * @return the instance
     */

    public static TimeSeriesTuple of( TimeSeries<Double> singleValued, TimeSeries<Ensemble> ensemble )
    {
        return new TimeSeriesTuple( singleValued, ensemble );
    }

    /**
     * Creates an instance with a single-valued time-series.
     * @param singleValued the single-valued time-series
     * @return the instance
     */

    public static TimeSeriesTuple ofSingleValued( TimeSeries<Double> singleValued )
    {
        return new TimeSeriesTuple( singleValued, null );
    }

    /**
     * Creates an instance with an ensemble time-series.
     * @param ensemble the ensemble time-series
     * @return the instance
     */

    public static TimeSeriesTuple ofEnsemble( TimeSeries<Ensemble> ensemble )
    {
        return new TimeSeriesTuple( null, ensemble );
    }

    /**
     * @return the single-valued time-series or null
     */

    public TimeSeries<Double> getSingleValuedTimeSeries()
    {
        return this.singleValued;
    }

    /**
     * @return the ensemble time-series or null
     */

    public TimeSeries<Ensemble> getEnsembleTimeSeries()
    {
        return this.ensemble;
    }

    /**
     * @return whether the single-valued time-series is set
     */

    public boolean hasSingleValuedTimeSeries()
    {
        return Objects.nonNull( this.singleValued );
    }

    /**
     * @return whether the ensemble time-series is set
     */

    public boolean hasEnsembleTimeSeries()
    {
        return Objects.nonNull( this.ensemble );
    }

    /**
     * Hidden constructor.
     * @param singleValued the single-valued series
     * @param ensemble the ensemble series
     */
    private TimeSeriesTuple( TimeSeries<Double> singleValued,
                             TimeSeries<Ensemble> ensemble )
    {
        this.singleValued = singleValued;
        this.ensemble = ensemble;
    }
}
