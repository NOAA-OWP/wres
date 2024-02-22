package wres.reading;

import java.util.Objects;

import wres.datamodel.Ensemble;
import wres.datamodel.time.TimeSeries;

/**
 * A small value class that stores a tuple of time series. There is up to one time-series for each type of time-series 
 * event value.
 * 
 * @author James Brown
 */

public class TimeSeriesTuple
{
    /** A single-valued time-series. */
    private final TimeSeries<Double> singleValued;

    /** An ensemble time-series. */
    private final TimeSeries<Ensemble> ensemble;

    /** The data source from which the time-series originate. */
    private final DataSource dataSource;

    /**
     * Creates an instance with a single-valued time-series and an ensemble time-series.
     * @param singleValued the single-valued time-series
     * @param ensemble the ensemble time-series
     * @param dataSource the data source, required
     * @return the instance
     * @throws NullPointerException if the data source is null
     */

    public static TimeSeriesTuple of( TimeSeries<Double> singleValued,
                                      TimeSeries<Ensemble> ensemble,
                                      DataSource dataSource )
    {
        return new TimeSeriesTuple( singleValued, ensemble, dataSource );
    }

    /**
     * Creates an instance with a single-valued time-series.
     * @param singleValued the single-valued time-series
     * @param dataSource the data source, required
     * @return the instance
     * @throws NullPointerException if the data source is null
     */

    public static TimeSeriesTuple ofSingleValued( TimeSeries<Double> singleValued,
                                                  DataSource dataSource )
    {
        return new TimeSeriesTuple( singleValued, null, dataSource );
    }

    /**
     * Creates an instance with an ensemble time-series.
     * @param ensemble the ensemble time-series
     * @param dataSource the data source, required
     * @return the instance
     * @throws NullPointerException if the data source is null
     */

    public static TimeSeriesTuple ofEnsemble( TimeSeries<Ensemble> ensemble,
                                              DataSource dataSource )
    {
        return new TimeSeriesTuple( null, ensemble, dataSource );
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
     * @return the data source
     */

    public DataSource getDataSource()
    {
        return this.dataSource;
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
     * @param dataSource the data source, required
     * @throws NullPointerException if the data source is null
     */
    private TimeSeriesTuple( TimeSeries<Double> singleValued,
                             TimeSeries<Ensemble> ensemble,
                             DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        this.singleValued = singleValued;
        this.ensemble = ensemble;
        this.dataSource = dataSource;
    }
}
