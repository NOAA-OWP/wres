package wres.reading;

import java.util.Objects;

import lombok.Getter;

import wres.datamodel.types.Ensemble;
import wres.datamodel.time.TimeSeries;

/**
 * A small value class that stores a tuple of time series. There is up to one time-series for each type of time-series 
 * event value. Currently, this implementation supports both single-valued events, composed of {@link Double} and,
 * separately, ensemble events composed as {@link Ensemble}. Thus, data sources may contain a mixture of single-valued
 * and ensemble time-series. Each tuple contains up to one time-series of each type and both are optional, i.e., a
 * tuple can be empty.
 *
 * @author James Brown
 */

@Getter
public class TimeSeriesTuple
{
    /** A single-valued time-series. */
    private final TimeSeries<Double> singleValuedTimeSeries;

    /** An ensemble time-series. */
    private final TimeSeries<Ensemble> ensembleTimeSeries;

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
     * @return whether the single-valued time-series is set
     */

    public boolean hasSingleValuedTimeSeries()
    {
        return Objects.nonNull( this.singleValuedTimeSeries );
    }

    /**
     * @return whether the ensemble time-series is set
     */

    public boolean hasEnsembleTimeSeries()
    {
        return Objects.nonNull( this.ensembleTimeSeries );
    }

    /**
     * Hidden constructor.
     * @param singleValuedTimeSeries the single-valued series
     * @param ensembleTimeSeries the ensemble series
     * @param dataSource the data source, required
     * @throws NullPointerException if the data source is null
     */
    private TimeSeriesTuple( TimeSeries<Double> singleValuedTimeSeries,
                             TimeSeries<Ensemble> ensembleTimeSeries,
                             DataSource dataSource )
    {
        Objects.requireNonNull( dataSource );

        this.singleValuedTimeSeries = singleValuedTimeSeries;
        this.ensembleTimeSeries = ensembleTimeSeries;
        this.dataSource = dataSource;
    }
}
