package wres.metrics.timeseries;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.MetricConstants;
import wres.config.yaml.components.DatasetOrientation;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.PairsStatisticOuter;
import wres.datamodel.time.TimeSeries;
import wres.metrics.Metric;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pairs;
import wres.statistics.generated.PairsMetric;
import wres.statistics.generated.PairsStatistic;
import wres.statistics.generated.ReferenceTime;

/**
 * A plot of single-valued time-series of pairs.
 *
 * @author James Brown
 */
public class SingleValuedTimeSeriesPlot implements Metric<Pool<TimeSeries<Pair<Double, Double>>>, PairsStatisticOuter>
{
    /** The metric. */
    private static final PairsMetric PAIRS_METRIC = PairsMetric.newBuilder()
                                                               .setName( MetricName.TIME_SERIES_PLOT )
                                                               .build();

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static SingleValuedTimeSeriesPlot of()
    {
        return new SingleValuedTimeSeriesPlot();
    }

    @Override
    public PairsStatisticOuter apply( Pool<TimeSeries<Pair<Double, Double>>> pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        Pairs.Builder pairs = Pairs.newBuilder()
                                   .addLeftVariableNames( DatasetOrientation.LEFT.toString()
                                                                                 .toUpperCase() )
                                   .addRightVariableNames( DatasetOrientation.RIGHT.toString()
                                                                                   .toUpperCase() );

        List<TimeSeries<Pair<Double, Double>>> series = pool.get();
        for ( TimeSeries<Pair<Double, Double>> nextSeries : series )
        {
            Pairs.TimeSeriesOfPairs.Builder nextBuilder = Pairs.TimeSeriesOfPairs.newBuilder();
            Map<ReferenceTime.ReferenceTimeType, Instant> referenceTimes = nextSeries.getReferenceTimes();
            // Add the reference times
            referenceTimes.entrySet()
                          .stream()
                          .map( r -> MessageFactory.getReferenceTime( r.getKey(), r.getValue() ) )
                          .forEach( nextBuilder::addReferenceTimes );
            // Add the pairs
            nextSeries.getEvents()
                      .stream()
                      .map( e -> Pairs.Pair.newBuilder()
                                           .addLeft( e.getValue()
                                                      .getLeft() )
                                           .addRight( e.getValue()
                                                       .getRight() )
                                           .setValidTime( MessageUtilities.getTimestamp( e.getTime() ) ) )
                      .forEach( nextBuilder::addPairs );
            pairs.addTimeSeries( nextBuilder );
        }

        PairsStatistic statistic = PairsStatistic.newBuilder()
                                                 .setStatistics( pairs )
                                                 .setMetric( PAIRS_METRIC.toBuilder()
                                                                         .setUnits( pool.getMetadata()
                                                                                        .getMeasurementUnit()
                                                                                        .getUnit() ) )
                                                 .build();

        return PairsStatisticOuter.of( statistic, pool.getMetadata() );
    }

    @Override
    public String toString()
    {
        return this.getMetricName()
                   .toString();
    }

    @Override
    public MetricConstants getMetricName()
    {
        return MetricConstants.TIME_SERIES_PLOT;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * Hidden constructor.
     */

    private SingleValuedTimeSeriesPlot()
    {
    }
}
