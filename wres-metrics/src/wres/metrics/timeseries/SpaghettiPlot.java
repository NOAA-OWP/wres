package wres.metrics.timeseries;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;

import wres.config.MetricConstants;
import wres.config.components.DatasetOrientation;
import wres.datamodel.Slicer;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolException;
import wres.datamodel.statistics.PairsStatisticOuter;
import wres.datamodel.time.Event;
import wres.datamodel.time.TimeSeries;
import wres.datamodel.types.Ensemble;
import wres.metrics.Metric;
import wres.metrics.MetricCalculationException;
import wres.statistics.MessageUtilities;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Pairs;
import wres.statistics.generated.PairsMetric;
import wres.statistics.generated.PairsStatistic;
import wres.statistics.generated.ReferenceTime;

/**
 * A plot of time-series of ensemble pairs.
 *
 * @author James Brown
 */
public class SpaghettiPlot implements Metric<Pool<TimeSeries<Pair<Double, Ensemble>>>, PairsStatisticOuter>
{
    /** The metric. */
    private static final PairsMetric PAIRS_METRIC = PairsMetric.newBuilder()
                                                               .setName( MetricName.SPAGHETTI_PLOT )
                                                               .build();

    /**
     * Returns an instance.
     *
     * @return an instance
     */

    public static SpaghettiPlot of()
    {
        return new SpaghettiPlot();
    }

    @Override
    public PairsStatisticOuter apply( Pool<TimeSeries<Pair<Double, Ensemble>>> pool )
    {
        if ( Objects.isNull( pool ) )
        {
            throw new PoolException( "Specify non-null input to the '" + this + "'." );
        }

        Pairs.Builder pairs = Pairs.newBuilder();

        List<TimeSeries<Pair<Double, Ensemble>>> series = pool.get()
                                                              .stream()
                                                              .filter( f -> !f.getEvents()
                                                                              .isEmpty() )
                                                              .toList();

        this.validateLabels( series );

        // Find the superset of labels across all series
        SortedSet<String> stringLabels = series.stream()
                                               .flatMap( t -> t.getEvents()
                                                               .stream()
                                                               .flatMap( e -> Arrays.stream( e.getValue()
                                                                                              .getRight()
                                                                                              .getLabels()
                                                                                              .getLabels() ) ) )
                                               .collect( Collectors.toCollection( TreeSet::new ) );

        // Add the variable names
        if ( !series.isEmpty() )
        {
            pairs.addLeftVariableNames( DatasetOrientation.LEFT.toString()
                                                               .toUpperCase() );
            pairs.addAllRightVariableNames( stringLabels );
        }

        for ( TimeSeries<Pair<Double, Ensemble>> nextSeries : series )
        {

            Pairs.TimeSeriesOfPairs.Builder nextBuilder = Pairs.TimeSeriesOfPairs.newBuilder();
            Map<ReferenceTime.ReferenceTimeType, Instant> referenceTimes = nextSeries.getReferenceTimes();

            // Add the reference times
            referenceTimes.entrySet()
                          .stream()
                          .map( r -> MessageFactory.getReferenceTime( r.getKey(), r.getValue() ) )
                          .forEach( nextBuilder::addReferenceTimes );

            // Create a function that maps the pairs
            Function<Event<Pair<Double, Ensemble>>, Pairs.Pair.Builder> mapper = pair ->
            {
                Pairs.Pair.Builder builder = Pairs.Pair.newBuilder()
                                                       .addLeft( pair.getValue()
                                                                     .getLeft() )
                                                       .setValidTime( MessageUtilities.getTimestamp( pair.getTime() ) );

                Ensemble unpadded = pair.getValue()
                                        .getRight();

                // Pad the ensemble with a consistent set of members. See GitHub #711.
                Ensemble padded = Slicer.pad( unpadded, stringLabels );

                for ( double nextMember : padded.getMembers() )
                {
                    builder.addRight( nextMember );
                }

                return builder;
            };

            // Map the pairs and add them
            nextSeries.getEvents()
                      .stream()
                      .map( mapper )
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
        return MetricConstants.SPAGHETTI_PLOT;
    }

    @Override
    public boolean hasRealUnits()
    {
        return true;
    }

    /**
     * Validates the presence of labels across all ensembles.
     *
     * @param timeSeries the time-series
     */

    private void validateLabels( List<TimeSeries<Pair<Double, Ensemble>>> timeSeries )
    {
        if ( timeSeries.stream()
                       .flatMap( t -> t.getEvents()
                                       .stream() )
                       .map( Event::getValue )
                       .anyMatch( e -> !e.getRight()
                                         .hasLabels() ) )
        {
            throw new MetricCalculationException( "Cannot calculate a spaghetti plot for ensemble time-series that do "
                                                  + "not contain labels." );
        }
    }

    /**
     * Hidden constructor.
     */

    private SpaghettiPlot()
    {
    }
}
