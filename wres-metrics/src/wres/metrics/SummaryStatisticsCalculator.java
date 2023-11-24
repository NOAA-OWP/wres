package wres.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

import net.jcip.annotations.ThreadSafe;
import org.eclipse.collections.api.list.primitive.MutableDoubleList;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.list.mutable.primitive.DoubleArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;

import wres.statistics.MessageFactory;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.DiagramMetric;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationDiagramStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Statistics;

/**
 * <p>Accepts raw statistics as they are computed and, on request, closes the instance to further input and calculates the
 * summary statistics nominated on construction. Optionally, filters the supplied statistics with a filter provided on
 * construction, ignoring any statistics that do not meet the filter condition. It is assumed that each blob of
 * statistics has a common structure for each supplied metric. For example, if one blob of statistics contains a
 * reliability diagram with five bins, then all other reliability diagrams provided to the same instance must contain
 * five bins.
 *
 * <p>Implementation notes:
 *
 * <p>This is an in-memory implementation, not backed by a database and is, therefore, constrained by system memory.
 * Callers should use the APIs advertised by this class, rather than the class itself, as that will allow the
 * implementation to be swapped for a database variant, when a database schema is available. See #45466.
 *
 * @author James Brown
 */

@ThreadSafe
public class SummaryStatisticsCalculator implements Supplier<List<Statistics>>, Consumer<Statistics>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( SummaryStatisticsCalculator.class );

    /** Filters durations. */
    private static final UnaryOperator<List<Duration>> DURATION_FILTER =
            durations -> durations.stream()
                                  .filter( Objects::nonNull )
                                  .toList();

    /** The cached samples of score statistics. */
    private final Map<DoubleScoreName, MutableDoubleList> doubleScores;

    /** The cached sample of diagram statistics with each column containing one sample and each row containing all
     * samples for one index of the diagram component. */
    private final Map<DiagramName, List<MutableDoubleList>> diagrams;

    /** The cached sample of duration score statistics. */
    private final Map<DurationScoreName, List<Duration>> durationScores;

    /** The cached sample of duration diagram statistics. */
    private final Map<MetricName, Map<Instant, List<Duration>>> durationDiagrams;

    /** Whether this instance has been completed and is read-only. */
    private final AtomicBoolean isComplete;

    /** A filter supplied on construction. */
    private final Predicate<Statistics> filter;

    /** The summary statistics to calculate on completion. */
    private final List<SummaryStatisticFunction> summaryStatistics;

    /** The calculated summary statistics. */
    private final List<Statistics> statistics;

    /** Locks the creation of new diagram row slots within the {@link #diagrams}. */
    private final ReentrantLock diagramRowSlotCreationLock = new ReentrantLock();

    /** The first statistics added to this instance. */
    private Statistics nominal = null;

    /**
     * Creates an instance.
     * @param summaryStatistics the summary statistics to calculate, required and not empty
     * @param filter an optional filter
     * @return an instance
     * @throws NullPointerException if the list of statistics is null
     * @throws IllegalArgumentException if the list of statistics is empty
     */
    public static SummaryStatisticsCalculator of( List<SummaryStatisticFunction> summaryStatistics,
                                                  Predicate<Statistics> filter )
    {
        return new SummaryStatisticsCalculator( summaryStatistics, filter );
    }

    /**
     * Closes the calculator to further additions via {@link #accept(Statistics)} and returns a {@link Statistics} for
     * each summary statistic requested on construction.
     *
     * @return the summary statistics
     */
    @Override
    public List<Statistics> get()
    {
        if ( !this.isComplete.getAndSet( true ) )
        {
            this.calculateSummaryStatistics();
        }

        return Collections.unmodifiableList( this.statistics );
    }

    /**
     * Adds the supplied statistics to the internal store.
     * @param statistics the statistics
     * @throws NullPointerException if the statistics is null
     * @throws IllegalArgumentException if this instance has been completed and no further statistics are expected
     */

    @Override
    public void accept( Statistics statistics )
    {
        Objects.requireNonNull( statistics );

        if ( this.isComplete.get() )
        {
            throw new IllegalArgumentException( "No further statistics can be added, as this instance has been marked "
                                                + "complete." );
        }

        // Filter statistics that do not meet the filter supplied on construction
        if ( !this.filter.test( statistics )
             && LOGGER.isTraceEnabled() )
        {
            LOGGER.debug( "Rejected these statistics as they do not meet the filter: {}.", statistics );
        }

        // Remove any statistics that do not support summary statistics
        statistics = this.filter( statistics );

        // Set the nominal statistics whose metadata will be used when reporting summary statistics
        if ( Objects.isNull( this.nominal ) )
        {
            this.nominal = statistics;
        }

        // Add the double score statistics
        this.addDoubleScores( statistics );
        // Add the duration score statistics
        this.addDurationScores( statistics );
        // Add the diagrams
        this.addDiagrams( statistics );
        // Add the duration diagrams
        this.addDurationDiagrams( statistics );

        LOGGER.debug( "Added statistics in thread {}.",
                      Thread.currentThread()
                            .getName() );
    }

    /**
     * Add the double score statistics to the internal store.
     * @param source the raw statistics
     */

    private void addDoubleScores( Statistics source )
    {
        List<DoubleScoreStatistic> scoresList = source.getScoresList();

        for ( DoubleScoreStatistic score : scoresList )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            for ( DoubleScoreStatistic.DoubleScoreStatisticComponent component : score.getStatisticsList() )
            {
                DoubleScoreName name = new DoubleScoreName( metricName,
                                                            component.getMetric()
                                                                     .getName() );

                MutableDoubleList samples = this.getOrAddDoubleScoreSlot( name );
                double scoreValue = component.getValue();
                samples.add( scoreValue );
            }
        }
    }

    /**
     * Adds a double score slot for the named metric, as needed, and returns the slot.
     * @param name the named metric
     * @return the slot
     */
    private MutableDoubleList getOrAddDoubleScoreSlot( DoubleScoreName name )
    {
        // Add a thread-safe mutable list
        this.doubleScores.putIfAbsent( name, new DoubleArrayList().asSynchronized() );
        return this.doubleScores.get( name );
    }

    /**
     * Add the duration score statistics to the internal store.
     * @param source the raw statistics
     */

    private void addDurationScores( Statistics source )
    {
        List<DurationScoreStatistic> scoresList = source.getDurationScoresList();

        for ( DurationScoreStatistic score : scoresList )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            for ( DurationScoreStatistic.DurationScoreStatisticComponent component : score.getStatisticsList() )
            {
                DurationScoreName name = new DurationScoreName( metricName,
                                                                component.getMetric()
                                                                         .getName() );

                List<Duration> samples = this.getOrAddDurationScoreSlot( name );
                Duration nextScore = MessageFactory.parse( component.getValue() );
                samples.add( nextScore );
            }
        }
    }

    /**
     * Adds a duration score slot for the named metric, as needed, and returns the slot.
     * @param name the named metric
     * @return the slot
     */
    private List<Duration> getOrAddDurationScoreSlot( DurationScoreName name )
    {
        // Add a thread-safe mutable list
        this.durationScores.putIfAbsent( name, new FastList<Duration>().asSynchronized() );
        return this.durationScores.get( name );
    }

    /**
     * Add the diagram statistics to the internal store.
     * @param source the raw statistics
     */

    private void addDiagrams( Statistics source )
    {
        List<DiagramStatistic> diagramsList = source.getDiagramsList();

        for ( DiagramStatistic diagram : diagramsList )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();
            for ( DiagramStatistic.DiagramStatisticComponent component : diagram.getStatisticsList() )
            {
                DiagramName name = new DiagramName( metricName,
                                                    component.getMetric()
                                                             .getName(),
                                                    component.getName() );
                List<MutableDoubleList> samples = this.getOrAddDiagramSlot( name );

                int valuesCount = component.getValuesCount();
                for ( int i = 0; i < valuesCount; i++ )
                {
                    MutableDoubleList row = this.getOrAddDiagramRowSlot( samples, i );
                    double value = component.getValues( i );
                    row.add( value );
                }
            }
        }
    }

    /**
     * Adds a diagram slot for the named metric, as needed, and returns the slot.
     * @param name the named metric
     * @return the slot
     */
    private List<MutableDoubleList> getOrAddDiagramSlot( DiagramName name )
    {
        // Add a thread-safe list (of lists)
        this.diagrams.putIfAbsent( name, new FastList<MutableDoubleList>().asSynchronized() );
        return this.diagrams.get( name );
    }

    /**
     * Adds a slot for a diagram row, as needed, and returns the slot.
     * @param slots the slots
     * @param slotIndex the slot index required
     * @return the diagram row slot
     */
    private MutableDoubleList getOrAddDiagramRowSlot( List<MutableDoubleList> slots, int slotIndex )
    {
        try
        {
            // Lock to ensure that the check on list size, creation and addition of a new list are all atomic. Even
            // though the outer list is a thread-safe variant, this entire sequence needs to be atomic otherwise
            // multiple threads could enter the "add list" step below (the multiple lists then being added atomically,
            // but duplicated)
            this.diagramRowSlotCreationLock.lock();

            if ( slotIndex < slots.size() )
            {
                return slots.get( slotIndex );
            }

            // Add a thread-safe list
            MutableDoubleList newSlot = new DoubleArrayList().asSynchronized();
            slots.add( newSlot );

            return newSlot;
        }
        finally
        {
            this.diagramRowSlotCreationLock.unlock();
        }
    }

    /**
     * Add the duration diagram statistics to the internal store.
     * @param source the raw statistics
     */

    private void addDurationDiagrams( Statistics source )
    {
        List<DurationDiagramStatistic> diagramsList = source.getDurationDiagramsList();

        for ( DurationDiagramStatistic diagram : diagramsList )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();

            Map<Instant, List<Duration>> samples = this.getOrAddDurationDiagramSlot( metricName );

            int valuesCount = diagram.getStatisticsCount();
            for ( int i = 0; i < valuesCount; i++ )
            {
                DurationDiagramStatistic.PairOfInstantAndDuration statistic = diagram.getStatistics( i );
                Instant instant = MessageFactory.parse( statistic.getTime() );
                Duration duration = MessageFactory.parse( statistic.getDuration() );

                // Create a thread safe list to add, if needed
                List<Duration> newList = new FastList<Duration>().asSynchronized();
                List<Duration> durations = samples.putIfAbsent( instant, newList );

                // Slot just created, get it
                if ( Objects.isNull( durations ) )
                {
                    durations = samples.get( instant );
                }

                durations.add( duration );
            }
        }
    }

    /**
     * Adds a duration diagram slot for the named metric, as needed, and returns the slot.
     * @param name the named metric
     * @return the slot
     */
    private Map<Instant, List<Duration>> getOrAddDurationDiagramSlot( MetricName name )
    {
        this.durationDiagrams.putIfAbsent( name, new ConcurrentHashMap<>() );
        return this.durationDiagrams.get( name );
    }

    /**
     * Calculates the summary statistics on request.
     */

    private void calculateSummaryStatistics()
    {
        if ( !this.statistics.isEmpty() )
        {
            LOGGER.debug( "The summary statistics have already been calculated." );
            return;
        }

        if ( Objects.isNull( this.nominal ) )
        {
            LOGGER.debug( "No statistics were added from which to calculate summary statistics." );
            return;
        }

        LOGGER.debug( "Calculating {} summary statistics: {}.", this.summaryStatistics.size(), this.summaryStatistics );

        for ( SummaryStatisticFunction summaryStatistic : this.summaryStatistics )
        {
            Statistics.Builder builder = this.nominal.toBuilder()
                                                     .setSummaryStatistic( summaryStatistic.statistic() );

            // Set the summary statistic for the double scores
            List<DoubleScoreStatistic.Builder> scoreBuilders = builder.getScoresBuilderList();
            this.calculateDoubleScoreSummaryStatistic( scoreBuilders, summaryStatistic );

            // Set the quantiles for the duration scores
            List<DurationScoreStatistic.Builder> durationScoreBuilders = builder.getDurationScoresBuilderList();
            this.calculateDurationScoreSummaryStatistics( durationScoreBuilders, summaryStatistic );

            // Set the quantiles for the diagrams
            List<DiagramStatistic.Builder> diagramBuilders = builder.getDiagramsBuilderList();
            this.calculateDiagramSummaryStatistics( diagramBuilders, summaryStatistic );

            // Set the quantiles for the diagrams
            List<DurationDiagramStatistic.Builder> durationDiagramBuilders = builder.getDurationDiagramsBuilderList();
            this.calculateDurationDiagramSummaryStatistics( durationDiagramBuilders, summaryStatistic );

            // Add the quantile
            this.statistics.add( builder.build() );
        }

        LOGGER.debug( "Finished setting the summary statistics. This calculator is now read only." );
    }

    /**
     * Calculates a summary statistics for the double scores.
     * @param builders the builders to update
     * @param summaryStatistic the summary statistic to calculate
     */

    private void calculateDoubleScoreSummaryStatistic( List<DoubleScoreStatistic.Builder> builders,
                                                       SummaryStatisticFunction summaryStatistic )
    {
        for ( DoubleScoreStatistic.Builder score : builders )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            List<DoubleScoreStatistic.DoubleScoreStatisticComponent.Builder> componentBuilders =
                    score.getStatisticsBuilderList();
            for ( DoubleScoreStatistic.DoubleScoreStatisticComponent.Builder component : componentBuilders )
            {
                DoubleScoreName name = new DoubleScoreName( metricName,
                                                            component.getMetric()
                                                                     .getName() );
                MutableDoubleList samples = this.doubleScores.get( name );
                double[] raw = samples.toArray();
                double statisticValue = summaryStatistic.applyAsDouble( raw );
                component.setValue( statisticValue );
            }
        }
    }

    /**
     * Calculates the summary statistic for the duration scores.
     * @param builders the builders to update
     * @param summaryStatistic the summary statistic
     */

    private void calculateDurationScoreSummaryStatistics( List<DurationScoreStatistic.Builder> builders,
                                                          SummaryStatisticFunction summaryStatistic )
    {
        Function<Duration[], Duration> durationStatistic =
                FunctionFactory.ofDurationFromUnivariateFunction( summaryStatistic );

        for ( DurationScoreStatistic.Builder score : builders )
        {
            MetricName metricName = score.getMetric()
                                         .getName();
            List<DurationScoreStatistic.DurationScoreStatisticComponent.Builder> componentBuilders =
                    score.getStatisticsBuilderList();
            for ( DurationScoreStatistic.DurationScoreStatisticComponent.Builder component : componentBuilders )
            {
                DurationScoreName name = new DurationScoreName( metricName,
                                                                component.getMetric()
                                                                         .getName() );
                List<Duration> samples = this.durationScores.get( name );

                // Filter any null values and sort. Null values can occur if the pairs were empty, for example,
                // and no duration errors were produced. Empty pairs can occur when slicing a realization by
                // threshold.
                samples = DURATION_FILTER.apply( samples );
                Duration[] durations = samples.toArray( new Duration[0] );

                Duration statisticValue = durationStatistic.apply( durations );
                com.google.protobuf.Duration statisticProto = MessageFactory.parse( statisticValue );
                component.setValue( statisticProto );
            }
        }
    }

    /**
     * Calculates the summary statistic for diagrams.
     * @param builders the builders to update
     * @param summaryStatistic the summary statistic
     */

    private void calculateDiagramSummaryStatistics( List<DiagramStatistic.Builder> builders,
                                                    SummaryStatisticFunction summaryStatistic )
    {
        for ( DiagramStatistic.Builder diagram : builders )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();
            List<DiagramStatistic.DiagramStatisticComponent.Builder> componentBuilders =
                    diagram.getStatisticsBuilderList();
            for ( DiagramStatistic.DiagramStatisticComponent.Builder component : componentBuilders )
            {
                DiagramName name = new DiagramName( metricName,
                                                    component.getMetric()
                                                             .getName(),
                                                    component.getName() );
                List<MutableDoubleList> samples = this.diagrams.get( name );

                int componentCount = samples.size();

                for ( int i = 0; i < componentCount; i++ )
                {
                    MutableDoubleList nextSamples = samples.get( i );
                    double[] nextSampleArray = nextSamples.toArray();
                    double statisticValue = summaryStatistic.applyAsDouble( nextSampleArray );
                    component.setValues( i, statisticValue );
                }
            }
        }
    }

    /**
     * Sets the quantiles for the duration diagram statistics.
     * @param builders the builders to update
     * @param summaryStatistic the summary statistic
     */

    private void calculateDurationDiagramSummaryStatistics( List<DurationDiagramStatistic.Builder> builders,
                                                            SummaryStatisticFunction summaryStatistic )
    {
        Function<Duration[], Duration> durationStatistic =
                FunctionFactory.ofDurationFromUnivariateFunction( summaryStatistic );

        for ( DurationDiagramStatistic.Builder diagram : builders )
        {
            MetricName metricName = diagram.getMetric()
                                           .getName();

            Map<Instant, List<Duration>> samples = this.durationDiagrams.get( metricName );

            int componentCount = diagram.getStatisticsCount();

            for ( int i = 0; i < componentCount; i++ )
            {
                DurationDiagramStatistic.PairOfInstantAndDuration.Builder pair = diagram.getStatisticsBuilder( i );
                Instant instant = MessageFactory.parse( pair.getTime() );
                List<Duration> sample = samples.get( instant );

                // Filter any null values and sort. Null values can occur if the pairs were empty, for example,
                // and no duration errors were produced. Empty pairs can occur when slicing a realization by
                // threshold.
                sample = DURATION_FILTER.apply( sample );

                Duration[] durations = sample.toArray( new Duration[0] );

                Duration statisticValue = durationStatistic.apply( durations );
                com.google.protobuf.Duration statisticProto = MessageFactory.parse( statisticValue );
                pair.setDuration( statisticProto );
            }
        }
    }

    /**
     * Creates an instance.
     * @param summaryStatistics the summary statistics to calculate, required and not empty
     * @param filter an optional filter
     * @throws NullPointerException if the list of statistics is null
     * @throws IllegalArgumentException if the list of statistics is empty
     */
    private SummaryStatisticsCalculator( List<SummaryStatisticFunction> summaryStatistics,
                                         Predicate<Statistics> filter )
    {
        Objects.requireNonNull( summaryStatistics );

        if ( Objects.isNull( filter ) )
        {
            this.filter = in -> true;
        }
        else
        {
            this.filter = filter;
        }

        this.summaryStatistics = summaryStatistics;
        this.isComplete = new AtomicBoolean();
        this.statistics = new ArrayList<>();

        // Create the slots for the sample statistics
        this.doubleScores = new ConcurrentHashMap<>();
        this.diagrams = new ConcurrentHashMap<>();
        this.durationScores = new ConcurrentHashMap<>();
        this.durationDiagrams = new ConcurrentHashMap<>();
    }

    /**
     * Removes any statistics that do not support sampling uncertainty estimation.
     *
     * @param statistics the statistics to filter
     * @return the filtered statistics
     */

    private Statistics filter( Statistics statistics )
    {
        Statistics.Builder builder = statistics.toBuilder()
                                               .clearScores()
                                               .clearDurationScores()
                                               .clearDiagrams()
                                               .clearDurationDiagrams()
                                               .clearOneBoxPerPair()
                                               .clearOneBoxPerPool();

        // Filter scores
        List<DoubleScoreStatistic> scoreList =
                statistics.getScoresList()
                          .stream()
                          .filter( n -> MetricConstants.valueOf( n.getMetric()
                                                                  .getName()
                                                                  .name() )
                                                       .isSamplingUncertaintyAllowed() )
                          .toList();
        builder.addAllScores( scoreList );

        // Filter duration scores
        List<DurationScoreStatistic> durationScoreList =
                statistics.getDurationScoresList()
                          .stream()
                          .filter( n -> MetricConstants.valueOf( n.getMetric()
                                                                  .getName()
                                                                  .name() )
                                                       .isSamplingUncertaintyAllowed() )
                          .toList();
        builder.addAllDurationScores( durationScoreList );

        // Filter diagrams
        List<DiagramStatistic> diagramList =
                statistics.getDiagramsList()
                          .stream()
                          .filter( n -> MetricConstants.valueOf( n.getMetric()
                                                                  .getName()
                                                                  .name() )
                                                       .isSamplingUncertaintyAllowed() )
                          .toList();
        builder.addAllDiagrams( diagramList );

        // Filter duration diagrams
        List<DurationDiagramStatistic> durationDiagramList =
                statistics.getDurationDiagramsList()
                          .stream()
                          .filter( n -> MetricConstants.valueOf( n.getMetric()
                                                                  .getName()
                                                                  .name() )
                                                       .isSamplingUncertaintyAllowed() )
                          .toList();
        builder.addAllDurationDiagrams( durationDiagramList );

        // Filter box plots per pool
        List<BoxplotStatistic> boxplotPerPoolList =
                statistics.getOneBoxPerPoolList()
                          .stream()
                          .filter( n -> MetricConstants.valueOf( n.getMetric()
                                                                  .getName()
                                                                  .name() )
                                                       .isSamplingUncertaintyAllowed() )
                          .toList();
        builder.addAllOneBoxPerPool( boxplotPerPoolList );

        // Filter box plots per pair
        List<BoxplotStatistic> boxplotPerPairList =
                statistics.getOneBoxPerPairList()
                          .stream()
                          .filter( n -> MetricConstants.valueOf( n.getMetric()
                                                                  .getName()
                                                                  .name() )
                                                       .isSamplingUncertaintyAllowed() )
                          .toList();
        builder.addAllOneBoxPerPair( boxplotPerPairList );

        return builder.build();
    }

    /**
     * The fully qualified name of a score whose sample quantiles must be estimated.
     * @param metricName the metric name
     * @param metricComponentName the metric component name
     */
    private record DoubleScoreName( MetricName metricName,
                                    DoubleScoreMetric.DoubleScoreMetricComponent.ComponentName metricComponentName ) {}

    /**
     * The fully qualified name of a diagram whose sample quantiles must be estimated.
     * @param metricName the metric name
     * @param metricComponentName the metric component name
     * @param qualifier the qualifier to use when the same component name is repeated
     */
    private record DiagramName( MetricName metricName,
                                DiagramMetric.DiagramMetricComponent.DiagramComponentName metricComponentName,
                                String qualifier ) {}

    /**
     * The fully qualified name of a duration score whose sample quantiles must be estimated.
     * @param metricName the metric name
     * @param metricComponentName the metric component name
     */
    private record DurationScoreName( MetricName metricName,
                                      DurationScoreMetric.DurationScoreMetricComponent.ComponentName metricComponentName ) {}

}
