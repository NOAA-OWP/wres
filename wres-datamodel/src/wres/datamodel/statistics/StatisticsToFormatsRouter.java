package wres.datamodel.statistics;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DestinationType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.MetricConstants;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.Slicer;
import wres.statistics.generated.DiagramStatistic;
import wres.statistics.generated.BoxplotStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.DurationDiagramStatistic;

/**
 * <p>A consumer that routes a collection of statistics to inner consumers than deliver specific output formats. There 
 * is up to one inner consumer for each type of statistic and format. The format is represented as a 
 * {@link DestinationType}, which includes aggregate destinations, such as {@link DestinationType#GRAPHIC} and 
 * {@link DestinationType#NUMERIC}, as well as specific format destinations, such as {@link DestinationType#PNG}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class StatisticsToFormatsRouter implements Function<Collection<Statistics>, Set<Path>>
{
    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( StatisticsToFormatsRouter.class );

    /**
     * Null output error string
     */

    private static final String NULL_OUTPUT_STRING = "Specify non-null outputs for product generation.";

    /**
     * Store of consumers for processing {@link DoubleScoreStatisticOuter} by {@link DestinationType} format.
     */

    private final Map<DestinationType, Function<List<DoubleScoreStatisticOuter>, Set<Path>>> doubleScoreConsumers =
            new EnumMap<>( DestinationType.class );

    /**
     * Store of consumers for processing {@link DurationScoreStatisticOuter} by {@link DestinationType} format.
     */

    private final Map<DestinationType, Function<List<DurationScoreStatisticOuter>, Set<Path>>> durationScoreConsumers =
            new EnumMap<>( DestinationType.class );

    /**
     * Store of consumers for processing {@link DiagramStatisticOuter} by {@link DestinationType} format.
     */

    private final Map<DestinationType, Function<List<DiagramStatisticOuter>, Set<Path>>> diagramConsumers =
            new EnumMap<>( DestinationType.class );

    /**
     * Store of consumers for processing {@link BoxplotStatisticOuter} by {@link DestinationType} format. The plots
     * contain one box per pair.
     */

    private final Map<DestinationType, Function<List<BoxplotStatisticOuter>, Set<Path>>> boxplotConsumersPerPair =
            new EnumMap<>( DestinationType.class );

    /**
     * Store of consumers for processing {@link BoxplotStatisticOuter} by {@link DestinationType} format. The plots
     * contain one box per pool.
     */

    private final Map<DestinationType, Function<List<BoxplotStatisticOuter>, Set<Path>>> boxplotConsumersPerPool =
            new EnumMap<>( DestinationType.class );

    /**
     * Store of consumers for processing {@link DiagramDiagramStatisticOuter} by {@link DestinationType} format.
     */

    private final Map<DestinationType, Function<List<DurationDiagramStatisticOuter>, Set<Path>>> durationDiagramConsumers =
            new EnumMap<>( DestinationType.class );

    /**
     * Store of consumers for processing {@link Statistics} by {@link DestinationType} format.
     */

    private final Map<DestinationType, Function<Statistics, Set<Path>>> allStatisticsConsumers =
            new EnumMap<>( DestinationType.class );

    /**
     * A map of output formats for which specific metrics should not be written. 
     */

    private final Map<DestinationType, Set<MetricConstants>> suppressTheseDestinationsForTheseMetrics;

    /**
     * The evaluation description.
     */

    private final Evaluation evaluationDescription;

    /**
     * Builder.
     * @author james.brown@hydrosolved.com
     */
    public static class Builder
    {

        /**
         * The evaluation description.
         */

        private Evaluation evaluationDescription;

        /**
         * Store of consumers for processing {@link DoubleScoreStatisticOuter} by {@link DestinationType} format.
         */

        private final Map<DestinationType, Function<List<DoubleScoreStatisticOuter>, Set<Path>>> doubleScoreConsumers =
                new EnumMap<>( DestinationType.class );

        /**
         * Store of consumers for processing {@link DurationScoreStatisticOuter} by {@link DestinationType} format.
         */

        private final Map<DestinationType, Function<List<DurationScoreStatisticOuter>, Set<Path>>> durationScoreConsumers =
                new EnumMap<>( DestinationType.class );

        /**
         * Store of consumers for processing {@link DiagramStatisticOuter} by {@link DestinationType} format.
         */

        private final Map<DestinationType, Function<List<DiagramStatisticOuter>, Set<Path>>> diagramConsumers =
                new EnumMap<>( DestinationType.class );

        /**
         * Store of consumers for processing {@link BoxplotStatisticOuter} by {@link DestinationType} format. The plots
         * contain one box per pair.
         */

        private final Map<DestinationType, Function<List<BoxplotStatisticOuter>, Set<Path>>> boxplotConsumersPerPair =
                new EnumMap<>( DestinationType.class );

        /**
         * Store of consumers for processing {@link BoxplotStatisticOuter} by {@link DestinationType} format. The plots
         * contain one box per pool.
         */

        private final Map<DestinationType, Function<List<BoxplotStatisticOuter>, Set<Path>>> boxplotConsumersPerPool =
                new EnumMap<>( DestinationType.class );

        /**
         * Store of consumers for processing {@link DiagramDiagramStatisticOuter} by {@link DestinationType} format.
         */

        private final Map<DestinationType, Function<List<DurationDiagramStatisticOuter>, Set<Path>>> durationDiagramConsumers =
                new EnumMap<>( DestinationType.class );

        /**
         * Store of consumers for processing {@link Statistics} by {@link DestinationType} format.
         */

        private final Map<DestinationType, Function<Statistics, Set<Path>>> allStatisticsConsumers =
                new EnumMap<>( DestinationType.class );

        /**
         * Sets the evaluation description.
         * @param evaluationDescription the evaluation description
         * @return the builder
         */
        public Builder setEvaluationDescription( Evaluation evaluationDescription )
        {
            this.evaluationDescription = evaluationDescription;
            return this;
        }

        /**
         * Adds a double score consumer to the builder for a given destination type.
         * @param destination the destination type
         * @param consumer the consumer
         * @return the builder
         * @throws NullPointerException if any input is null
         */
        public Builder addDoubleScoreConsumer( DestinationType destination,
                                               Function<List<DoubleScoreStatisticOuter>, Set<Path>> consumer )
        {
            Objects.requireNonNull( destination );
            Objects.requireNonNull( consumer );

            this.doubleScoreConsumers.put( destination, consumer );
            return this;
        }

        /**
         * Adds a duration score consumer to the builder for a given destination type.
         * @param destination the destination type
         * @param consumer the consumer
         * @return the builder
         * @throws NullPointerException if any input is null
         */
        public Builder addDurationScoreConsumer( DestinationType destination,
                                                 Function<List<DurationScoreStatisticOuter>, Set<Path>> consumer )
        {
            Objects.requireNonNull( destination );
            Objects.requireNonNull( consumer );

            this.durationScoreConsumers.put( destination, consumer );
            return this;
        }

        /**
         * Adds a diagram consumer to the builder for a given destination type.
         * @param destination the destination type
         * @param consumer the consumer
         * @return the builder
         * @throws NullPointerException if any input is null
         */
        public Builder addDiagramConsumer( DestinationType destination,
                                           Function<List<DiagramStatisticOuter>, Set<Path>> consumer )
        {
            Objects.requireNonNull( destination );
            Objects.requireNonNull( consumer );

            this.diagramConsumers.put( destination, consumer );
            return this;
        }

        /**
         * Adds a box plot per pair consumer to the builder for a given destination type.
         * @param destination the destination type
         * @param consumer the consumer
         * @return the builder
         * @throws NullPointerException if any input is null
         */
        public Builder addBoxplotConsumerPerPair( DestinationType destination,
                                                  Function<List<BoxplotStatisticOuter>, Set<Path>> consumer )
        {
            Objects.requireNonNull( destination );
            Objects.requireNonNull( consumer );

            this.boxplotConsumersPerPair.put( destination, consumer );
            return this;
        }

        /**
         * Adds a box plot per pool consumer to the builder for a given destination type.
         * @param destination the destination type
         * @param consumer the consumer
         * @return the builder
         * @throws NullPointerException if any input is null
         */
        public Builder addBoxplotConsumerPerPool( DestinationType destination,
                                                  Function<List<BoxplotStatisticOuter>, Set<Path>> consumer )
        {
            Objects.requireNonNull( destination );
            Objects.requireNonNull( consumer );

            this.boxplotConsumersPerPool.put( destination, consumer );
            return this;
        }

        /**
         * Adds a duration diagram consumer to the builder for a given destination type.
         * @param destination the destination type
         * @param consumer the consumer
         * @return the builder
         * @throws NullPointerException if any input is null
         */
        public Builder addDurationDiagramConsumer( DestinationType destination,
                                                   Function<List<DurationDiagramStatisticOuter>, Set<Path>> consumer )
        {
            Objects.requireNonNull( destination );
            Objects.requireNonNull( consumer );

            this.durationDiagramConsumers.put( destination, consumer );
            return this;
        }

        /**
         * Adds a consumer for all types of statistics to the builder for a given destination type.
         * @param destination the destination type
         * @param consumer the consumer
         * @return the builder
         * @throws NullPointerException if any input is null
         */
        public Builder addStatisticsConsumer( DestinationType destination,
                                              Function<Statistics, Set<Path>> consumer )
        {
            Objects.requireNonNull( destination );
            Objects.requireNonNull( consumer );

            this.allStatisticsConsumers.put( destination, consumer );
            return this;
        }

        /**
         * Builds an instance.
         * @return an instance
         */
        public StatisticsToFormatsRouter build()
        {
            return new StatisticsToFormatsRouter( this );
        }

    }

    /**
     * Produces output for each type available in the input.
     * 
     * @param statistics the list of statistics
     * @return the paths written
     * @throws StatisticsToFormatsRoutingException if consumption fails for any reason
     */

    @Override
    public Set<Path> apply( Collection<Statistics> statistics )
    {
        if ( Objects.isNull( statistics ) )
        {
            throw new StatisticsToFormatsRoutingException( "Cannot consumer null statistics." );
        }

        Set<Path> paths = new HashSet<>();

        try
        {
            // Split the statistics into two groups as there may be separate statistics for a baseline
            Function<? super Statistics, ? extends LeftOrRightOrBaseline> classifier = statistic -> {
                if ( !statistic.hasPool() && statistic.hasBaselinePool() )
                {
                    return LeftOrRightOrBaseline.BASELINE;
                }

                return LeftOrRightOrBaseline.RIGHT;
            };

            Map<LeftOrRightOrBaseline, List<Statistics>> groups =
                    statistics.stream()
                              .collect( Collectors.groupingBy( classifier ) );

            // Iterate the types
            for ( Map.Entry<LeftOrRightOrBaseline, List<Statistics>> nextEntry : groups.entrySet() )
            {
                LeftOrRightOrBaseline key = nextEntry.getKey();
                List<Statistics> value = nextEntry.getValue();
                Set<Path> innerPaths = this.accept( value, key == LeftOrRightOrBaseline.BASELINE );
                paths.addAll( innerPaths );
            }
        }
        // Wrap with some context
        catch ( RuntimeException e )
        {
            throw new StatisticsToFormatsRoutingException( "While routing evaluation statistics for consumption.", e );
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Accept some statistics for consumption.
     * @param statistics the statistics
     * @param isBaselinePool is true if the statistics refer to a baseline pool (when generating separate statistics 
     *            for both a main pool and baseline pool).
     * @return the paths written
     * @throws NullPointerException if the statistics are null
     */

    private Set<Path> accept( Collection<Statistics> statistics, boolean isBaselinePool )
    {
        Objects.requireNonNull( statistics );

        Set<Path> paths = new HashSet<>();

        // Supplies the pool metadata from either the baseline pool or the main pool
        Function<Statistics, Pool> poolSupplier = statistic -> {
            if ( isBaselinePool )
            {
                return statistic.getBaselinePool();
            }
            return statistic.getPool();
        };

        // Diagram output available
        if ( statistics.stream().anyMatch( next -> next.getDiagramsCount() > 0 ) )
        {
            List<DiagramStatisticOuter> wrapped = this.getWrappedAndSortedStatistics( statistics,
                                                                                      this.getDiagramMapper( poolSupplier ) );
            Set<Path> innerPaths = this.processDiagramOutputs( wrapped );
            paths.addAll( innerPaths );
        }

        // Box-plot output available per pair
        if ( statistics.stream().anyMatch( next -> next.getOneBoxPerPairCount() > 0 ) )
        {
            Function<Statistics, List<BoxplotStatistic>> supplier = Statistics::getOneBoxPerPairList;
            List<BoxplotStatisticOuter> wrapped = this.getWrappedAndSortedStatistics( statistics,
                                                                                      this.getBoxplotMapper( supplier,
                                                                                                             poolSupplier ) );
            Set<Path> innerPaths = this.processBoxPlotOutputsPerPair( wrapped );
            paths.addAll( innerPaths );
        }

        // Box-plot output available per pool
        if ( statistics.stream().anyMatch( next -> next.getOneBoxPerPoolCount() > 0 ) )
        {
            Function<Statistics, List<BoxplotStatistic>> supplier = Statistics::getOneBoxPerPoolList;
            List<BoxplotStatisticOuter> wrapped = this.getWrappedAndSortedStatistics( statistics,
                                                                                      this.getBoxplotMapper( supplier,
                                                                                                             poolSupplier ) );

            Set<Path> innerPaths = this.processBoxPlotOutputsPerPool( wrapped );
            paths.addAll( innerPaths );
        }

        // Ordinary scores available
        if ( statistics.stream().anyMatch( next -> next.getScoresCount() > 0 ) )
        {
            List<DoubleScoreStatisticOuter> wrapped = this.getWrappedAndSortedStatistics( statistics,
                                                                                          this.getDoubleScoreMapper( poolSupplier ) );

            Set<Path> innerPaths = this.processDoubleScoreOutputs( wrapped );
            paths.addAll( innerPaths );
        }

        // Duration scores available
        if ( statistics.stream().anyMatch( next -> next.getDurationScoresCount() > 0 ) )
        {
            List<DurationScoreStatisticOuter> wrapped = this.getWrappedAndSortedStatistics( statistics,
                                                                                            this.getDurationScoreMapper( poolSupplier ) );

            Set<Path> innerPaths = this.processDurationScoreOutputs( wrapped );
            paths.addAll( innerPaths );
        }

        // Duration diagrams available
        if ( statistics.stream().anyMatch( next -> next.getDurationDiagramsCount() > 0 ) )
        {
            List<DurationDiagramStatisticOuter> wrapped = this.getWrappedAndSortedStatistics( statistics,
                                                                                              this.getDurationDiagramMapper( poolSupplier ) );

            Set<Path> innerPaths = this.processDurationDiagramStatistic( wrapped );
            paths.addAll( innerPaths );
        }

        // Consumers of all statistics
        if ( !this.allStatisticsConsumers.isEmpty() )
        {
            Set<Path> innerPaths = this.processMultiStatistics( statistics );
            paths.addAll( innerPaths );
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * <p>Returns wrapped statistics from unwrapped statistics. Additionally sort the statistics by time window and 
     * threshold. a
     *
     * <p>Some of the existing consumers, notably the PNG consumers, currently assume that the statistics are sorted. 
     * The consumers should probably not make this assumption, else it should be clear in the API.
     * 
     * @param <W> the wrapped statistic type
     * @param statistics the statistics
     * @param mapper the supplier of unwrapped statistics from a bucket of many types
     * @return the wrapped and sorted statistics 
     */

    private <W extends Statistic<?>> List<W> getWrappedAndSortedStatistics( Collection<Statistics> statistics,
                                                                            Function<Statistics, List<W>> mapper )
    {
        List<W> wrapped = statistics.stream()
                                    .map( mapper )
                                    .flatMap( List::stream )
                                    .collect( Collectors.toUnmodifiableList() );

        return Slicer.sortByTimeWindowAndThreshold( wrapped );
    }

    /**
     * Returns a mapper function that maps between raw statistics and wrapped diagrams.
     * 
     * @param poolSupplier the pool supplier
     * @return the mapper
     */

    private Function<Statistics, List<DiagramStatisticOuter>>
            getDiagramMapper( Function<Statistics, Pool> poolSupplier )
    {
        return someStats -> {
            List<DiagramStatistic> diagrams = someStats.getDiagramsList();
            Function<DiagramStatistic, DiagramStatisticOuter> innerMapper =
                    nextDiagram -> DiagramStatisticOuter.of( nextDiagram,
                                                             SampleMetadata.of( this.getEvaluationDescription(),
                                                                                poolSupplier.apply( someStats ) ) );
            return diagrams.stream()
                           .map( innerMapper )
                           .collect( Collectors.toUnmodifiableList() );
        };
    }

    /**
     * Returns a mapper function that maps between raw statistics and wrapped boxplot statistics.
     * 
     * @param supplier the supplier of boxplot statistics
     * @param poolSupplier the pool supplier
     * @return the mapper
     */

    private Function<Statistics, List<BoxplotStatisticOuter>>
            getBoxplotMapper( Function<Statistics, List<BoxplotStatistic>> supplier,
                              Function<Statistics, Pool> poolSupplier )
    {
        return someStats -> {
            List<BoxplotStatistic> boxes = supplier.apply( someStats );
            Function<BoxplotStatistic, BoxplotStatisticOuter> innerMapper =
                    nextBoxplot -> BoxplotStatisticOuter.of( nextBoxplot,
                                                             SampleMetadata.of( this.getEvaluationDescription(),
                                                                                poolSupplier.apply( someStats ) ) );
            return boxes.stream()
                        .map( innerMapper )
                        .collect( Collectors.toUnmodifiableList() );
        };
    }

    /**
     * Returns a mapper function that maps between raw statistics and wrapped double scores.
     * 
     * @param poolSupplier the pool supplier
     * @return the mapper
     */

    private Function<Statistics, List<DoubleScoreStatisticOuter>>
            getDoubleScoreMapper( Function<Statistics, Pool> poolSupplier )
    {
        return someStats -> {
            List<DoubleScoreStatistic> scores = someStats.getScoresList();
            Function<DoubleScoreStatistic, DoubleScoreStatisticOuter> innerMapper =
                    nextScore -> DoubleScoreStatisticOuter.of( nextScore,
                                                               SampleMetadata.of( this.getEvaluationDescription(),
                                                                                  poolSupplier.apply( someStats ) ) );
            return scores.stream()
                         .map( innerMapper )
                         .collect( Collectors.toUnmodifiableList() );
        };
    }

    /**
     * Returns a mapper function that maps between raw statistics and wrapped duration scores.
     * 
     * @param poolSupplier the pool supplier
     * @return the mapper
     */

    private Function<Statistics, List<DurationScoreStatisticOuter>>
            getDurationScoreMapper( Function<Statistics, Pool> poolSupplier )
    {
        return someStats -> {
            List<DurationScoreStatistic> scores = someStats.getDurationScoresList();
            Function<DurationScoreStatistic, DurationScoreStatisticOuter> innerMapper =
                    nextScore -> DurationScoreStatisticOuter.of( nextScore,
                                                                 SampleMetadata.of( this.getEvaluationDescription(),
                                                                                    poolSupplier.apply( someStats ) ) );
            return scores.stream()
                         .map( innerMapper )
                         .collect( Collectors.toUnmodifiableList() );
        };
    }

    /**
     * Returns a mapper function that maps between raw statistics and wrapped duration scores.
     * 
     * @param poolSupplier the pool supplier
     * @return the mapper
     */

    private Function<Statistics, List<DurationDiagramStatisticOuter>>
            getDurationDiagramMapper( Function<Statistics, Pool> poolSupplier )
    {
        return someStats -> {
            List<DurationDiagramStatistic> diagrams = someStats.getDurationDiagramsList();
            Function<DurationDiagramStatistic, DurationDiagramStatisticOuter> innerMapper =
                    nextDiagram -> DurationDiagramStatisticOuter.of( nextDiagram,
                                                                     SampleMetadata.of( this.getEvaluationDescription(),
                                                                                        poolSupplier.apply( someStats ) ) );
            return diagrams.stream()
                           .map( innerMapper )
                           .collect( Collectors.toUnmodifiableList() );
        };
    }

    /**
     * Processes {@link DiagramStatisticOuter}.
     * 
     * @param outputs the outputs to consume
     * @return the paths written
     * @throws NullPointerException if the input is null
     */

    private Set<Path> processDiagramOutputs( List<DiagramStatisticOuter> outputs )
    {
        Objects.requireNonNull( outputs, NULL_OUTPUT_STRING );

        Set<Path> paths = new HashSet<>();

        LOGGER.debug( "Routing {} diagram statistics to {} format writers with destination types {}.",
                      outputs.size(),
                      this.diagramConsumers.size(),
                      this.diagramConsumers.keySet() );

        // Iterate through the consumers
        for ( Entry<DestinationType, Function<List<DiagramStatisticOuter>, Set<Path>>> next : this.diagramConsumers.entrySet() )
        {
            this.log( outputs, next.getKey(), true );

            List<DiagramStatisticOuter> filtered = this.getFilteredStatisticsForThisDestinationType( outputs,
                                                                                                     next.getKey() );

            // Consume the output
            Set<Path> innerPaths = next.getValue().apply( filtered );
            paths.addAll( innerPaths );

            this.log( outputs, next.getKey(), false );
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Processes {@link BoxplotStatisticOuter} per pair.
     * 
     * @param outputs the output to consume
     * @return the paths written
     * @throws NullPointerException if the input is null
     */

    private Set<Path> processBoxPlotOutputsPerPair( List<BoxplotStatisticOuter> outputs )
    {
        Objects.requireNonNull( outputs, NULL_OUTPUT_STRING );

        Set<Path> paths = new HashSet<>();

        LOGGER.debug( "Routing {} box plot statistics (per pair type) to {} format writers with destination types {}.",
                      outputs.size(),
                      this.boxplotConsumersPerPair.size(),
                      this.boxplotConsumersPerPair.keySet() );

        // Iterate through the consumers
        for ( Entry<DestinationType, Function<List<BoxplotStatisticOuter>, Set<Path>>> next : this.boxplotConsumersPerPair.entrySet() )
        {
            this.log( outputs, next.getKey(), true );

            List<BoxplotStatisticOuter> filtered = this.getFilteredStatisticsForThisDestinationType( outputs,
                                                                                                     next.getKey() );

            // Consume the output
            Set<Path> innerPaths = next.getValue().apply( filtered );
            paths.addAll( innerPaths );

            this.log( outputs, next.getKey(), false );
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Processes {@link BoxplotStatisticOuter} per pool.
     * 
     * @param outputs the output to consume
     * @return the paths written
     * @throws NullPointerException if the input is null
     */

    private Set<Path> processBoxPlotOutputsPerPool( List<BoxplotStatisticOuter> outputs )
    {
        Objects.requireNonNull( outputs, NULL_OUTPUT_STRING );

        Set<Path> paths = new HashSet<>();

        LOGGER.debug( "Routing {} box plot statistics (per pool type) to {} format writers with destination types {}.",
                      outputs.size(),
                      this.boxplotConsumersPerPool.size(),
                      this.boxplotConsumersPerPool.keySet() );

        // Iterate through the consumers
        for ( Entry<DestinationType, Function<List<BoxplotStatisticOuter>, Set<Path>>> next : this.boxplotConsumersPerPool.entrySet() )
        {
            this.log( outputs, next.getKey(), true );

            List<BoxplotStatisticOuter> filtered = this.getFilteredStatisticsForThisDestinationType( outputs,
                                                                                                     next.getKey() );

            // Consume the output
            Set<Path> innerPaths = next.getValue().apply( filtered );
            paths.addAll( innerPaths );

            this.log( outputs, next.getKey(), false );
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Processes {@link DoubleScoreStatisticOuter}.
     * 
     * @param outputs the output to consume
     * @return the paths written
     * @throws NullPointerException if the input is null
     */

    private Set<Path> processDoubleScoreOutputs( List<DoubleScoreStatisticOuter> outputs )
    {
        Objects.requireNonNull( outputs, NULL_OUTPUT_STRING );

        Set<Path> paths = new HashSet<>();

        LOGGER.debug( "Routing {} double score statistics to {} format writers with destination types {}.",
                      outputs.size(),
                      this.doubleScoreConsumers.size(),
                      this.doubleScoreConsumers.keySet() );

        // Iterate through the consumers
        for ( Entry<DestinationType, Function<List<DoubleScoreStatisticOuter>, Set<Path>>> next : this.doubleScoreConsumers.entrySet() )
        {
            this.log( outputs, next.getKey(), true );

            List<DoubleScoreStatisticOuter> filtered = this.getFilteredStatisticsForThisDestinationType( outputs,
                                                                                                         next.getKey() );

            // Consume the output
            Set<Path> innerPaths = next.getValue().apply( filtered );
            paths.addAll( innerPaths );

            this.log( outputs, next.getKey(), false );
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Processes {@link DurationScoreStatisticOuter}.
     * 
     * @param outputs the output to consume
     * @return the paths written
     * @throws NullPointerException if the input is null
     */

    private Set<Path> processDurationScoreOutputs( List<DurationScoreStatisticOuter> outputs )
    {
        Objects.requireNonNull( outputs, NULL_OUTPUT_STRING );

        Set<Path> paths = new HashSet<>();

        LOGGER.debug( "Routing {} duration score statistics to {} format writers with destination types {}.",
                      outputs.size(),
                      this.durationScoreConsumers.size(),
                      this.durationScoreConsumers.keySet() );

        // Iterate through the consumers
        for ( Entry<DestinationType, Function<List<DurationScoreStatisticOuter>, Set<Path>>> next : this.durationScoreConsumers.entrySet() )
        {
            this.log( outputs, next.getKey(), true );

            List<DurationScoreStatisticOuter> filtered = this.getFilteredStatisticsForThisDestinationType( outputs,
                                                                                                           next.getKey() );

            // Consume the output
            Set<Path> innerPaths = next.getValue().apply( filtered );
            paths.addAll( innerPaths );

            this.log( outputs, next.getKey(), false );
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Processes {@link DiagramDiagramStatisticOuter}.
     * 
     * @param outputs the output to consume
     * @return the paths written
     * @throws NullPointerException if the input is null
     */

    private Set<Path> processDurationDiagramStatistic( List<DurationDiagramStatisticOuter> outputs )
    {
        Objects.requireNonNull( outputs, NULL_OUTPUT_STRING );

        Set<Path> paths = new HashSet<>();

        LOGGER.debug( "Routing {} duration diagram statistics to {} format writers with destination types {}.",
                      outputs.size(),
                      this.durationDiagramConsumers.size(),
                      this.durationDiagramConsumers.keySet() );

        // Iterate through the consumers
        for ( Entry<DestinationType, Function<List<DurationDiagramStatisticOuter>, Set<Path>>> next : this.durationDiagramConsumers.entrySet() )
        {
            this.log( outputs, next.getKey(), true );

            List<DurationDiagramStatisticOuter> filtered =
                    this.getFilteredStatisticsForThisDestinationType( outputs, next.getKey() );

            // Consume the output
            Set<Path> innerPaths = next.getValue().apply( filtered );
            paths.addAll( innerPaths );

            this.log( outputs, next.getKey(), false );
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Processes {@link Statistics} for consumers of all statistics.
     * 
     * @param statistics the statistics to consume
     * @return the paths mutated
     * @throws NullPointerException if the input is null
     */

    private Set<Path> processMultiStatistics( Collection<Statistics> statistics )
    {
        Objects.requireNonNull( statistics, NULL_OUTPUT_STRING );

        Set<Path> paths = new HashSet<>();

        LOGGER.debug( "Routing {} mixed packets of statistics to {} format writers with destination types {}.",
                      statistics.size(),
                      this.allStatisticsConsumers.size(),
                      this.allStatisticsConsumers.keySet() );

        // Iterate through the consumers
        for ( Entry<DestinationType, Function<Statistics, Set<Path>>> next : this.allStatisticsConsumers.entrySet() )
        {
            for ( Statistics nextStatistics : statistics )
            {
                Set<Path> innerPaths = next.getValue().apply( nextStatistics );
                paths.addAll( innerPaths );
            }
        }

        return Collections.unmodifiableSet( paths );
    }

    /**
     * Logs the status of product generation.
     * 
     * @param <T> the output type
     * @param output the output
     * @param type the output type
     * @param startOfProcess is true to log the start, false to log the end
     */

    private <T extends Statistic<?>> void
            log( List<T> output, DestinationType type, boolean startOfProcess )
    {

        String positionString = "Completed ";
        if ( startOfProcess )
        {
            positionString = "Started ";
        }

        if ( !output.isEmpty() )
        {
            LOGGER.debug( "{} processing of result type '{}' for '{}' "
                          + "at time window {}.",
                          positionString,
                          type,
                          output.get( 0 )
                                .getMetadata()
                                .getPool()
                                .getGeometryTuples( 0 )
                                .getLeft()
                                .getName(),
                          output.get( 0 )
                                .getMetadata()
                                .getTimeWindow() );
        }
        else
        {
            LOGGER.debug( "{} processing of result type '{}' for unknown data at "
                          + "unknown time window.",
                          positionString,
                          type );
        }
    }

    /**
     * Filters the input statistics for the prescribed destination type relative to the output types that should be 
     * suppressed for particular statistics. See {@link #getSuppressTheseMetricsForThisDestinationType(DestinationType).
     * 
     * @param statistics the statistics to filter
     * @param destinationType the destination type by which to filter
     * @return a filtered list of statistics, omitting those to be suppressed for the prescribed destination type
     * @throws NullPointerException if any input is null
     */

    private <T extends Statistic<?>> List<T> getFilteredStatisticsForThisDestinationType( List<T> statistics,
                                                                                          DestinationType destinationType )
    {
        Objects.requireNonNull( statistics );

        Objects.requireNonNull( destinationType );

        Set<MetricConstants> suppress = this.getSuppressTheseMetricsForThisDestinationType( destinationType );

        // Filter suppressed types
        if ( Objects.nonNull( suppress ) )
        {
            return Slicer.filter( statistics, next -> !suppress.contains( next.getMetricName() ) );
        }

        // Nothing filtered
        return statistics;
    }

    /**
     * Returns the metrics that should be suppressed for the prescribed destination.
     * 
     * @return the map of destination types to statistics for suppression
     */

    private Set<MetricConstants> getSuppressTheseMetricsForThisDestinationType( DestinationType destinationType )
    {
        return this.suppressTheseDestinationsForTheseMetrics.get( destinationType );
    }

    /**
     * @return the evaluation description.
     */

    private Evaluation getEvaluationDescription()
    {
        return this.evaluationDescription;
    }

    /**
     * @param outputs the outputs description to check
     * @return a set of metrics to suppress for each destination type.
     */

    private Map<DestinationType, Set<MetricConstants>> getMetricsToSuppressForEachDestination( Outputs outputs )
    {
        Objects.requireNonNull( outputs );

        Map<DestinationType, Set<MetricConstants>> returnMe = new EnumMap<>( DestinationType.class );

        // PNG
        if ( outputs.hasPng() && outputs.getPng().hasOptions() )
        {
            List<MetricName> ignore = outputs.getPng().getOptions().getIgnoreList();
            Set<MetricConstants> mapped = ignore.stream()
                                                .map( next -> MetricConstants.valueOf( next.name() ) )
                                                .collect( Collectors.toUnmodifiableSet() );
            returnMe.put( DestinationType.PNG, mapped );
            returnMe.put( DestinationType.GRAPHIC, mapped );
        }
        // SVG
        if ( outputs.hasSvg() && outputs.getSvg().hasOptions() )
        {
            List<MetricName> ignore = outputs.getSvg().getOptions().getIgnoreList();
            Set<MetricConstants> mapped = ignore.stream()
                                                .map( next -> MetricConstants.valueOf( next.name() ) )
                                                .collect( Collectors.toUnmodifiableSet() );
            returnMe.put( DestinationType.SVG, mapped );
        }

        return Collections.unmodifiableMap( returnMe );
    }

    /**
     * Build a product processor that writes conditionally.
     * 
     * @param builder the builder
     * @throws WresProcessingException if the project is invalid for writing
     * @throws NullPointerException if any required input is null
     */

    private StatisticsToFormatsRouter( Builder builder )
    {
        // Set then validate
        this.evaluationDescription = builder.evaluationDescription;

        Objects.requireNonNull( this.evaluationDescription );

        Outputs outputsDescription = this.getEvaluationDescription()
                                         .getOutputs();

        this.suppressTheseDestinationsForTheseMetrics =
                this.getMetricsToSuppressForEachDestination( outputsDescription );

        this.doubleScoreConsumers.putAll( builder.doubleScoreConsumers );
        this.durationScoreConsumers.putAll( builder.durationScoreConsumers );
        this.diagramConsumers.putAll( builder.diagramConsumers );
        this.durationDiagramConsumers.putAll( builder.durationDiagramConsumers );
        this.boxplotConsumersPerPair.putAll( builder.boxplotConsumersPerPair );
        this.boxplotConsumersPerPool.putAll( builder.boxplotConsumersPerPool );
        this.allStatisticsConsumers.putAll( builder.allStatisticsConsumers );
    }

    /**
     * Exception to throw when statistics cannot be routed.
     */

    private static class StatisticsToFormatsRoutingException extends RuntimeException
    {

        private static final long serialVersionUID = -7654568836842809914L;

        /**
         * Constructs a {@link StatisticsToFormatsRoutingException} with the specified message.
         * 
         * @param message the message.
         */

        public StatisticsToFormatsRoutingException( String message )
        {
            super( message );
        }

        /**
         * Builds a {@link StatisticsToFormatsRoutingException} with the specified message.
         * 
         * @param message the message.
         * @param cause the cause of the exception
         */

        public StatisticsToFormatsRoutingException( String message, Throwable cause )
        {
            super( message, cause );
        }
    }

}
