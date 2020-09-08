package wres.vis.client;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.ProjectConfigException;
import wres.config.generated.DestinationType;
import wres.config.generated.LeftOrRightOrBaseline;
import wres.datamodel.MetricConstants;
import wres.datamodel.MetricConstants.StatisticType;
import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.Slicer;
import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.DiagramStatisticOuter;
import wres.datamodel.statistics.DurationDiagramStatisticOuter;
import wres.datamodel.statistics.Statistic;
import wres.events.ConsumerException;
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
import wres.vis.writing.BoxPlotGraphicsWriter;
import wres.vis.writing.DiagramGraphicsWriter;
import wres.vis.writing.DoubleScoreGraphicsWriter;
import wres.vis.writing.DurationDiagramGraphicsWriter;
import wres.vis.writing.DurationScoreGraphicsWriter;
import wres.vis.writing.GraphicsWriteException;

/**
 * <p>Consumes statistics, creates graphics, and writes graphics formats from them.
 * 
 * @author james.brown@hydrosolved.com
 */

class StatisticsConsumer implements Consumer<Collection<Statistics>>, Closeable, Supplier<Set<Path>>
{
    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( StatisticsConsumer.class );

    /**
     * Null output error string
     */

    private static final String NULL_OUTPUT_STRING = "Specify non-null outputs for product generation.";

    /**
     * Only writes when the condition is true.
     */

    private final BiPredicate<StatisticType, DestinationType> writeWhenTrue;

    /**
     * Store of consumers for processing {@link DoubleScoreStatisticOuter} by {@link DestinationType} format.
     */

    private final Map<DestinationType, Consumer<List<DoubleScoreStatisticOuter>>> doubleScoreConsumers =
            new EnumMap<>( DestinationType.class );

    /**
     * Store of consumers for processing {@link DurationScoreStatisticOuter} by {@link DestinationType} format.
     */

    private final Map<DestinationType, Consumer<List<DurationScoreStatisticOuter>>> durationScoreConsumers =
            new EnumMap<>( DestinationType.class );

    /**
     * Store of consumers for processing {@link DiagramStatisticOuter} by {@link DestinationType} format.
     */

    private final Map<DestinationType, Consumer<List<DiagramStatisticOuter>>> diagramConsumers =
            new EnumMap<>( DestinationType.class );

    /**
     * Store of consumers for processing {@link BoxplotStatisticOuter} by {@link DestinationType} format. The plots
     * contain one box per pair.
     */

    private final Map<DestinationType, Consumer<List<BoxplotStatisticOuter>>> boxPlotConsumersPerPair =
            new EnumMap<>( DestinationType.class );

    /**
     * Store of consumers for processing {@link BoxplotStatisticOuter} by {@link DestinationType} format. The plots
     * contain one box per pool.
     */

    private final Map<DestinationType, Consumer<List<BoxplotStatisticOuter>>> boxPlotConsumersPerPool =
            new EnumMap<>( DestinationType.class );

    /**
     * Store of consumers for processing {@link DiagramDiagramStatisticOuter} by {@link DestinationType} format.
     */

    private final Map<DestinationType, Consumer<List<DurationDiagramStatisticOuter>>> durationDiagramConsumers =
            new EnumMap<>( DestinationType.class );

    /**
     * A map of output formats for which specific metrics should not be written. 
     */

    private final Map<DestinationType, Set<MetricConstants>> suppressTheseDestinationsForTheseMetrics;

    /**
     * List of resources that ProductProcessor opened that it needs to close
     */
    private final List<Closeable> resourcesToClose;

    /**
     * List of potential writers that the ProductProcessor opened that supply
     * a list of paths that those writers actually ended up writing to.
     */
    private final List<Supplier<Set<Path>>> writersToPaths;

    /**
     * The evaluation description.
     */

    private final Evaluation evaluationDescription;

    /**
     * The output directory.
     */

    private final Path outputDirectory;

    final AtomicBoolean consumerBuilt = new AtomicBoolean();

    /**
     * Build the consumer.
     * 
     * @param evaluationDescription the evaluation description
     * @param writeWhenTrue the condition under which outputs should be written
     * @param outputDirectory the output directory
     * @throws NullPointerException if any of the inputs are null
     * @throws WresProcessingException if the project is invalid for writing
     */

    static StatisticsConsumer of( Evaluation evaluationDescription,
                                  BiPredicate<StatisticType, DestinationType> writeWhenTrue,
                                  Path outputDirectory )
    {
        return new StatisticsConsumer( evaluationDescription,
                                       writeWhenTrue,
                                       outputDirectory );
    }

    /**
     * Produces graphical and numerical output for each type available in the input.
     * 
     * @param statistics the list of statistics
     * @throws ConsumerException if consumption fails for any reason
     */

    @Override
    public void accept( Collection<Statistics> statistics )
    {
        if ( Objects.isNull( statistics ) )
        {
            throw new ConsumerException( "Cannot consumer null statistics." );
        }

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
            groups.forEach( ( ( a, b ) -> this.acceptInner( b, a == LeftOrRightOrBaseline.BASELINE ) ) );
        }
        // Better to throw a common type here as a JMS MessageListener is expected to handle all exceptions
        // and it is better to aggregate them into one type than to catch a generic java.lang.Exception in 
        // a MessageListener. It is possible that other types could occur, which could make the application
        // hang on failing to consume all expected messages. This only applies to internal consumers that 
        // can break the flow with exceptions. Eventually, all consumers will be external. 
        catch ( RuntimeException e )
        {
            throw new ConsumerException( "While consuming evaluation statistics.", e );
        }
    }

    /**
     * Accept some statistics for consumption.
     * @param statistics the statistics
     * @param isBaselinePool is true if the statistics refer to a baseline pool (when generating separate statistics 
     *            for both a main pool and baseline pool).
     * @throws NullPointerException if the statistics are null
     */

    public void acceptInner( Collection<Statistics> statistics, boolean isBaselinePool )
    {
        Objects.requireNonNull( statistics );

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
            this.processDiagramOutputs( wrapped );
        }

        // Box-plot output available per pair
        if ( statistics.stream().anyMatch( next -> next.getOneBoxPerPairCount() > 0 ) )
        {
            Function<Statistics, List<BoxplotStatistic>> supplier = Statistics::getOneBoxPerPairList;
            List<BoxplotStatisticOuter> wrapped = this.getWrappedAndSortedStatistics( statistics,
                                                                                      this.getBoxplotMapper( supplier,
                                                                                                             poolSupplier ) );
            this.processBoxPlotOutputsPerPair( wrapped );
        }

        // Box-plot output available per pool
        if ( statistics.stream().anyMatch( next -> next.getOneBoxPerPoolCount() > 0 ) )
        {
            Function<Statistics, List<BoxplotStatistic>> supplier = Statistics::getOneBoxPerPoolList;
            List<BoxplotStatisticOuter> wrapped = this.getWrappedAndSortedStatistics( statistics,
                                                                                      this.getBoxplotMapper( supplier,
                                                                                                             poolSupplier ) );

            this.processBoxPlotOutputsPerPool( wrapped );
        }

        // Ordinary scores available
        if ( statistics.stream().anyMatch( next -> next.getScoresCount() > 0 ) )
        {
            List<DoubleScoreStatisticOuter> wrapped = this.getWrappedAndSortedStatistics( statistics,
                                                                                          this.getDoubleScoreMapper( poolSupplier ) );

            this.processDoubleScoreOutputs( wrapped );
        }

        // Duration scores available
        if ( statistics.stream().anyMatch( next -> next.getDurationScoresCount() > 0 ) )
        {
            List<DurationScoreStatisticOuter> wrapped = this.getWrappedAndSortedStatistics( statistics,
                                                                                            this.getDurationScoreMapper( poolSupplier ) );

            this.processDurationScoreOutputs( wrapped );
        }

        // Duration diagrams available
        if ( statistics.stream().anyMatch( next -> next.getDurationDiagramsCount() > 0 ) )
        {
            List<DurationDiagramStatisticOuter> wrapped = this.getWrappedAndSortedStatistics( statistics,
                                                                                              this.getDurationDiagramMapper( poolSupplier ) );

            this.processDurationDiagramStatistic( wrapped );
        }

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
     * @return the paths to write.
     */

    private List<Supplier<Set<Path>>> getWritersToPaths()
    {
        return Collections.unmodifiableList( this.writersToPaths );
    }

    /**
     * @return paths actually written to by this processor so far.
     */
    @Override
    public Set<Path> get()
    {
        Set<Path> paths = new HashSet<>();

        for ( Supplier<Set<Path>> supplierOfPaths : this.getWritersToPaths() )
        {
            paths.addAll( supplierOfPaths.get() );
        }

        LOGGER.debug( "Returning paths from {} {}: {}", this.getClass().getName(), this, paths );
        return Collections.unmodifiableSet( paths );
    }

    /**
     * Builds a set of consumers for writing graphics to supplied destinations.
     * 
     * @param outputs a description of the outputs required
     * @param destinations the destinations
     */

    private void buildGraphicsConsumers( Outputs outputs )
    {
        Objects.requireNonNull( outputs );

        if ( outputs.hasPng() || outputs.hasSvg() )
        {
            this.buildGraphicsConsumersForEachStatisticType( outputs );
        }

        this.consumerBuilt.set( true );
    }

    /**
     * Adds a graphic consumer for each type of statistical output and for one format type.
     * @param outputs a description of the outputs required
     */

    private void buildGraphicsConsumersForEachStatisticType( Outputs outputs )
    {
        Path pathToWrite = this.getOutputDirectory();

        // Build the consumers conditionally
        if ( this.writeWhenTrue.test( StatisticType.DIAGRAM, DestinationType.GRAPHIC ) )
        {
            DiagramGraphicsWriter diagramWriter = DiagramGraphicsWriter.of( outputs,
                                                                            pathToWrite );
            this.diagramConsumers.put( DestinationType.GRAPHIC,
                                       diagramWriter );
            this.writersToPaths.add( diagramWriter );
        }

        if ( this.writeWhenTrue.test( StatisticType.BOXPLOT_PER_PAIR, DestinationType.GRAPHIC ) )
        {
            BoxPlotGraphicsWriter boxPlotWriter = BoxPlotGraphicsWriter.of( outputs,
                                                                            pathToWrite );
            this.boxPlotConsumersPerPair.put( DestinationType.GRAPHIC,
                                              boxPlotWriter );
            this.writersToPaths.add( boxPlotWriter );
        }

        if ( this.writeWhenTrue.test( StatisticType.BOXPLOT_PER_POOL, DestinationType.GRAPHIC ) )
        {
            BoxPlotGraphicsWriter boxPlotWriter = BoxPlotGraphicsWriter.of( outputs,
                                                                            pathToWrite );
            this.boxPlotConsumersPerPool.put( DestinationType.GRAPHIC,
                                              boxPlotWriter );
            this.writersToPaths.add( boxPlotWriter );
        }

        if ( this.writeWhenTrue.test( StatisticType.DURATION_DIAGRAM, DestinationType.GRAPHIC ) )
        {
            DurationDiagramGraphicsWriter pairedWriter = DurationDiagramGraphicsWriter.of( outputs,
                                                                                           pathToWrite );
            this.durationDiagramConsumers.put( DestinationType.GRAPHIC,
                                               pairedWriter );
            this.writersToPaths.add( pairedWriter );
        }

        if ( this.writeWhenTrue.test( StatisticType.DOUBLE_SCORE, DestinationType.GRAPHIC ) )
        {
            DoubleScoreGraphicsWriter doubleScoreWriter =
                    DoubleScoreGraphicsWriter.of( outputs,
                                                  pathToWrite );
            this.doubleScoreConsumers.put( DestinationType.GRAPHIC,
                                           doubleScoreWriter );
            this.writersToPaths.add( doubleScoreWriter );
        }

        if ( this.writeWhenTrue.test( StatisticType.DURATION_SCORE, DestinationType.GRAPHIC ) )
        {
            DurationScoreGraphicsWriter durationScoreWriter =
                    DurationScoreGraphicsWriter.of( outputs,
                                                    pathToWrite );
            this.durationScoreConsumers.put( DestinationType.GRAPHIC,
                                             durationScoreWriter );
            this.writersToPaths.add( durationScoreWriter );
        }
    }

    /**
     * Processes {@link DiagramStatisticOuter}.
     * 
     * @param outputs the outputs to consume
     * @throws NullPointerException if the input is null
     */

    private void processDiagramOutputs( List<DiagramStatisticOuter> outputs )
    {
        Objects.requireNonNull( outputs, NULL_OUTPUT_STRING );

        // Iterate through the consumers
        for ( Entry<DestinationType, Consumer<List<DiagramStatisticOuter>>> next : this.diagramConsumers.entrySet() )
        {
            // Consume conditionally
            if ( this.writeWhenTrue.test( StatisticType.DIAGRAM, next.getKey() ) )
            {
                log( outputs, next.getKey(), true );

                List<DiagramStatisticOuter> filtered = this.getFilteredStatisticsForThisDestinationType( outputs,
                                                                                                         next.getKey() );

                // Consume the output
                next.getValue().accept( filtered );

                log( outputs, next.getKey(), false );
            }
        }
    }

    /**
     * Processes {@link BoxplotStatisticOuter} per pair.
     * 
     * @param outputs the output to consume
     * @throws NullPointerException if the input is null
     */

    private void processBoxPlotOutputsPerPair( List<BoxplotStatisticOuter> outputs )
    {
        Objects.requireNonNull( outputs, NULL_OUTPUT_STRING );

        // Iterate through the consumers
        for ( Entry<DestinationType, Consumer<List<BoxplotStatisticOuter>>> next : this.boxPlotConsumersPerPair.entrySet() )
        {
            // Consume conditionally
            if ( this.writeWhenTrue.test( StatisticType.BOXPLOT_PER_PAIR, next.getKey() ) )
            {
                log( outputs, next.getKey(), true );

                List<BoxplotStatisticOuter> filtered = this.getFilteredStatisticsForThisDestinationType( outputs,
                                                                                                         next.getKey() );

                // Consume the output
                next.getValue().accept( filtered );

                log( outputs, next.getKey(), false );
            }
        }
    }

    /**
     * Processes {@link BoxplotStatisticOuter} per pool.
     * 
     * @param outputs the output to consume
     * @throws NullPointerException if the input is null
     */

    private void processBoxPlotOutputsPerPool( List<BoxplotStatisticOuter> outputs )
    {
        Objects.requireNonNull( outputs, NULL_OUTPUT_STRING );

        // Iterate through the consumers
        for ( Entry<DestinationType, Consumer<List<BoxplotStatisticOuter>>> next : this.boxPlotConsumersPerPool.entrySet() )
        {
            // Consume conditionally
            if ( this.writeWhenTrue.test( StatisticType.BOXPLOT_PER_POOL, next.getKey() ) )
            {
                log( outputs, next.getKey(), true );

                List<BoxplotStatisticOuter> filtered = this.getFilteredStatisticsForThisDestinationType( outputs,
                                                                                                         next.getKey() );

                // Consume the output
                next.getValue().accept( filtered );

                log( outputs, next.getKey(), false );
            }
        }
    }

    /**
     * Processes {@link DoubleScoreStatisticOuter}.
     * 
     * @param outputs the output to consume
     * @throws NullPointerException if the input is null
     */

    private void processDoubleScoreOutputs( List<DoubleScoreStatisticOuter> outputs )
    {
        Objects.requireNonNull( outputs, NULL_OUTPUT_STRING );

        // Iterate through the consumers
        for ( Entry<DestinationType, Consumer<List<DoubleScoreStatisticOuter>>> next : this.doubleScoreConsumers.entrySet() )
        {
            // Consume conditionally
            if ( this.writeWhenTrue.test( StatisticType.DOUBLE_SCORE, next.getKey() ) )
            {
                log( outputs, next.getKey(), true );

                List<DoubleScoreStatisticOuter> filtered = this.getFilteredStatisticsForThisDestinationType( outputs,
                                                                                                             next.getKey() );

                // Consume the output
                next.getValue().accept( filtered );

                log( outputs, next.getKey(), false );
            }
        }
    }

    /**
     * Processes {@link DurationScoreStatisticOuter}.
     * 
     * @param outputs the output to consume
     * @throws NullPointerException if the input is null
     */

    private void processDurationScoreOutputs( List<DurationScoreStatisticOuter> outputs )
    {
        Objects.requireNonNull( outputs, NULL_OUTPUT_STRING );

        // Iterate through the consumers
        for ( Entry<DestinationType, Consumer<List<DurationScoreStatisticOuter>>> next : this.durationScoreConsumers.entrySet() )
        {
            // Consume conditionally
            if ( this.writeWhenTrue.test( StatisticType.DURATION_SCORE, next.getKey() ) )
            {
                log( outputs, next.getKey(), true );

                List<DurationScoreStatisticOuter> filtered = this.getFilteredStatisticsForThisDestinationType( outputs,
                                                                                                               next.getKey() );

                // Consume the output
                next.getValue().accept( filtered );

                log( outputs, next.getKey(), false );
            }
        }
    }

    /**
     * Processes {@link DiagramDiagramStatisticOuter}.
     * 
     * @param outputs the output to consume
     * @throws NullPointerException if the input is null
     */

    private void processDurationDiagramStatistic( List<DurationDiagramStatisticOuter> outputs )
    {
        Objects.requireNonNull( outputs, NULL_OUTPUT_STRING );

        // Iterate through the consumers
        for ( Entry<DestinationType, Consumer<List<DurationDiagramStatisticOuter>>> next : this.durationDiagramConsumers.entrySet() )
        {
            // Consume conditionally
            if ( this.writeWhenTrue.test( StatisticType.DURATION_DIAGRAM, next.getKey() ) )
            {
                log( outputs, next.getKey(), true );

                List<DurationDiagramStatisticOuter> filtered =
                        this.getFilteredStatisticsForThisDestinationType( outputs, next.getKey() );

                // Consume the output
                next.getValue().accept( filtered );

                log( outputs, next.getKey(), false );
            }
        }
    }

    /**
     * Closes resources that were opened by this class.
     * 
     * @throws IOException if any one consumer could not be closed.
     */

    @Override
    public void close() throws IOException
    {
        int countFailedToClose = 0;
        IOException reThrow = null;
        for ( Closeable resource : this.resourcesToClose )
        {
            LOGGER.debug( "About to close {}", resource );

            try
            {
                resource.close();
            }
            catch ( IOException ioe )
            {
                countFailedToClose++;
                reThrow = ioe;
                // Not much we can do at this point. We tried to close, but
                // we need to try to close all the other resources too before
                // the software exits. Will rethrow the last one below.
                LOGGER.warn( "Unable to close resource {}", resource, ioe );
            }
        }

        // Rethrow
        if ( countFailedToClose > 0 )
        {
            throw new IOException( "While attempting to close a statistics consumer, failed to close "
                                   + countFailedToClose
                                   + " dependent resources. The last exception follows.",
                                   reThrow );
        }
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
     * suppressed for particular statistics. See `getSuppressTheseMetricsForThisDestinationType()` down below.
     * 
     * @param statistics the statistics to filter
     * @param destinationType the destination type by which to filter
     * @return a filtered list of statistics, omitting those to be suppressed for the prescribed destination type
     * @throws NullPointerException if any input is null
     */

    private <T extends Statistic<?>> List<T>
            getFilteredStatisticsForThisDestinationType( List<T> statistics, DestinationType destinationType )
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
     * @return the output directory.
     */

    private Path getOutputDirectory()
    {
        return this.outputDirectory;
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
     * @param evaluationDescription the evaluation description
     * @param writeWhenTrue the condition under which outputs should be written
     * @param outputDirectory the output directory
     * @throws NullPointerException if any of the inputs are null
     * @throws WresProcessingException if the project is invalid for writing
     */

    private StatisticsConsumer( Evaluation evaluationDescription,
                                BiPredicate<StatisticType, DestinationType> writeWhenTrue,
                                Path outputDirectory )
    {
        Objects.requireNonNull( writeWhenTrue, "Specify a non-null condition to ignore." );
        Objects.requireNonNull( evaluationDescription );
        Objects.requireNonNull( outputDirectory );

        this.resourcesToClose = new ArrayList<>( 1 );
        this.writersToPaths = new ArrayList<>();
        this.writeWhenTrue = writeWhenTrue;
        this.evaluationDescription = evaluationDescription;
        this.outputDirectory = outputDirectory;

        Outputs outputsDescription = this.getEvaluationDescription()
                                         .getOutputs();

        this.suppressTheseDestinationsForTheseMetrics =
                this.getMetricsToSuppressForEachDestination( outputsDescription );

        // Register output consumers
        try
        {
            this.buildGraphicsConsumers( outputsDescription );
        }
        catch ( ProjectConfigException e )
        {
            throw new GraphicsWriteException( "While processing the project configuration to write output:", e );
        }
    }

}
