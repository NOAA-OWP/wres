package wres.pipeline;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.DoubleFunction;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.EvaluationDeclaration;
import wres.datamodel.statistics.DoubleScoreStatisticOuter;
import wres.datamodel.statistics.DurationScoreStatisticOuter;
import wres.datamodel.statistics.StatisticsToFormatsRouter;
import wres.datamodel.time.TimeSeriesSlicer;
import wres.events.subscribe.ConsumerFactory;
import wres.statistics.MessageUtilities;
import wres.vis.writing.PairsStatisticsGraphicsWriter;
import wres.writing.csv.statistics.CommaSeparatedBoxPlotWriter;
import wres.writing.csv.statistics.CommaSeparatedDiagramWriter;
import wres.writing.csv.statistics.CommaSeparatedDurationDiagramWriter;
import wres.writing.csv.statistics.CommaSeparatedScoreWriter;
import wres.writing.csv.statistics.CsvStatisticsWriter;
import wres.writing.netcdf.NetcdfOutputWriter;
import wres.writing.protobuf.ProtobufWriter;
import wres.events.subscribe.StatisticsConsumer;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.DoubleScoreMetric;
import wres.statistics.generated.DoubleScoreStatistic;
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.DurationScoreStatistic;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Consumer.Format;
import wres.vis.writing.BoxplotGraphicsWriter;
import wres.vis.writing.DiagramGraphicsWriter;
import wres.vis.writing.DoubleScoreGraphicsWriter;
import wres.vis.writing.DurationDiagramGraphicsWriter;
import wres.vis.writing.DurationScoreGraphicsWriter;

/**
 * Implementation of a {@link ConsumerFactory} for statistics writing.
 * @author James Brown
 */

class StatisticsConsumerFactory implements ConsumerFactory
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( StatisticsConsumerFactory.class );

    /** Consumer description. */
    private final Consumer consumerDescription;

    /** Project declaration. */
    private final EvaluationDeclaration declaration;

    /** The netcdf writer. */
    private final List<NetcdfOutputWriter> netcdfWriters;

    @Override
    public StatisticsConsumer getConsumer( Evaluation evaluation, Path path )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( path );

        Outputs outputs = evaluation.getOutputs();

        // Resources to close on completion.
        List<Closeable> resources = new ArrayList<>();

        Collection<Format> formats = this.getConsumerDescription()
                                         .getFormatsList();

        LOGGER.debug( "Creating a statistics consumer for these formats: {}.", formats );

        StatisticsToFormatsRouter.Builder builder = new StatisticsToFormatsRouter.Builder();
        ChronoUnit durationUnits = this.declaration.durationFormat();

        // Netcdf, CSV2 and protobuf are incremental formats, plus box plots per pair where graphics are required

        // Netcdf: unlike other formats this writer is injected because it has an oddball choreography whereby blob
        // creation needs to happen before statistics are written, but after thresholds are read. When creation/writing 
        // happens on-the-fly, this can be simplified
        if ( formats.contains( Format.NETCDF ) )
        {
            for ( NetcdfOutputWriter writer : this.netcdfWriters )
            {
                builder.addDoubleScoreConsumer( wres.config.yaml.components.Format.NETCDF, writer );

                // Add a duration score consumer that writes double scores
                Function<List<DurationScoreStatisticOuter>, Set<Path>> durationScoreWriter =
                        scores -> writer.apply( this.mapDurationScores( scores ) );
                builder.addDurationScoreConsumer( wres.config.yaml.components.Format.NETCDF, durationScoreWriter );
            }
        }

        // CSV2
        if ( formats.contains( Format.CSV2 ) )
        {
            Path fullPath = path.resolve( CsvStatisticsWriter.DEFAULT_FILE_NAME );

            DoubleFunction<String> formatter = this.getDecimalFormatter( this.declaration );
            CsvStatisticsWriter writer = CsvStatisticsWriter.of( evaluation,
                                                                 fullPath,
                                                                 true,
                                                                 durationUnits,
                                                                 formatter );

            builder.addStatisticsConsumer( wres.config.yaml.components.Format.CSV2,
                                           writer );

            resources.add( writer );
        }

        // Protobuf
        if ( formats.contains( Format.PROTOBUF ) )
        {
            Path protobufPath = path.resolve( "evaluation.pb3" );
            Function<Statistics, Set<Path>> protoWriter = ProtobufWriter.of( protobufPath, evaluation );
            builder.addStatisticsConsumer( wres.config.yaml.components.Format.PROTOBUF, protoWriter );
        }

        // Graphics
        if ( this.hasGraphics( formats ) )
        {
            // Specific formats are filtered at runtime via the router using the Outputs declaration
            BoxplotGraphicsWriter boxPlotWriter = BoxplotGraphicsWriter.of( outputs, path );
            builder.addBoxplotConsumerPerPair( wres.config.yaml.components.Format.GRAPHIC,
                                               boxPlotWriter )
                   .addPairsStatisticsConsumer( wres.config.yaml.components.Format.GRAPHIC,
                                                PairsStatisticsGraphicsWriter.of( outputs, path ) );
        }

        Function<Collection<Statistics>, Set<Path>> router = builder.setEvaluationDescription( evaluation )
                                                                    .build();

        return new StatisticsConsumer()
        {
            @Override
            public void close()
            {
                StatisticsConsumerFactory.closeResources( resources );
            }

            @Override
            public Set<Path> apply( Collection<Statistics> statistics )
            {
                return router.apply( statistics );
            }
        };
    }

    @Override
    public StatisticsConsumer getGroupedConsumer( Evaluation evaluation, Path path )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( path );

        Outputs outputs = evaluation.getOutputs();

        Collection<Format> formats = this.getConsumerDescription()
                                         .getFormatsList();

        LOGGER.debug( "Creating a grouped statistics consumer for these formats: {}.", formats );

        StatisticsToFormatsRouter.Builder builder = new StatisticsToFormatsRouter.Builder();

        // Graphics
        if ( this.hasGraphics( formats ) )
        {
            // Use the wres.config.yaml.components.Format.GRAPHIC identifier because there is a single writer for
            // multiple graphics formats
            builder.addBoxplotConsumerPerPool( wres.config.yaml.components.Format.GRAPHIC,
                                               BoxplotGraphicsWriter.of( outputs, path ) )
                   .addDoubleScoreConsumer( wres.config.yaml.components.Format.GRAPHIC,
                                            DoubleScoreGraphicsWriter.of( outputs, path ) )
                   .addDurationScoreConsumer( wres.config.yaml.components.Format.GRAPHIC,
                                              DurationScoreGraphicsWriter.of( outputs, path ) )
                   .addDiagramConsumer( wres.config.yaml.components.Format.GRAPHIC,
                                        DiagramGraphicsWriter.of( outputs, path ) )
                   .addDurationDiagramConsumer( wres.config.yaml.components.Format.GRAPHIC,
                                                DurationDiagramGraphicsWriter.of( outputs, path ) );
        }

        // Note that diagrams are always written by group, even if all the statistics could be written per pool because
        // grouping is currently done per feature and not, for example, per pool within a feature (grouping over the 
        // various thresholds that are messaged separately). To allow for writing per pool, an additional layer of 
        // message grouping would be needed.
        StatisticsToFormatsRouter router = builder.setEvaluationDescription( evaluation )
                                                  .build();
        return StatisticsConsumer.getResourceFreeConsumer( router );
    }

    @Override
    public Consumer getConsumerDescription()
    {
        return this.consumerDescription;
    }

    /**
     * Close resources on completion.
     * @param resources the resources to close
     */

    private static void closeResources( List<Closeable> resources )
    {
        LOGGER.debug( "Closing consumers." );

        // Best faith effort to close each one, logging errors
        for ( Closeable closeMe : resources )
        {
            try
            {
                closeMe.close();
            }
            catch ( IOException e )
            {
                LOGGER.warn( "Failed to close a format writer.", e );
            }
        }
    }

    /**
     * Returns <code>true</code> if graphics are required and will be delivered by this consumer factory,
     * otherwise <code>false</code>.
     *
     * @param formats the formats
     * @return true if graphics consumers are required, otherwise false.
     */

    private boolean hasGraphics( Collection<Format> formats )
    {
        return formats.contains( Format.PNG ) || formats.contains( Format.SVG );
    }

    /**
     * Builds an instance.
     * @param consumerId the consumer identifier
     * @param formats the formats to be delivered
     * @param netcdfWriters The netcdf writers, if any.
     * @param declaration the project declaration for netcdf writing
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if no formats are declared
     */

    StatisticsConsumerFactory( String consumerId,
                               Set<Format> formats,
                               List<NetcdfOutputWriter> netcdfWriters,
                               EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( consumerId );
        Objects.requireNonNull( formats );
        Objects.requireNonNull( declaration );

        if ( formats.contains( Format.NETCDF ) )
        {
            Objects.requireNonNull( netcdfWriters );

            if ( netcdfWriters.isEmpty() )
            {
                throw new IllegalArgumentException( "Expected at least one NetCDF writer." );
            }
        }

        this.consumerDescription = Consumer.newBuilder()
                                           .setConsumerId( consumerId )
                                           .addAllFormats( formats )
                                           .build();

        this.declaration = declaration;
        // Do not add the netcdf writers to the list of resources, only the resources created here. Better to destroy
        // resources where they are created.
        this.netcdfWriters = netcdfWriters;
    }

    /**
     * Returns a formatter for decimal values as strings, null if none is defined.
     *
     * @param declaration the project declaration
     * @return a formatter
     */

    private DoubleFunction<String> getDecimalFormatter( EvaluationDeclaration declaration )
    {
        java.text.Format formatter = declaration.decimalFormat();

        return doubleValue -> {
            if ( Objects.nonNull( formatter ) )
            {
                return formatter.format( doubleValue );
            }

            return Double.toString( doubleValue );
        };
    }

    /**
     * Maps duration scores to double scores for writing to NetCDF, which only consumes double scores.
     * @param scores the duration scores
     * @return the double scores
     */
    private List<DoubleScoreStatisticOuter> mapDurationScores( List<DurationScoreStatisticOuter> scores )
    {
        List<DoubleScoreStatisticOuter> mapped = new ArrayList<>();

        // Get the preferred duration units
        ChronoUnit units = this.declaration.durationFormat();

        for ( DurationScoreStatisticOuter next : scores )
        {
            List<DurationScoreStatistic.DurationScoreStatisticComponent> c = next.getStatistic()
                                                                                 .getStatisticsList();

            List<DoubleScoreStatistic.DoubleScoreStatisticComponent> d = new ArrayList<>();
            List<DoubleScoreMetric.DoubleScoreMetricComponent> e = new ArrayList<>();
            for ( DurationScoreStatistic.DurationScoreStatisticComponent nextComponent : c )
            {
                DurationScoreMetric.DurationScoreMetricComponent metricComponent = nextComponent.getMetric();
                MetricName metricName = metricComponent.getName();

                com.google.protobuf.Duration minimum = metricComponent.getMinimum();
                Duration minimumDuration = MessageUtilities.getDuration( minimum );
                double minimumDecimal = TimeSeriesSlicer.durationToDecimalMilliPrecision( minimumDuration, units );
                com.google.protobuf.Duration maximum = metricComponent.getMaximum();
                Duration maximumDuration = MessageUtilities.getDuration( maximum );
                double maximumDecimal = TimeSeriesSlicer.durationToDecimalMilliPrecision( maximumDuration, units );
                com.google.protobuf.Duration optimum = metricComponent.getOptimum();
                Duration optimumDuration = MessageUtilities.getDuration( optimum );
                double optimumDecimal = TimeSeriesSlicer.durationToDecimalMilliPrecision( optimumDuration, units );

                DoubleScoreMetric.DoubleScoreMetricComponent doubleMetric =
                        DoubleScoreMetric.DoubleScoreMetricComponent.newBuilder()
                                                                    .setName( metricName )
                                                                    .setMinimum( minimumDecimal )
                                                                    .setMaximum( maximumDecimal )
                                                                    .setOptimum( optimumDecimal )
                                                                    .setUnits( units.name() )
                                                                    .build();

                com.google.protobuf.Duration statistic = nextComponent.getValue();
                Duration statisticDuration = MessageUtilities.getDuration( statistic );
                double decimalStatistic = TimeSeriesSlicer.durationToDecimalMilliPrecision( statisticDuration, units );
                DoubleScoreStatistic.DoubleScoreStatisticComponent doubleStatistic =
                        DoubleScoreStatistic.DoubleScoreStatisticComponent.newBuilder()
                                                                          .setMetric( doubleMetric )
                                                                          .setValue( decimalStatistic )
                                                                          .build();
                d.add( doubleStatistic );
                e.add( doubleMetric );
            }

            DurationScoreStatistic durationScore = next.getStatistic();
            DurationScoreMetric durationMetric = durationScore.getMetric();
            DoubleScoreMetric doubleMetric = DoubleScoreMetric.newBuilder()
                                                              .setName( durationMetric.getName() )
                                                              .addAllComponents( e )
                                                              .build();

            DoubleScoreStatistic nextDouble = DoubleScoreStatistic.newBuilder()
                                                                  .addAllStatistics( d )
                                                                  .setMetric( doubleMetric )
                                                                  .addAllStatistics( d )
                                                                  .build();
            DoubleScoreStatisticOuter outer = DoubleScoreStatisticOuter.of( nextDouble,
                                                                            next.getPoolMetadata(),
                                                                            next.getSummaryStatistic() );
            mapped.add( outer );
        }

        return Collections.unmodifiableList( mapped );
    }
}
