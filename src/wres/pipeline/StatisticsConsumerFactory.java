package wres.pipeline;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.DoubleFunction;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.statistics.StatisticsToFormatsRouter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.events.subscribe.ConsumerFactory;
import wres.io.config.ConfigHelper;
import wres.io.writing.csv.statistics.CommaSeparatedBoxPlotWriter;
import wres.io.writing.csv.statistics.CommaSeparatedDiagramWriter;
import wres.io.writing.csv.statistics.CommaSeparatedDurationDiagramWriter;
import wres.io.writing.csv.statistics.CommaSeparatedScoreWriter;
import wres.io.writing.csv.statistics.CsvStatisticsWriter;
import wres.io.writing.netcdf.NetcdfOutputWriter;
import wres.io.writing.protobuf.ProtobufWriter;
import wres.statistics.MessageFactory;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Evaluation;
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
    private final ProjectConfig projectConfig;

    /** The netcdf writer. */
    private final List<NetcdfOutputWriter> netcdfWriters;

    /** Resources to close on completion. */
    private final List<Closeable> resources;

    @Override
    public Function<Statistics, Set<Path>> getConsumer( Evaluation evaluation, Path path )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( path );

        Outputs outputs = evaluation.getOutputs();
        Collection<Format> formats = this.getConsumerDescription()
                                         .getFormatsList();

        LOGGER.debug( "Creating a statistics consumer for these formats: {}.", formats );

        StatisticsToFormatsRouter.Builder builder = new StatisticsToFormatsRouter.Builder();

        String durationUnitsString = this.projectConfig.getOutputs()
                                                       .getDurationFormat()
                                                       .value()
                                                       .toUpperCase();

        ChronoUnit durationUnits = ChronoUnit.valueOf( durationUnitsString );

        // Netcdf and protobuf are incremental formats, plus box plots per pair where graphics are required

        // Netcdf: unlike other formats this writer is injected because it has an oddball choreography whereby blob
        // creation needs to happen before statistics are written, but after thresholds are read. When creation/writing 
        // happens on-the-fly, this can be simplified
        if ( formats.contains( Format.NETCDF ) )
        {
            for ( NetcdfOutputWriter writer : this.netcdfWriters )
            {
                builder.addDoubleScoreConsumer( wres.config.yaml.components.Format.NETCDF,
                                                writer );
            }
        }

        // CSV2
        if ( formats.contains( Format.CSV2 ) )
        {
            Path fullPath = path.resolve( CsvStatisticsWriter.DEFAULT_FILE_NAME );

            DoubleFunction<String> formatter = this.getDecimalFormatter( this.projectConfig,
                                                                         Set.of( DestinationType.CSV2 ) );
            CsvStatisticsWriter writer = CsvStatisticsWriter.of( evaluation,
                                                                 fullPath,
                                                                 true,
                                                                 durationUnits,
                                                                 formatter );

            builder.addStatisticsConsumer( wres.config.yaml.components.Format.CSV2,
                                           statistics -> {
                                               Path aPath = writer.apply( statistics );
                                               return Set.of( aPath );
                                           } );

            this.resources.add( writer );
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
                                               boxPlotWriter );
        }

        // Old-style CSV
        if ( formats.contains( Format.CSV ) )
        {
            builder.addBoxplotConsumerPerPair( wres.config.yaml.components.Format.CSV,
                                               CommaSeparatedBoxPlotWriter.of( this.projectConfig,
                                                                               durationUnits,
                                                                               path ) );
        }

        Function<Collection<Statistics>, Set<Path>> router = builder.setEvaluationDescription( evaluation )
                                                                    .build();

        return statistics -> router.apply( List.of( statistics ) );
    }

    @Override
    public Function<Collection<Statistics>, Set<Path>> getGroupedConsumer( Evaluation evaluation, Path path )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( path );

        Outputs outputs = evaluation.getOutputs();

        Collection<Format> formats = this.getConsumerDescription()
                                         .getFormatsList();

        LOGGER.debug( "Creating a grouped statistics consumer for these formats: {}.", formats );

        StatisticsToFormatsRouter.Builder builder = new StatisticsToFormatsRouter.Builder();

        // CSV
        if ( formats.contains( Format.CSV ) )
        {
            String durationUnitsString = this.projectConfig.getOutputs()
                                                           .getDurationFormat()
                                                           .value()
                                                           .toUpperCase();

            // Formatted doubles to write
            DoubleFunction<String> formatter =
                    this.getDecimalFormatter( this.projectConfig,
                                              Set.of( DestinationType.CSV, DestinationType.NUMERIC ) );
            Function<DoubleScoreComponentOuter, String> doubleMapper =
                    format -> formatter.apply( format.getData().getValue() );

            ChronoUnit durationUnits = ChronoUnit.valueOf( durationUnitsString );


            builder.addDiagramConsumer( wres.config.yaml.components.Format.CSV,
                                        CommaSeparatedDiagramWriter.of( this.projectConfig,
                                                                        durationUnits,
                                                                        path ) )
                   .addBoxplotConsumerPerPool( wres.config.yaml.components.Format.CSV,
                                               CommaSeparatedBoxPlotWriter.of( this.projectConfig,
                                                                               durationUnits,
                                                                               path ) )
                   .addDurationDiagramConsumer( wres.config.yaml.components.Format.CSV,
                                                CommaSeparatedDurationDiagramWriter.of( this.projectConfig,
                                                                                        durationUnits,
                                                                                        path ) )
                   .addDurationScoreConsumer( wres.config.yaml.components.Format.CSV,
                                              CommaSeparatedScoreWriter.of( this.projectConfig,
                                                                            durationUnits,
                                                                            path,
                                                                            next -> MessageFactory.parse( next.getData()
                                                                                                              .getValue() )
                                                                                                  .toString() ) )
                   .addDoubleScoreConsumer( wres.config.yaml.components.Format.CSV,
                                            CommaSeparatedScoreWriter.of( this.projectConfig,
                                                                          durationUnits,
                                                                          path,
                                                                          doubleMapper ) );
        }

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
        return builder.setEvaluationDescription( evaluation )
                      .build();
    }

    @Override
    public Consumer getConsumerDescription()
    {
        return this.consumerDescription;
    }

    @Override
    public void close()
    {
        LOGGER.debug( "Closing the consumer factory." );

        // Best faith effort to close each one, logging errors
        for ( Closeable closeMe : this.resources )
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
     * @param projectConfig the project declaration for netcdf writing
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if no formats are declared
     */

    StatisticsConsumerFactory( String consumerId,
                               Set<Format> formats,
                               List<NetcdfOutputWriter> netcdfWriters,
                               ProjectConfig projectConfig )
    {
        Objects.requireNonNull( consumerId );
        Objects.requireNonNull( formats );
        Objects.requireNonNull( projectConfig );

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

        this.projectConfig = projectConfig;
        this.resources = new ArrayList<>();
        // Do not add the netcdf writers to the list of resources, only the resources created here. Better to destroy
        // resources where they are created.
        this.netcdfWriters = netcdfWriters;
    }

    /**
     * Returns a formatter for decimal values as strings, null if none is defined.
     * 
     * @param projectConfig the project declaration
     * @param destinationTypes the destination types
     * @return a formatter
     */

    private DoubleFunction<String> getDecimalFormatter( ProjectConfig projectConfig,
                                                        Set<DestinationType> destinationTypes )
    {
        // Find the relevant config
        DestinationConfig config = projectConfig.getOutputs()
                                                .getDestination()
                                                .stream()
                                                .filter( next -> destinationTypes.contains( next.getType() ) )
                                                .findAny()
                                                .orElse( null );

        java.text.Format formatter = ConfigHelper.getDecimalFormatter( config );

        return doubleValue -> {
            if ( Objects.nonNull( formatter ) )
            {
                return formatter.format( doubleValue );
            }

            return Double.toString( doubleValue );
        };
    }
}
