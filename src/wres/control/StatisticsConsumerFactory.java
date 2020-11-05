package wres.control;

import java.nio.file.Path;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import wres.config.generated.DestinationConfig;
import wres.config.generated.DestinationType;
import wres.config.generated.ProjectConfig;
import wres.datamodel.messages.MessageFactory;
import wres.datamodel.statistics.StatisticsToFormatsRouter;
import wres.datamodel.statistics.DoubleScoreStatisticOuter.DoubleScoreComponentOuter;
import wres.events.subscribe.ConsumerFactory;
import wres.io.config.ConfigHelper;
import wres.io.writing.commaseparated.statistics.CommaSeparatedBoxPlotWriter;
import wres.io.writing.commaseparated.statistics.CommaSeparatedDiagramWriter;
import wres.io.writing.commaseparated.statistics.CommaSeparatedDurationDiagramWriter;
import wres.io.writing.commaseparated.statistics.CommaSeparatedScoreWriter;
import wres.io.writing.netcdf.NetcdfOutputWriter;
import wres.io.writing.protobuf.ProtobufWriter;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Consumer.Format;
import wres.system.SystemSettings;
import wres.vis.writing.BoxPlotGraphicsWriter;
import wres.vis.writing.DiagramGraphicsWriter;
import wres.vis.writing.DoubleScoreGraphicsWriter;
import wres.vis.writing.DurationDiagramGraphicsWriter;
import wres.vis.writing.DurationScoreGraphicsWriter;

/**
 * Implementation of a {@link ConsumerFactory} for statistics writing.
 * @author james.brown@hydrosolved.com
 */

class StatisticsConsumerFactory implements ConsumerFactory
{

    /**
     * Consumer description.
     */

    private final Consumer consumerDescription;

    /**
     * System settings.
     */

    private final SystemSettings systemSettings;

    /**
     * Project declaration.
     */

    private final ProjectConfig projectConfig;
    
    /**
     * The netcdf writer.
     */

    private final NetcdfOutputWriter netcdfWriter;
    
    /**
     * Decimal formatter.
     */

    private final java.text.Format decimalFormatter;

    @Override
    public Function<Collection<Statistics>, Set<Path>> getConsumer( Evaluation evaluation, String evaluationId )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( evaluationId );

        Outputs outputs = evaluation.getOutputs();
        Collection<Format> formats = this.getConsumerDescription()
                                         .getFormatsList();
        Path outputsDirectory = ConsumerFactory.getPathToWrite( evaluationId, this.getConsumerId() );

        StatisticsToFormatsRouter.Builder builder = new StatisticsToFormatsRouter.Builder();

        // Netcdf and protobuf are incremental formats, plus box plots per pair where graphics are required

        // Netcdf: unlike other formats this writer is injected because it has an oddball choreography whereby blob
        // creation needs to happen before statistics are written, but after thresholds are read. When creation/writing 
        // happens on-the-fly, this can be simplified
        if ( formats.contains( Format.NETCDF ) )
        {
            builder.addDoubleScoreConsumer( DestinationType.NETCDF, this.netcdfWriter );
        }

        // Protobuf
        if ( formats.contains( Format.PROTOBUF ) )
        {
            Path protobufPath = outputsDirectory.resolve( "evaluation.pb3" );
            Function<Statistics, Set<Path>> protoWriter = ProtobufWriter.of( protobufPath, evaluation );
            builder.addStatisticsConsumer( DestinationType.PROTOBUF, protoWriter );
        }

        // Graphics
        if ( this.hasGraphics( formats ) )
        {
            // Specific formats are filtered at runtime via the router using the Outputs declaration
            builder.addBoxplotConsumerPerPair( DestinationType.GRAPHIC,
                                               BoxPlotGraphicsWriter.of( outputs, outputsDirectory ) );
        }

        return builder.setEvaluationDescription( evaluation )
                      .build();
    }

    @Override
    public Function<Collection<Statistics>, Set<Path>> getGroupedConsumer( Evaluation evaluation, String evaluationId )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( evaluationId );

        Outputs outputs = evaluation.getOutputs();
        Path outputsDirectory = ConsumerFactory.getPathToWrite( evaluationId, this.getConsumerId() );
        Collection<Format> formats = this.getConsumerDescription()
                                         .getFormatsList();

        StatisticsToFormatsRouter.Builder builder = new StatisticsToFormatsRouter.Builder();

        // CSV
        if ( formats.contains( Format.CSV ) )
        {
            String durationUnitsString = this.projectConfig.getOutputs()
                                                           .getDurationFormat()
                                                           .value()
                                                           .toUpperCase();

            // Formatted doubles to write
            Function<DoubleScoreComponentOuter, String> doubleMapper = next -> {
                if ( Objects.nonNull( this.decimalFormatter ) )
                {
                    return this.decimalFormatter.format( next.getData().getValue() );
                }

                return Double.toString( next.getData().getValue() );
            };

            ChronoUnit durationUnits = ChronoUnit.valueOf( durationUnitsString );


            builder.addDiagramConsumer( DestinationType.CSV,
                                        CommaSeparatedDiagramWriter.of( this.projectConfig,
                                                                        durationUnits,
                                                                        outputsDirectory ) )
                   .addBoxplotConsumerPerPair( DestinationType.CSV,
                                               CommaSeparatedBoxPlotWriter.of( this.projectConfig,
                                                                               durationUnits,
                                                                               outputsDirectory ) )
                   .addBoxplotConsumerPerPool( DestinationType.CSV,
                                               CommaSeparatedBoxPlotWriter.of( this.projectConfig,
                                                                               durationUnits,
                                                                               outputsDirectory ) )
                   .addDurationDiagramConsumer( DestinationType.CSV,
                                                CommaSeparatedDurationDiagramWriter.of( this.projectConfig,
                                                                                        durationUnits,
                                                                                        outputsDirectory ) )
                   .addDurationScoreConsumer( DestinationType.CSV,
                                              CommaSeparatedScoreWriter.of( this.projectConfig,
                                                                            durationUnits,
                                                                            outputsDirectory,
                                                                            next -> MessageFactory.parse( next.getData()
                                                                                                              .getValue() )
                                                                                                  .toString() ) )
                   .addDoubleScoreConsumer( DestinationType.CSV,
                                            CommaSeparatedScoreWriter.of( this.projectConfig,
                                                                          durationUnits,
                                                                          outputsDirectory,
                                                                          doubleMapper ) );
        }

        // Graphics
        if ( this.hasGraphics( formats ) )
        {
            builder.addBoxplotConsumerPerPool( DestinationType.GRAPHIC,
                                               BoxPlotGraphicsWriter.of( outputs, outputsDirectory ) )
                   .addDoubleScoreConsumer( DestinationType.GRAPHIC,
                                            DoubleScoreGraphicsWriter.of( outputs, outputsDirectory ) )
                   .addDurationScoreConsumer( DestinationType.GRAPHIC,
                                              DurationScoreGraphicsWriter.of( outputs, outputsDirectory ) )
                   .addDiagramConsumer( DestinationType.GRAPHIC,
                                        DiagramGraphicsWriter.of( outputs, outputsDirectory ) )
                   .addDurationDiagramConsumer( DestinationType.GRAPHIC,
                                                DurationDiagramGraphicsWriter.of( outputs, outputsDirectory ) );
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

    /**
     * @return the subscriber identifier
     */

    private String getConsumerId()
    {
        return this.consumerDescription.getConsumerId();
    }

    /**
     * @param formats the formats
     * @return true if graphics consumers are required, otherwise false.
     */

    private boolean hasGraphics( Collection<Format> formats )
    {
        // No external graphics, and some graphics formats required, then graphics needed 
        return !this.systemSettings.hasExternalGraphics()
               && ( formats.contains( Format.PNG ) || formats.contains( Format.SVG ) );
    }

    /**
     * Builds an instance.
     * @param consumerId the consumer identifier
     * @param formats the formats to be delivered
     * @param systemSettings the system settings for netcdf writing
     * @param netcdfWriter a netcdf writer
     * @param projectConfig the project declaration for netcdf writing
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if no formats are declared
     */

    StatisticsConsumerFactory( String consumerId,
                               Set<Format> formats,
                               SystemSettings systemSettings,
                               NetcdfOutputWriter netcdfWriter,
                               ProjectConfig projectConfig )
    {
        Objects.requireNonNull( consumerId );
        Objects.requireNonNull( formats );
        Objects.requireNonNull( systemSettings );
        Objects.requireNonNull( projectConfig );

        if ( formats.contains( Format.NETCDF ) )
        {
            Objects.requireNonNull( netcdfWriter );
        }

        this.consumerDescription = Consumer.newBuilder()
                                           .setConsumerId( consumerId )
                                           .addAllFormats( formats )
                                           .build();
        this.systemSettings = systemSettings;
        this.projectConfig = projectConfig;
        this.netcdfWriter = netcdfWriter;

        this.decimalFormatter = this.getDecimalFormatter( this.projectConfig );
    }


    /**
     * Returns a formatter for decimal values as strings, null if none is defined.
     * 
     * @param projectConfig the project declaration
     * @return a formatter or null
     */

    private java.text.Format getDecimalFormatter( ProjectConfig projectConfig )
    {
        for ( DestinationConfig next : projectConfig.getOutputs().getDestination() )
        {
            if ( next.getType() == DestinationType.CSV || next.getType() == DestinationType.NUMERIC )
            {
                return ConfigHelper.getDecimalFormatter( next );
            }
        }

        return null;
    }

}
