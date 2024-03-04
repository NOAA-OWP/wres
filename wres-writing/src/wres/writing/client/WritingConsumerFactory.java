package wres.writing.client;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.text.DecimalFormat;
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

import wres.datamodel.statistics.StatisticsToFormatsRouter;
import wres.events.subscribe.ConsumerFactory;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Statistics;
import wres.writing.csv.statistics.CsvStatisticsWriter;
import wres.writing.protobuf.ProtobufWriter;

/**
 * Implementation of a {@link ConsumerFactory} for statistics writing.
 * @author Evan Pagryzinski
 */

class WritingConsumerFactory implements ConsumerFactory
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( WritingConsumerFactory.class );

    /** Consumer description. */
    private final Consumer consumerDescription;

    /** Resources to close on completion. */
    private final List<Closeable> resources;


    @Override
    public Function<Statistics, Set<Path>> getConsumer( Evaluation evaluation, Path path )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( path );

        Collection<Format> formats = this.getConsumerDescription()
                                         .getFormatsList();


        LOGGER.debug( "Creating a statistics consumer for these formats: {}.", formats );

        StatisticsToFormatsRouter.Builder builder = new StatisticsToFormatsRouter.Builder();
        Outputs outputs = evaluation.getOutputs();

        // CSV2
        if ( outputs.hasCsv2() )
        {
            ChronoUnit durationUnits = this.getDurationUnitsFromOutputs( outputs );
            Path fullPath = path.resolve( CsvStatisticsWriter.DEFAULT_FILE_NAME );
            DoubleFunction<String> formatter = this.getDecimalFormatter( evaluation );

            CsvStatisticsWriter writer = CsvStatisticsWriter.of( evaluation,
                                                                 fullPath,
                                                                 false,
                                                                 durationUnits,
                                                                 formatter );

            builder.addStatisticsConsumer( wres.config.yaml.components.Format.CSV2,
                                           writer );

            this.resources.add( writer );
        }

        // Protobuf
        if ( outputs.hasProtobuf() )
        {
            Path protobufPath = path.resolve( "evaluation.pb3" );
            Function<Statistics, Set<Path>> protoWriter = ProtobufWriter.of( protobufPath, evaluation );
            builder.addStatisticsConsumer( wres.config.yaml.components.Format.PROTOBUF, protoWriter );
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

        Collection<Format> formats = this.getConsumerDescription()
                                         .getFormatsList();

        LOGGER.debug( "Creating a grouped statistics consumer for these formats: {}.", formats );

        StatisticsToFormatsRouter.Builder builder = new StatisticsToFormatsRouter.Builder();

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
     * Builds an instance.
     * @param consumerId the consumer identifier
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if no formats are declared
     */

    WritingConsumerFactory( String consumerId )
    {
        Objects.requireNonNull( consumerId );

        this.consumerDescription = Consumer.newBuilder()
                                           .setConsumerId( consumerId )
                                           .addFormats( Format.PROTOBUF )
                                           .addFormats( Format.CSV2 )
                                           .build();

        this.resources = new ArrayList<>();
    }

    /**
     * Returns a formatter for decimal values as strings
     *
     * @param declaration the project declaration
     * @return a formatter
     */

    private DoubleFunction<String> getDecimalFormatter( Evaluation declaration )
    {
        //Gets the decimal mask pattern
        String decimalFormat = declaration.getOutputs().getCsv2().getOptions().getDecimalFormat();
        java.text.Format formatter = new DecimalFormat( decimalFormat );

        return doubleValue -> {
            if ( !decimalFormat.isEmpty() )
            {
                return formatter.format( doubleValue );
            }

            return Double.toString( doubleValue );
        };
    }

    private ChronoUnit getDurationUnitsFromOutputs( Outputs outputs )
    {
        Objects.requireNonNull( outputs );

        return ChronoUnit.valueOf( outputs.getCsv2().getOptions().getLeadUnit().name() );
    }
}
