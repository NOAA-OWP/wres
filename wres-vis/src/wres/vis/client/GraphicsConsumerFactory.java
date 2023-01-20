package wres.vis.client;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DestinationType;
import wres.datamodel.statistics.StatisticsToFormatsRouter;
import wres.events.subscribe.ConsumerFactory;
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
 * Implementation of a {@link ConsumerFactory} for graphics writing.
 * @author James Brown
 */

class GraphicsConsumerFactory implements ConsumerFactory
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( GraphicsConsumerFactory.class );

    /** Consumer description. */
    private final Consumer consumerDescription;

    @Override
    public Function<Statistics, Set<Path>> getConsumer( Evaluation evaluation, Path path )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( path );

        if ( LOGGER.isDebugEnabled() )
        {
            Collection<Format> formats = this.getConsumerDescription()
                                             .getFormatsList();

            LOGGER.debug( "Creating a statistics consumer for these formats: {}.", formats );
        }

        Outputs outputs = evaluation.getOutputs();

        StatisticsToFormatsRouter.Builder builder = new StatisticsToFormatsRouter.Builder();

        Function<Collection<Statistics>, Set<Path>> router = builder.setEvaluationDescription( evaluation )
                                                                    .addBoxplotConsumerPerPair( DestinationType.GRAPHIC,
                                                                                                BoxplotGraphicsWriter.of( outputs,
                                                                                                                          path ) )
                                                                    .build();


        return statistics -> router.apply( List.of( statistics ) );
    }

    @Override
    public Function<Collection<Statistics>, Set<Path>> getGroupedConsumer( Evaluation evaluation, Path path )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( path );

        Outputs outputs = evaluation.getOutputs();

        if ( LOGGER.isDebugEnabled() )
        {
            Collection<Format> formats = this.getConsumerDescription()
                                             .getFormatsList();

            LOGGER.debug( "Creating a grouped statistics consumer for these formats: {}.", formats );
        }

        StatisticsToFormatsRouter.Builder builder = new StatisticsToFormatsRouter.Builder();

        // Note that diagrams are always written by group, even if all the statistics could be written per pool because
        // grouping is currently done per feature and not, for example, per pool within a feature (grouping over the 
        // various thresholds that are messaged separately). To allow for writing per pool, an additional layer of 
        // message grouping would be needed.       
        return builder.setEvaluationDescription( evaluation )
                      .addBoxplotConsumerPerPool( DestinationType.GRAPHIC,
                                                  BoxplotGraphicsWriter.of( outputs, path ) )
                      .addDoubleScoreConsumer( DestinationType.GRAPHIC,
                                               DoubleScoreGraphicsWriter.of( outputs, path ) )
                      .addDurationScoreConsumer( DestinationType.GRAPHIC,
                                                 DurationScoreGraphicsWriter.of( outputs, path ) )
                      .addDiagramConsumer( DestinationType.GRAPHIC,
                                           DiagramGraphicsWriter.of( outputs, path ) )
                      .addDurationDiagramConsumer( DestinationType.GRAPHIC,
                                                   DurationDiagramGraphicsWriter.of( outputs, path ) )
                      .build();
    }

    @Override
    public Consumer getConsumerDescription()
    {
        return this.consumerDescription;
    }

    /**
     * Builds an instance.
     * @param consumerId the consumer identifier
     * @throws NullPointerException if the consumer identifier is null
     */

    GraphicsConsumerFactory( String consumerId )
    {
        Objects.requireNonNull( consumerId );

        this.consumerDescription = Consumer.newBuilder()
                                           .setConsumerId( consumerId )
                                           .addFormats( Format.PNG )
                                           .addFormats( Format.SVG )
                                           .build();
    }

    @Override
    public void close() throws IOException
    {
        // No-op
    }

}
