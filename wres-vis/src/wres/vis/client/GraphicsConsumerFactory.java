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
 * Implementation of a {@link ConsumerFactory} for graphics writing. A single consumer is registered for all graphics
 * formats because each consumer can handle multiple formats, and it is efficient to create each graphics abstraction
 * (i.e., {@link org.jfree.chart.JFreeChart}) only once in memory for all formats.
 *
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

        // Use the wres.config.yaml.components.Format.GRAPHIC identifier because there is a single writer for multiple
        // graphics formats
        Function<Collection<Statistics>, Set<Path>> router
                = builder.setEvaluationDescription( evaluation )
                         .addBoxplotConsumerPerPair( wres.config.yaml.components.Format.GRAPHIC,
                                                     BoxplotGraphicsWriter.of( outputs, path ) )
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
        // message grouping would be needed. Use the wres.config.yaml.components.Format.GRAPHIC identifier because
        // there is a single writer for multiple graphics formats
        return builder.setEvaluationDescription( evaluation )
                      .addBoxplotConsumerPerPool( wres.config.yaml.components.Format.GRAPHIC,
                                                  BoxplotGraphicsWriter.of( outputs, path ) )
                      .addDoubleScoreConsumer( wres.config.yaml.components.Format.GRAPHIC,
                                               DoubleScoreGraphicsWriter.of( outputs, path ) )
                      .addDurationScoreConsumer( wres.config.yaml.components.Format.GRAPHIC,
                                                 DurationScoreGraphicsWriter.of( outputs, path ) )
                      .addDiagramConsumer( wres.config.yaml.components.Format.GRAPHIC,
                                           DiagramGraphicsWriter.of( outputs, path ) )
                      .addDurationDiagramConsumer( wres.config.yaml.components.Format.GRAPHIC,
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
