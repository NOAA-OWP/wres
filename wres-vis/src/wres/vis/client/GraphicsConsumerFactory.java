package wres.vis.client;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DestinationType;
import wres.datamodel.statistics.StatisticsToFormatsRouter;
import wres.events.subscribe.ConsumerException;
import wres.events.subscribe.ConsumerFactory;
import wres.statistics.generated.Consumer;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.Consumer.Format;
import wres.vis.writing.BoxPlotGraphicsWriter;
import wres.vis.writing.DiagramGraphicsWriter;
import wres.vis.writing.DoubleScoreGraphicsWriter;
import wres.vis.writing.DurationDiagramGraphicsWriter;
import wres.vis.writing.DurationScoreGraphicsWriter;

/**
 * Implementation of a {@link ConsumerFactory} for graphics writing.
 * @author james.brown@hydrosolved.com
 */

class GraphicsConsumerFactory implements ConsumerFactory
{

    private static final Logger LOGGER = LoggerFactory.getLogger( GraphicsConsumerFactory.class );

    /**
     * Consumer description.
     */

    private final Consumer consumerDescription;

    @Override
    public Function<Collection<Statistics>, Set<Path>> getConsumer( Evaluation evaluation, String evaluationId )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( evaluationId );

        Outputs outputs = evaluation.getOutputs();
        Path outputsDirectory = this.getPathToWrite( evaluationId );

        StatisticsToFormatsRouter.Builder builder = new StatisticsToFormatsRouter.Builder();

        return builder.setEvaluationDescription( evaluation )
                      .addBoxplotConsumerPerPair( DestinationType.GRAPHIC,
                                                  BoxPlotGraphicsWriter.of( outputs, outputsDirectory ) )
                      .build();
    }

    @Override
    public Function<Collection<Statistics>, Set<Path>> getGroupedConsumer( Evaluation evaluation, String evaluationId )
    {
        Objects.requireNonNull( evaluation );
        Objects.requireNonNull( evaluationId );

        Outputs outputs = evaluation.getOutputs();
        Path outputsDirectory = this.getPathToWrite( evaluationId );

        StatisticsToFormatsRouter.Builder builder = new StatisticsToFormatsRouter.Builder();

        // Note that diagrams are always written by group, even if all the statistics could be written per pool because
        // grouping is currently done per feature and not, for example, per pool within a feature (grouping over the 
        // various thresholds that are messaged separately). To allow for writing per pool, an additional layer of 
        // message grouping would be needed.       
        return builder.setEvaluationDescription( evaluation )
                      .addBoxplotConsumerPerPool( DestinationType.GRAPHIC,
                                                  BoxPlotGraphicsWriter.of( outputs, outputsDirectory ) )
                      .addDoubleScoreConsumer( DestinationType.GRAPHIC,
                                               DoubleScoreGraphicsWriter.of( outputs, outputsDirectory ) )
                      .addDurationScoreConsumer( DestinationType.GRAPHIC,
                                                 DurationScoreGraphicsWriter.of( outputs, outputsDirectory ) )
                      .addDiagramConsumer( DestinationType.GRAPHIC,
                                           DiagramGraphicsWriter.of( outputs, outputsDirectory ) )
                      .addDurationDiagramConsumer( DestinationType.GRAPHIC,
                                                   DurationDiagramGraphicsWriter.of( outputs, outputsDirectory ) )
                      .build();
    }

    @Override
    public Consumer getConsumerDescription()
    {
        return this.consumerDescription;
    }

    /**
     * Returns a path to write, creating a temporary directory for the outputs with the correct permissions, as needed. 
     *
     * @param evaluationId the evaluation identifier
     * @return the path to the temporary output directory
     * @throws ConsumerException if the temporary directory cannot be created
     * @throws NullPointerException if the evaluationId is null
     */

    private Path getPathToWrite( String evaluationId )
    {
        Objects.requireNonNull( evaluationId );

        // Where outputs files will be written
        Path outputDirectory = null;
        String tempDir = System.getProperty( "java.io.tmpdir" );

        try
        {
            Path namedPath = Paths.get( tempDir, "wres_evaluation_output_" + evaluationId );

            // POSIX-compliant    
            if ( FileSystems.getDefault().supportedFileAttributeViews().contains( "posix" ) )
            {
                Set<PosixFilePermission> permissions = EnumSet.of( PosixFilePermission.OWNER_READ,
                                                                   PosixFilePermission.OWNER_WRITE,
                                                                   PosixFilePermission.OWNER_EXECUTE,
                                                                   PosixFilePermission.GROUP_READ,
                                                                   PosixFilePermission.GROUP_WRITE,
                                                                   PosixFilePermission.GROUP_EXECUTE );

                FileAttribute<Set<PosixFilePermission>> fileAttribute =
                        PosixFilePermissions.asFileAttribute( permissions );

                // Create if not exists
                outputDirectory = Files.createDirectories( namedPath, fileAttribute );
            }
            // Not POSIX-compliant
            else
            {
                outputDirectory = Files.createDirectories( namedPath );
            }
        }
        catch ( IOException e )
        {
            throw new ConsumerException( "Encountered an error in subscriber " + this.getConsumerId()
                                         + " while attempting to create a temporary "
                                         + "directory for the graphics from evaluation "
                                         + evaluationId
                                         + ".",
                                         e );
        }

        // Render absolute
        if ( !outputDirectory.isAbsolute() )
        {
            outputDirectory = outputDirectory.toAbsolutePath();
        }

        LOGGER.debug( "While processing evaluation {} in subscriber {}, created output directory {}.",
                      evaluationId,
                      this.getConsumerId(),
                      outputDirectory );

        return outputDirectory;
    }

    /**
     * @return the subscriber identifier
     */

    private String getConsumerId()
    {
        return this.consumerDescription.getConsumerId();
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

}
