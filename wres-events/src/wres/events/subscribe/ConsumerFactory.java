package wres.events.subscribe;

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

import wres.statistics.generated.Consumer;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Statistics;

/**
 * <p>An interface that supplies consumers for evaluation statistics. The interface supplies two types of consumers:
 * 
 * <ol>
 * <li>Consumers that consume one pool of statistics at a time. In other words, consumption happens immediately.</li>
 * <li>Consumers that consume multiple pools of statistics associated with a message group, such as a geographic 
 * feature. In other words, consumption is deferred until all statistics are available.</li>
 * </ol>
 * 
 * 
 * <p>A consumer is supplied at evaluation time based on an {@link Evaluation} description. Consumers are supplied at 
 * evaluation time because the consumers may depend on the evaluation description, either to determine consumers 
 * required or to use the description of the evaluation to qualify the statistics consumed.
 * 
 * <p>Each consumer consumes a collection of {@link Statistics} and returns a set of {@link Path} mutated. The consumer
 * may contain one or more underlying consumers that each consume the same statistics.
 * 
 * <p><b>Implementation notes:</b>
 * 
 * <p>At least one of the methods of this interface should return a non-trivial consumer. Both methods should return a
 * non-trivial consumer if both styles of consumption are required (incremental and grouped). For example, if the 
 * consumption abstracts a single consumer and that consumer writes to a numerical format whereby each pool of 
 * statistics can be written as it arrives, then the {@link #getGroupedConsumer(Evaluation, String)} may return a
 * trivial consumer as follows:
 * 
 * <p><code>return statistics {@literal ->} Set.of();</code>
 * 
 * @author james.brown@hydrosolved.com
 */

public interface ConsumerFactory
{

    /**
     * Creates a consumer for a given evaluation description.
     * 
     * @param evaluation the evaluation description
     * @param evaluationId the evaluation identifier
     * @return a consumer
     * @throws ConsumerException if the consumer could not be created for any reason
     */

    Function<Collection<Statistics>, Set<Path>> getConsumer( Evaluation evaluation, String evaluationId );

    /**
     * Creates a consumer of grouped statistics for a given evaluation description.
     * 
     * @param evaluation the evaluation description
     * @param evaluationId the evaluation identifier
     * @return a grouped consumer
     * @throws ConsumerException if the consumer could not be created for any reason
     */

    Function<Collection<Statistics>, Set<Path>> getGroupedConsumer( Evaluation evaluation, String evaluationId );

    /**
     * Returns a basic description of the consumers that are created by this factory, including the formats they offer.
     * 
     * @return the consumer description
     */

    Consumer getConsumerDescription();

    /**
     * Returns a path to write, creating a temporary directory for the outputs with the correct permissions, as needed. 
     *
     * @param evaluationId the evaluation identifier
     * @param consumerId the consumer identifier used to help with messaging
     * @return the path to the temporary output directory
     * @throws ConsumerException if the temporary directory cannot be created
     * @throws NullPointerException if any input is null
     */

    static Path getPathToWrite( String evaluationId,
                                String consumerId )
    {
        Objects.requireNonNull( evaluationId );
        Objects.requireNonNull( consumerId );

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
            throw new ConsumerException( "Encountered an error in subscriber " + consumerId
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

        return outputDirectory;
    }

}
