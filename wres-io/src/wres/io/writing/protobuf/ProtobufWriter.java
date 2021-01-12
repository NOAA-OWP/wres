package wres.io.writing.protobuf;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.jcip.annotations.GuardedBy;
import net.jcip.annotations.Immutable;
import wres.io.writing.WriteException;
import wres.io.writing.commaseparated.statistics.CommaSeparatedWriteException;
import wres.statistics.generated.Evaluation;
import wres.statistics.generated.Statistics;
import wres.statistics.generated.EvaluationStatus;

/**
 * <p>Writes an evaluation in protobuf format. The evaluation is represented by a sequence of messages. The first 
 * message contains a description of the {@link Evaluation}. All subsequent messages contain {@link Statistics}, each 
 * message containing one pool of statistics. Messages that contain {@link EvaluationStatus} events are not written. 
 * 
 * <p>The expected pattern for writing is one writer per evaluation, which encapsulates one path to write.
 *
 * @author james.brown@hydrosolved.com
 */

@Immutable
public class ProtobufWriter implements Function<Statistics,Set<Path>>
{

    private static final Logger LOGGER = LoggerFactory.getLogger( ProtobufWriter.class );

    /**The path to write.*/
    @GuardedBy( "lock" )
    private final Path path;

    /**A lock that guards the path to write.*/
    private final ReentrantLock lock;

    /**
     * Appends a statistics message to the path encapsulated by this writer.
     * 
     * @param statistics the statistics to write
     * @throws ProtobufWriteException if the statistics could not be written
     * @throws NullPointerException if the input is null
     */

    @Override
    public Set<Path> apply( Statistics statistics )
    {
        Objects.requireNonNull( statistics );

        LOGGER.debug( "Writing a statistics message to {}.", this.getPathToWrite() );

        this.getLockForWriting().lock();

        try ( OutputStream fileOut = Files.newOutputStream( this.getPathToWrite(),
                                                            StandardOpenOption.APPEND );
              OutputStream buffer = new BufferedOutputStream( fileOut ) )
        {
            // Write with framing
            statistics.writeDelimitedTo( buffer );
        }
        catch ( IOException e )
        {
            throw new ProtobufWriteException( "While writing an evaluation message to " + this.getPathToWrite()
                                              + ", encountered an exception.",
                                              e );
        }
        finally
        {
            this.getLockForWriting().unlock();
        }

        LOGGER.debug( "Finished writing a statistics message to {}.", this.getPathToWrite() );
        
        return Set.of( this.getPathToWrite() );
    }

    /**
     * Creates an instance of a writer with a path to write.
     * 
     * @param path the path to write
     * @param evaluation the evaluation description
     * @return a writer instance
     * @throws ProtobufWriteException if the evaluation could not be written to the path
     * @throws NullPointerException if any input is null
     */

    public static ProtobufWriter of( Path path, Evaluation evaluation )
    {
        return new ProtobufWriter( path, evaluation );
    }

    /**
     * Constructor.
     *
     * @param path the path to write
     * @param evaluation the evaluation description
     * @throws ProtobufWriteException if the evaluation could not be written
     * @throws NullPointerException if any input is null
     */

    private ProtobufWriter( Path path, Evaluation evaluation )
    {
        Objects.requireNonNull( path, "Specify a non-null path for writing." );
        Objects.requireNonNull( evaluation, "Specify a non-null evaluation to write." );

        this.path = path;
        this.lock = new ReentrantLock();
        this.writeEvaluation( path, evaluation );
    }

    /**
     * Writes the evaluation message to the path.
     * 
     * @param path the path to write
     * @param evaluation the evaluation description
     * @throws ProtobufWriteException if the evaluation could not be written
     */

    private void writeEvaluation( Path path, Evaluation evaluation )
    {
        LOGGER.debug( "Writing an evaluation message to {}.", path );

        this.getLockForWriting().lock();

        try ( OutputStream fileOut = Files.newOutputStream( path,
                                                            StandardOpenOption.CREATE,
                                                            StandardOpenOption.TRUNCATE_EXISTING );
              OutputStream buffer = new BufferedOutputStream( fileOut ) )
        {
            // Write with framing
            evaluation.writeDelimitedTo( buffer );
        }
        catch ( IOException e )
        {
            throw new ProtobufWriteException( "While writing an evaluation message to " + path
                                              + ", encountered an exception.",
                                              e );
        }
        finally
        {
            this.getLockForWriting().unlock();
        }

        LOGGER.debug( "Finished writing an evaluation message to {}.", path );
    }

    /**
     * @return the lock for writing
     */

    private ReentrantLock getLockForWriting()
    {
        return this.lock;
    }

    /**
     * @return the path to write.
     */

    private Path getPathToWrite()
    {
        return this.path;
    }

    /**
     * A runtime exception associated with writing metric outputs of Comma Separated Values (CSV).
     * 
     * @author james.brown@hydrosolved.com
     */

    private static class ProtobufWriteException extends WriteException
    {
        /** Serial identifier.*/

        private static final long serialVersionUID = 310749020741932142L;

        /**
         * Constructs a {@link CommaSeparatedWriteException} with the specified message.
         * 
         * @param message the message.
         * @param cause the cause of the exception
         */

        public ProtobufWriteException( final String message, final Throwable cause )
        {
            super( message, cause );
        }
    }
}
