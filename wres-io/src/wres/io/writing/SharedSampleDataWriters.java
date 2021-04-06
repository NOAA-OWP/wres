package wres.io.writing;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

import wres.datamodel.pools.Pool;
import wres.io.writing.commaseparated.pairs.EnsemblePairsWriter;
import wres.io.writing.commaseparated.pairs.SingleValuedPairsWriter;

/**
 * A temporary class intended to host pairs writers. Currently, wres-io creates 
 * some friction by exposing the wildcard type of {@link Pool}, rather than 
 * a non-wildcard type. This class resolves that friction by providing access to 
 * parameterized types of writing on request.
 * 
 * TODO: remove this class in favor of the direct application of a parameterized 
 * {@link SingleValuedPairsWriter} or {@link EnsemblePairsWriter} once the wres-io 
 * uses non-wildcard types of {@link Pool}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SharedSampleDataWriters implements Supplier<Set<Path>>, Closeable
{

    /**
     * The shared writer for single-valued pairs.
     */

    private final SingleValuedPairsWriter singleValuedWriter;

    /**
     * The shared writer for ensemble pairs.
     */

    private final EnsemblePairsWriter ensembleWriter;

    /**
     * Return an instance.
     * 
     * @param outputPath the required output path
     * @param timeResolution the required time resolution for writing pairs
     * @param decimalFormatter the optional decimal formatter
     * @return the container of shared writers
     * @throws NullPointerException if the outputPath or timeResolution is null
     */

    public static SharedSampleDataWriters
            of( Path outputPath, ChronoUnit timeResolution, DecimalFormat decimalFormatter )
    {
        return new SharedSampleDataWriters( outputPath, timeResolution, decimalFormatter );
    }

    @Override
    public Set<Path> get()
    {
        Set<Path> returnMe = new HashSet<>();
        
        returnMe.addAll( this.singleValuedWriter.get() );
        returnMe.addAll( this.ensembleWriter.get() );
        
        return Collections.unmodifiableSet( returnMe );
    }

    @Override
    public void close() throws IOException
    {
        // Try to close the first writer
        try
        {
            if ( Objects.nonNull( this.singleValuedWriter ) )
            {
                this.singleValuedWriter.close();
            }
        }
        // Failed, but try to close the second writer
        catch ( IOException e )
        {
            if ( Objects.nonNull( this.ensembleWriter ) )
            {
                this.ensembleWriter.close();
            }

            throw new IOException( "On attempting to close writer instances:", e );
        }

        // Close the second writer
        if ( Objects.nonNull( this.ensembleWriter ) )
        {
            this.ensembleWriter.close();
        }
    }

    /**
     * Returns the single-valued writer.
     * 
     * @return the single-valued writer
     */

    public SingleValuedPairsWriter getSingleValuedWriter()
    {
        return this.singleValuedWriter;
    }

    /**
     * Returns the single-valued writer.
     * 
     * @return the single-valued writer
     */

    public EnsemblePairsWriter getEnsembleWriter()
    {
        return this.ensembleWriter;
    }

    /**
     * Hidden constructor.
     * 
     * @param outputPath the required output path
     * @param timeResolution the required time resolution for writing pairs
     * @param decimalFormatter the optional decimal formatter
     * @throws NullPointerException if the outputPath or timeResolution is null
     */
    private SharedSampleDataWriters( Path outputPath,
                                     ChronoUnit timeResolution,
                                     DecimalFormat decimalFormatter )
    {
        this.singleValuedWriter = SingleValuedPairsWriter.of( outputPath,
                                                              timeResolution,
                                                              decimalFormatter );

        this.ensembleWriter = EnsemblePairsWriter.of( outputPath, timeResolution, decimalFormatter );
    }

}
