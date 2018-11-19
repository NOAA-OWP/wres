package wres.io.writing;

import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.sampledata.SampleData;
import wres.datamodel.sampledata.pairs.TimeSeriesOfEnsemblePairs;
import wres.datamodel.sampledata.pairs.TimeSeriesOfSingleValuedPairs;
import wres.io.writing.commaseparated.pairs.EnsemblePairsWriter;
import wres.io.writing.commaseparated.pairs.PairsWriter;
import wres.io.writing.commaseparated.pairs.SingleValuedPairsWriter;

/**
 * A temporary class intended to store parameterized types of {@link PairsWriter}. Currently, wres-io creates 
 * some friction by exposing the wildcard type of {@link SampleData}, rather than a non-wildcard type. This class 
 * resolves that friction by providing access to parameterized types of writing on request.
 * 
 * TODO: remove this class in favor of the direct application of a parameterized {@link PairsWriter} once 
 * the wres-io uses non-wildcard types of {@link SampleData}.
 * 
 * @author james.brown@hydrosolved.com
 */
public class SharedSampleDataWriters implements Consumer<SampleData<?>>, Supplier<Set<Path>>
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( SharedSampleDataWriters.class );
    
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
        Set<Path> returnMe = new HashSet<>( singleValuedWriter.get() );
        returnMe.addAll( ensembleWriter.get() );

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * Writes the pairs.
     * 
     * @throws NullPointerException if the input is null or required metadata is null
     * @throws IllegalArgumentException if the input is an unexpected type
     */

    @Override
    public void accept( SampleData<?> sampleData )
    {
        if ( sampleData instanceof TimeSeriesOfSingleValuedPairs )
        {
            this.getSingleValuedWriter().accept( (TimeSeriesOfSingleValuedPairs) sampleData );
        }
        else if ( sampleData instanceof TimeSeriesOfEnsemblePairs )
        {
            this.getEnsembleWriter().accept( (TimeSeriesOfEnsemblePairs) sampleData );
        }
        else
        {
            throw new IllegalArgumentException( "Unsupported type of pairs for writing." );
        }
        
        LOGGER.debug( "Completed writing pairs for feature '{}' and time window {} to {}.",
                      sampleData.getMetadata().getIdentifier().getGeospatialID(),
                      sampleData.getMetadata().getTimeWindow(),
                      this.get() );
    }

    /**
     * Returns the single-valued writer.
     * 
     * @return the single-valued writer
     */

    private SingleValuedPairsWriter getSingleValuedWriter()
    {
        return this.singleValuedWriter;
    }

    /**
     * Returns the single-valued writer.
     * 
     * @return the single-valued writer
     */

    private EnsemblePairsWriter getEnsembleWriter()
    {
        return this.ensembleWriter;
    }

    /**
     * Hidden constructor.
     * 
     * @param outputPath the required output path
     * @param timeResolution the required time resolution for writing pairs
     * @param decimalFormatter the optional decimal formatter
     * @return the container of shared writers
     * @throws NullPointerException if the outputPath or timeResolution is null
     */

    private SharedSampleDataWriters( Path outputPath, ChronoUnit timeResolution, DecimalFormat decimalFormatter )
    {
        this.singleValuedWriter = SingleValuedPairsWriter.of( outputPath, timeResolution, decimalFormatter );

        this.ensembleWriter = EnsemblePairsWriter.of( outputPath, timeResolution, decimalFormatter );
    }

}
