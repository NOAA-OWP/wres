package wres.io.writing.csv.pairs;

import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.DoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.Ensemble;
import wres.datamodel.MissingValues;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.time.TimeSeries;
import wres.io.writing.WriteException;

/**
 * Class for writing a {@link Pool} that contains ensemble forecasts. This writer must be primed for writing before
 * the header is written because the ensemble data structure may be sparse/non-rectangular whereby successive pools
 * compose a different set of ensemble members. Since this writer abstracts a rectangular/columnar format, the superset
 * of column names (ensemble member identifiers) must be supplied between instantiation time (when this information is
 * unknown) and write time using {@link #prime(SortedSet)}.
 *
 * @author James Brown
 */

public class EnsemblePairsWriter extends PairsWriter<Double, Ensemble>
{
    /** Whether the writer has been primed. */
    private final AtomicBoolean primed = new AtomicBoolean();

    /** The ensemble member names to write. */
    private SortedSet<String> ensembleNames;

    /**
     * Build an instance of a writer.
     *
     * @param pathToPairs the path to write, required
     * @param timeResolution the time resolution at which to write datetime and duration information, required
     * @return the writer
     * @throws NullPointerException if either input is null
     */

    public static EnsemblePairsWriter of( Path pathToPairs, ChronoUnit timeResolution )
    {
        return new EnsemblePairsWriter( pathToPairs, timeResolution, null, false );
    }

    /**
     * Build an instance of a writer.
     *
     * @param pathToPairs the path to write, required
     * @param timeResolution the time resolution at which to write datetime and duration information, required
     * @param decimalFormatter the optional formatter for writing decimal values, optional
     * @return the writer
     * @throws NullPointerException if any of the required inputs is null
     */

    public static EnsemblePairsWriter of( Path pathToPairs, ChronoUnit timeResolution, DecimalFormat decimalFormatter )
    {
        return new EnsemblePairsWriter( pathToPairs, timeResolution, decimalFormatter, false );
    }

    /**
     * Build an instance of a writer.
     *
     * @param pathToPairs the path to write, required
     * @param timeResolution the time resolution at which to write datetime and duration information, required
     * @param decimalFormatter the optional formatter for writing decimal values, optional
     * @param gzip boolean to determine if output should be gzip
     * @return the writer
     * @throws NullPointerException if any of the required inputs is null
     */

    public static EnsemblePairsWriter of( Path pathToPairs,
                                          ChronoUnit timeResolution,
                                          DecimalFormat decimalFormatter,
                                          boolean gzip )
    {
        return new EnsemblePairsWriter( pathToPairs, timeResolution, decimalFormatter, gzip );
    }

    /**
     * Prime this writer with the ensemble member names that can be expected across all pools to be written. This
     * method must be called before any other information is written to the file, otherwise the writer will enter an
     * exceptional state.
     * @param ensembleNames the ensemble names to write
     */

    public void prime( SortedSet<String> ensembleNames )
    {
        Objects.requireNonNull( ensembleNames );

        this.ensembleNames = Collections.unmodifiableSortedSet( new TreeSet<>( ensembleNames ) );
        this.primed.set( true );
    }

    @Override
    StringJoiner getHeaderFromPairs( Pool<TimeSeries<Pair<Double, Ensemble>>> pairs )
    {
        if( ! this.primed.get() )
        {
            throw new WriteException( "The ensemble pair writer has not been primed for writing." );
        }

        StringJoiner joiner = super.getHeaderFromPairs( pairs );

        String unit = pairs.getMetadata()
                           .getMeasurementUnit()
                           .getUnit();

        joiner.add( "LEFT IN " + unit );

        int pairCount = PoolSlicer.getPairCount( pairs );
        if ( pairCount > 0 )
        {
            for ( String nextName : this.getRightValueNames() )
            {
                joiner.add( "RIGHT MEMBER " + nextName + " IN " + unit );
            }
        }
        else
        {
            joiner.add( "RIGHT IN " + unit );
        }

        return joiner;
    }

    @Override
    SortedSet<String> getRightValueNames()
    {
        return this.ensembleNames;
    }

    /**
     * Hidden constructor.
     *
     * @param pathToPairs the path to write
     * @param timeResolution the time resolution at which to write datetime and duration information
     * @param decimalFormatter the optional formatter for writing decimal values
     * @param gzip boolean to determine if output should be gzip
     * @throws NullPointerException if any of the expected inputs is null
     */

    private EnsemblePairsWriter( Path pathToPairs,
                                 ChronoUnit timeResolution,
                                 DecimalFormat decimalFormatter,
                                 boolean gzip )
    {
        super( pathToPairs,
               timeResolution,
               EnsemblePairsWriter.getPairFormatter( decimalFormatter ),
               gzip );
    }

    /**
     * Returns the string formatter from the paired input using an optional {@link DecimalFormat}.
     *
     * @param decimalFormatter the optional decimal formatter, may be null
     * @return the string formatter
     */

    private static BiFunction<SortedSet<String>, Pair<Double, Ensemble>, String> getPairFormatter( DecimalFormat decimalFormatter )
    {
        return ( columnNames, pair ) -> {
            StringJoiner joiner = new StringJoiner( PairsWriter.DELIMITER );

            DoubleFunction<String> handleNaNs = input -> {
                if ( Double.isNaN( input ) || Objects.isNull( decimalFormatter ) )
                {
                    return Double.toString( input );
                }

                return decimalFormatter.format( input );
            };

            // Add left
            joiner.add( handleNaNs.apply( pair.getLeft() ) );

            // Add right members
            Ensemble ensemble = pair.getRight();
            if ( ensemble.hasLabels() )
            {
                // Iterate the members are put a missing inband when the member label is missing
                Ensemble.Labels labels = ensemble.getLabels();
                for ( String name : columnNames )
                {
                    if ( labels.hasLabel( name ) )
                    {
                        double doubleValue = ensemble.getMember( name );
                        String stringValue = handleNaNs.apply( doubleValue );
                        joiner.add( stringValue );
                    }
                    else
                    {
                        String stringValue = handleNaNs.apply( MissingValues.DOUBLE );
                        joiner.add( stringValue );
                    }
                }
            }
            // No labels, so write the structure as it appears. If there are missings, this will be non-rectangular
            else
            {
                Arrays.stream( pair.getRight()
                                   .getMembers() )
                      .forEach( nextMember -> joiner.add( handleNaNs.apply( nextMember ) ) );
            }

            return joiner.toString();
        };
    }
}
