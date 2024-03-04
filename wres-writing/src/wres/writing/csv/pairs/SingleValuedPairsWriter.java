package wres.writing.csv.pairs;

import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Objects;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.DoubleFunction;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.pools.Pool;
import wres.datamodel.time.TimeSeries;

/**
 * Class for writing a {@link Pool} that contains single-valued pairs.
 *
 * @author James Brown
 */

public class SingleValuedPairsWriter extends PairsWriter<Double, Double>
{
    /**
     * Build an instance of a writer.
     *
     * @param pathToPairs the path to write, required
     * @param timeResolution the time resolution at which to write datetime and duration information, required
     * @return the writer
     * @throws NullPointerException if either input is null
     */

    public static SingleValuedPairsWriter of( Path pathToPairs,
                                              ChronoUnit timeResolution )
    {
        return new SingleValuedPairsWriter( pathToPairs, timeResolution, null, false );
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

    public static SingleValuedPairsWriter of( Path pathToPairs,
                                              ChronoUnit timeResolution,
                                              DecimalFormat decimalFormatter )
    {
        return new SingleValuedPairsWriter( pathToPairs, timeResolution, decimalFormatter, false );
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

    public static SingleValuedPairsWriter of( Path pathToPairs,
                                              ChronoUnit timeResolution,
                                              DecimalFormat decimalFormatter,
                                              boolean gzip )
    {
        return new SingleValuedPairsWriter( pathToPairs, timeResolution, decimalFormatter, gzip );
    }

    @Override
    SortedSet<String> getRightValueNames()
    {
        // Default/empty names
        return Collections.unmodifiableSortedSet( new TreeSet<>() );
    }

    @Override
    StringJoiner getHeaderFromPairs( Pool<TimeSeries<Pair<Double, Double>>> pairs )
    {
        StringJoiner joiner = super.getHeaderFromPairs( pairs );

        joiner.add( "LEFT IN " + pairs.getMetadata().getMeasurementUnit().getUnit() );

        joiner.add( "RIGHT IN " + pairs.getMetadata().getMeasurementUnit().getUnit() );

        return joiner;
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

    private SingleValuedPairsWriter( Path pathToPairs,
                                     ChronoUnit timeResolution,
                                     DecimalFormat decimalFormatter,
                                     boolean gzip )
    {
        super( pathToPairs,
               timeResolution,
               SingleValuedPairsWriter.getPairFormatter( decimalFormatter ),
               gzip );
    }

    /**
     * Returns the string formatter from the paired input using an optional {@link DecimalFormat}.
     *
     * @param decimalFormatter the optional decimal formatter, may be null
     * @return the string formatter
     */

    private static BiFunction<SortedSet<String>, Pair<Double, Double>, String> getPairFormatter( DecimalFormat decimalFormatter )
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

            // Add right
            joiner.add( handleNaNs.apply( pair.getRight() ) );

            return joiner.toString();

        };
    }

}
