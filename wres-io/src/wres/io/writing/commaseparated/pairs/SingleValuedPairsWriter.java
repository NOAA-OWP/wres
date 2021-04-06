package wres.io.writing.commaseparated.pairs;

import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.pools.Pool;

/**
 * Class for writing a {@link Pool} that contains single-valued pairs.
 * 
 * @author james.brown@hydrosolved.com
 */

public class SingleValuedPairsWriter extends PairsWriter<Double, Double>
{

    /**
     * Build an instance of a writer.
     * 
     * @param pathToPairs the path to write
     * @param timeResolution the time resolution at which to write datetime and duration information
     * @return the writer
     * @throws NullPointerException if either input is null
     */

    public static SingleValuedPairsWriter of( Path pathToPairs,
                                              ChronoUnit timeResolution )
    {
        return new SingleValuedPairsWriter( pathToPairs, timeResolution, null );
    }

    /**
     * Build an instance of a writer.
     * 
     * @param pathToPairs the path to write
     * @param timeResolution the time resolution at which to write datetime and duration information
     * @param decimalFormatter the optional formatter for writing decimal values
     * @return the writer
     * @throws NullPointerException if the pathToPairs is null or the timeResolution is null
     */

    public static SingleValuedPairsWriter of( Path pathToPairs,
                                              ChronoUnit timeResolution,
                                              DecimalFormat decimalFormatter )
    {
        return new SingleValuedPairsWriter( pathToPairs, timeResolution, decimalFormatter );
    }

    @Override
    StringJoiner getHeaderFromPairs( Pool<Pair<Double, Double>> pairs )
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
     * @throws NullPointerException if any of the expected inputs is null
     */

    private SingleValuedPairsWriter( Path pathToPairs,
                                     ChronoUnit timeResolution,
                                     DecimalFormat decimalFormatter )
    {
        super( pathToPairs,
               timeResolution,
               SingleValuedPairsWriter.getPairFormatter( decimalFormatter ) );
    }

    /**
     * Returns the string formatter from the paired input using an optional {@link DecimalFormat}.
     *  
     * @param decimalFormatter the optional decimal formatter, may be null
     * @return the string formatter
     */

    private static Function<Pair<Double, Double>, String> getPairFormatter( DecimalFormat decimalFormatter )
    {
        return pair -> {

            StringJoiner joiner = new StringJoiner( PairsWriter.DELIMITER );

            Function<Double, String> handleNaNs = input -> {
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
