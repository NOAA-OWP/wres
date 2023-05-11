package wres.io.writing.csv.pairs;

import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.StringJoiner;
import java.util.function.DoubleFunction;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;

import wres.datamodel.Ensemble;
import wres.datamodel.pools.Pool;
import wres.datamodel.pools.PoolSlicer;
import wres.datamodel.time.TimeSeries;

/**
 * Class for writing a {@link Pool} that contains ensemble forecasts.
 * 
 * @author James Brown
 */

public class EnsemblePairsWriter extends PairsWriter<Double, Ensemble>
{
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

    @Override
    StringJoiner getHeaderFromPairs( Pool<TimeSeries<Pair<Double, Ensemble>>> pairs )
    {
        StringJoiner joiner = super.getHeaderFromPairs( pairs );

        String unit = pairs.getMetadata()
                           .getMeasurementUnit()
                           .getUnit();

        joiner.add( "LEFT IN " + unit );

        int pairCount = PoolSlicer.getPairCount( pairs );
        if ( pairCount > 0 )
        {
            int memberCount = this.getEnsembleMemberCount( pairs );

            for ( int i = 1; i <= memberCount; i++ )
            {
                joiner.add( "RIGHT MEMBER " + i + " IN " + unit );
            }
        }
        else
        {
            joiner.add( "RIGHT IN " + unit );
        }

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

    private EnsemblePairsWriter( Path pathToPairs, ChronoUnit timeResolution, DecimalFormat decimalFormatter, boolean gzip )
    {
        super( pathToPairs, timeResolution, EnsemblePairsWriter.getPairFormatter( decimalFormatter ), gzip );
    }

    /**
     * Returns the largest number of ensemble members in the input.
     * 
     * @param pairs the pairs
     * @return the largest number of ensemble members
     */

    private int getEnsembleMemberCount( Pool<TimeSeries<Pair<Double, Ensemble>>> pairs )
    {
        OptionalInt members = pairs.get()
                                   .stream()
                                   .flatMap( next -> next.getEvents().stream() )
                                   .mapToInt( next -> next.getValue().getRight().getMembers().length )
                                   .max();

        if ( members.isPresent() )
        {
            return members.getAsInt();
        }

        return 0;
    }

    /**
     * Returns the string formatter from the paired input using an optional {@link DecimalFormat}.
     *  
     * @param decimalFormatter the optional decimal formatter, may be null
     * @return the string formatter
     */

    private static Function<Pair<Double, Ensemble>, String> getPairFormatter( DecimalFormat decimalFormatter )
    {
        return pair -> {
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
            Arrays.stream( pair.getRight().getMembers() )
                  .forEach( nextMember -> joiner.add( handleNaNs.apply( nextMember ) ) );

            return joiner.toString();
        };
    }

}
