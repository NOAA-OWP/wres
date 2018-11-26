package wres.io.writing.commaseparated.pairs;

import java.nio.file.Path;
import java.text.DecimalFormat;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Objects;
import java.util.StringJoiner;
import java.util.function.Function;

import wres.datamodel.metadata.TimeScale;
import wres.datamodel.sampledata.pairs.EnsemblePair;
import wres.datamodel.sampledata.pairs.TimeSeriesOfEnsemblePairs;

/**
 * Class for writing {@link TimeSeriesOfEnsemblePairs}.
 * 
 * @author james.brown@hydrosolved.com
 */

public class EnsemblePairsWriter extends PairsWriter<EnsemblePair,TimeSeriesOfEnsemblePairs>
{

    /**
     * Build an instance of a writer.
     * 
     * @param pathToPairs the path to write
     * @param timeResolution the time resolution at which to write datetime and duration information
     * @return the writer
     * @throws NullPointerException if either input is null
     */

    public static EnsemblePairsWriter of( Path pathToPairs, ChronoUnit timeResolution )
    {
        return new EnsemblePairsWriter( pathToPairs, timeResolution, null );
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

    public static EnsemblePairsWriter of( Path pathToPairs, ChronoUnit timeResolution, DecimalFormat decimalFormatter )
    {
        return new EnsemblePairsWriter( pathToPairs, timeResolution, decimalFormatter );
    }

    @Override
    String getHeaderFromPairs( TimeSeriesOfEnsemblePairs pairs )
    {
        Objects.requireNonNull( pairs, "Cannot obtain header from null pairs." );

        StringJoiner joiner = new StringJoiner( "," );

        joiner.add( "FEATURE DESCRIPTION" )
              .add( "VALID TIME" );

        if ( pairs.getMetadata().hasTimeScale() )
        {
            TimeScale timeScale = pairs.getMetadata().getTimeScale();

            joiner.add( "LEAD DURATION IN " + this.getTimeResolution().toString().toUpperCase()
                        + " ["
                        + timeScale.getFunction()
                        + " OVER PAST "
                        + timeScale.getPeriod().get( this.getTimeResolution() )
                        + " "
                        + this.getTimeResolution().toString().toUpperCase()
                        + "]" );
        }
        else
        {
            joiner.add( "LEAD DURATION IN " + this.getTimeResolution().toString().toUpperCase() );
        }

        joiner.add( "LEFT IN " + pairs.getMetadata().getMeasurementUnit().getUnit() );

        if ( !pairs.getRawData().isEmpty() )
        {
            int memberCount = pairs.getRawData().get( 0 ).getRight().length;
            for ( int i = 1; i <= memberCount; i++ )
            {
                joiner.add( "RIGHT MEMBER " + i + " IN " + pairs.getMetadata().getMeasurementUnit().getUnit() );
            }
        }
        else
        {
            joiner.add( "RIGHT IN " + pairs.getMetadata().getMeasurementUnit().getUnit() );
        }

        return joiner.toString();
    }

    /**
     * Hidden constructor.
     * 
     * @param pathToPairs the path to write
     * @param timeResolution the time resolution at which to write datetime and duration information
     * @param decimalFormatter the optional formatter for writing decimal values
     * @throws NullPointerException if any of the expected inputs is null
     */

    private EnsemblePairsWriter( Path pathToPairs, ChronoUnit timeResolution, DecimalFormat decimalFormatter )
    {
        super( pathToPairs, timeResolution, EnsemblePairsWriter.getPairFormatter( decimalFormatter ) );
    }

    /**
     * Returns the string formatter from the paired input using an optional {@link DecimalFormat}.
     *  
     * @param decimalFormatter the optional decimal formatter, may be null
     * @return the string formatter
     */

    private static Function<EnsemblePair, String> getPairFormatter( DecimalFormat decimalFormatter )
    {
        return ( pair ) -> {

            StringJoiner joiner = new StringJoiner( PairsWriter.DELIMITER );


            // Format left and right values
            if ( Objects.nonNull( decimalFormatter ) )
            {
                joiner.add( decimalFormatter.format( pair.getLeft() ) );

                // Add the ensemble members
                Arrays.stream( pair.getRight() )
                      .forEach( nextMember -> joiner.add( decimalFormatter.format( nextMember ) ) );

            }
            // No format
            else
            {
                joiner.add( Double.toString( pair.getLeft() ) );

                // Add the ensemble members
                Arrays.stream( pair.getRight() )
                      .forEach( nextMember -> joiner.add( Double.toString( nextMember ) ) );
            }

            return joiner.toString();
        };
    }
    
}
