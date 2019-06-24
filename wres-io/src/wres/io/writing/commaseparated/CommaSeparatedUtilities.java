package wres.io.writing.commaseparated;

import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.sampledata.SampleMetadata;
import wres.datamodel.scale.TimeScale;
import wres.util.TimeHelper;

/**
 * Utility class that helps to write Comma Separated Values (CSV).
 *
 * @author james.brown@hydrosolved.com
 */
public class CommaSeparatedUtilities
{

    /**
     * Delimiter for the header.
     */

    static final String HEADER_DELIMITER = " ";

    /**
     * Returns a default header from the {@link SampleMetadata} to which additional information may be appended. Does
     * not include the valid time information and is therefore deprecated for removal once #57932 is complete, to
     * be replaced with {@link #getTimeWindowHeaderFromSampleMetadata(SampleMetadata, ChronoUnit)} in all circumstances,
     * including for pairs and statistics.
     *
     * @param sampleMetadata the sample metadata
     * @param durationUnits the duration units for lead times
     * @return default header information
     * @throws NullPointerException if either input is null
     */

    @Deprecated
    public static StringJoiner getPartialTimeWindowHeaderFromSampleMetadata( SampleMetadata sampleMetadata,
                                                                             ChronoUnit durationUnits )
    {
        StringJoiner fullWindow =
                CommaSeparatedUtilities.getTimeWindowHeaderFromSampleMetadata( sampleMetadata, durationUnits );
        
        String adaptedWindow = fullWindow.toString();
               
        // If valid times are explicitly set and the reference times are not set, then use the valid times
        // because there is only one pair of times until #57932. Fixes #58112
        // Again, *only* use valid times if the reference times are unbounded and the valid times not.
        if ( sampleMetadata.getTimeWindow().hasUnboundedReferenceTimes()
             && !sampleMetadata.getTimeWindow().hasUnboundedValidTimes() )
        {
            adaptedWindow = adaptedWindow.replace( "EARLIEST ISSUE TIME,", "" );
            adaptedWindow = adaptedWindow.replace( "LATEST ISSUE TIME,", "" );    
        }
        // Reference times are bounded
        else 
        {
            adaptedWindow = adaptedWindow.replace( "EARLIEST VALID TIME,", "" );
            adaptedWindow = adaptedWindow.replace( "LATEST VALID TIME,", "" );        
        }

        StringJoiner joiner = new StringJoiner( "," );
        joiner.add( adaptedWindow );
        
        return joiner;
    }
    
    /**
     * Returns a default header from the {@link SampleMetadata} to which additional information may be appended.
     *
     * @param sampleMetadata the sample metadata
     * @param durationUnits the duration units for lead times
     * @return default header information
     * @throws NullPointerException if either input is null
     */

    public static StringJoiner getTimeWindowHeaderFromSampleMetadata( SampleMetadata sampleMetadata,
                                                                      ChronoUnit durationUnits )
    {
        Objects.requireNonNull( sampleMetadata, "Cannot determine the default CSV header from null metadata." );

        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );

        StringJoiner joiner = new StringJoiner( "," );

        String timeScale = "";

        // Set the time scale string, unless instantaneous
        if ( sampleMetadata.hasTimeScale() )
        {
            TimeScale s = sampleMetadata.getTimeScale();

            if( s.isInstantaneous() )
            {
                timeScale = HEADER_DELIMITER+ s.toString();
            }
            else
            {
                timeScale = HEADER_DELIMITER
                            + "["
                            + s.getFunction()
                            + HEADER_DELIMITER
                            + "OVER"
                            + HEADER_DELIMITER
                            + "PAST"
                            + HEADER_DELIMITER
                            + TimeHelper.durationToLongUnits( s.getPeriod(),
                                                              durationUnits )
                            + HEADER_DELIMITER
                            + durationUnits.name()
                            + "]";
            }
        }

        joiner.add( "EARLIEST ISSUE TIME" )
              .add( "LATEST ISSUE TIME" )
              .add( "EARLIEST VALID TIME" )
              .add( "LATEST VALID TIME" )
              .add( "EARLIEST" + HEADER_DELIMITER
                    + "LEAD"
                    + HEADER_DELIMITER
                    + "TIME"
                    + HEADER_DELIMITER
                    + "IN"
                    + HEADER_DELIMITER
                    + durationUnits.name()
                    + timeScale )
              .add( "LATEST" + HEADER_DELIMITER
                    + "LEAD"
                    + HEADER_DELIMITER
                    + "TIME"
                    + HEADER_DELIMITER
                    + "IN"
                    + HEADER_DELIMITER
                    + durationUnits.name()
                    + timeScale );

        return joiner;
    }    

}
