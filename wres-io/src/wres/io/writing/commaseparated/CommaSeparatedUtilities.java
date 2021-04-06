package wres.io.writing.commaseparated;

import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.StringJoiner;

import wres.datamodel.pools.PoolMetadata;
import wres.datamodel.scale.TimeScaleOuter;
import wres.statistics.generated.Pool;
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
     * Returns a default header from the {@link PoolMetadata} to which additional information may be appended.
     *
     * @param sampleMetadata the sample metadata
     * @param durationUnits the duration units for lead times
     * @return default header information
     * @throws NullPointerException if either input is null
     */

    public static StringJoiner getTimeWindowHeaderFromSampleMetadata( PoolMetadata sampleMetadata,
                                                                      ChronoUnit durationUnits )
    {
        Objects.requireNonNull( sampleMetadata, "Cannot determine the default CSV header from null metadata." );

        Objects.requireNonNull( durationUnits, "Specify non-null duration units." );

        StringJoiner joiner = new StringJoiner( "," );

        String timeScale = "";

        // Set the time scale string, unless instantaneous
        if ( sampleMetadata.hasTimeScale() )
        {
            TimeScaleOuter s = sampleMetadata.getTimeScale();

            if ( s.isInstantaneous() )
            {
                timeScale = HEADER_DELIMITER + s.toString();
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

    /**
     * Returns the name of a geographic feature from an instance of {@link PoolMetadata}.
     * 
     * @param metadata the metadata
     * @return name the feature name
     * @throws NullPointerException if the input is null
     */

    public static String getFeatureNameFromMetadata( PoolMetadata metadata )
    {
        Objects.requireNonNull( metadata );

        String featureName = "UNKNOWN";

        Pool pool = metadata.getPool();

        if ( Objects.nonNull( pool ) && pool.getGeometryTuplesCount() > 0 )
        {
            // TODO: decide if "right" is sufficient or a combination is better.
            featureName = pool.getGeometryTuples( 0 )
                              .getRight()
                              .getName();
        }

        return featureName;
    }

}
