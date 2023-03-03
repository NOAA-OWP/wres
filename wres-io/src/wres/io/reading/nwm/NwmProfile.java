package wres.io.reading.nwm;

import java.time.Duration;
import java.util.Objects;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Represents a profile of a NWM forecast
 */
class NwmProfile
{
    private static final String MUST_HAVE_POSITIVE_TIMESTEP_DURATION_NOT =
            "Must have positive timestep duration, not ";

    @Override
    public String toString()
    {
        return new ToStringBuilder( this, ToStringStyle.SHORT_PREFIX_STYLE )
                .append( "blobCount", blobCount )
                .append( "memberCount", memberCount )
                .append( "durationBetweenValidDatetimes",
                         durationBetweenValidDatetimes )
                .append( "isVector", isVector )
                .append( "timeLabel", timeLabel )
                .append( "nwmConfiguration", nwmConfiguration )
                .append( "nwmOutputType", nwmOutputType )
                .append( "nwmSubdirectoryPrefix", nwmSubdirectoryPrefix )
                .append( "nwmLocationLabel", nwmLocationLabel )
                .append( "isEnsembleLike", isEnsembleLike )
                .append( "durationPastMidnight", durationPastMidnight )
                .append( "durationBetweenReferenceDatetimes",
                         durationBetweenReferenceDatetimes )
                .toString();
    }

    enum TimeLabel
    {
        F,
        TM;

        @Override
        public String toString()
        {
            return this.name()
                       .toLowerCase();
        }
    }

    /** Name of the global attribute containing a reference datetime */
    private static final String REFERENCE_DATETIME_VARIABLE = "reference_time";

    /** Name of the variable that contains feature ids */
    private static final String FEATURE_VARIABLE = "feature_id";

    /** Name of the variable that contains the valid datetime */
    private static final String VALID_DATETIME_VARIABLE = "time";

    /** Name of the variable that contains the ensemble member number */
    private static final String MEMBER_ATTRIBUTE = "ensemble_member_number";

    /** Count of how many blobs comprise a single forecast for each trace */
    private final int blobCount;

    /** Count of how many members are in this forecast. (1 if no ensemble) */
    private final int memberCount;

    /** Width of a time step between each valid datetime in the forecast */
    private final Duration durationBetweenValidDatetimes;

    /** True when vector, false when gridded */
    private final boolean isVector;

    /**
     * The time label used by this NWM timeseries, "f" for forecast, "tm" for
     * analysis_assimilation.
     */
    private final TimeLabel timeLabel;

    /**
     * The class of timeseries, e.g. analysis_assim_long, medium_range.
     * Does not include the "member" label.
     */
    private final String nwmConfiguration;

    /**
     * The type of timeseries, e.g. land, reservoir, channel_rt.
     */
    private final String nwmOutputType;

    /**
     * The subdirectory in which the data is found, e.g. analysis_assim_extend
     * or forcing_analysis_assim_extend or short_range_hawaii. This differs
     * from the nwmConfiguration, and is extended in an ensemble dataset, e.g.
     * medium_range becomes medium_range_mem4.
     */
    private final String nwmSubdirectoryPrefix;

    /**
     * The location label, e.g. "conus" or "hawaii"
     */
    private final String nwmLocationLabel;

    /**
     * <p>Whether the URLs for the forecast uses ensemble-like names.
     *
     * <p>This is needed to extract member 1 of the medium range ensemble as if
     * it were a single-valued forecast due to the API for medium range data.
     */
    private final boolean isEnsembleLike;

    /**
     * The duration between each NWM timeseries dataset reference datetime.
     * Assumes a fixed interval between executions of the NWM model software or
     * at least between each executions configured reference datetimes.
     */
    private final Duration durationBetweenReferenceDatetimes;

    /**
     * <p>The duration past midnight Zulu when the first data appears.
     *
     * <p>For example, Puerto Rico short-range forecasts are issued every 12 hours
     * but the first forecast after midnight Zulu is at T06Z.
     */
    private final Duration durationPastMidnight;

    NwmProfile( int blobCount,
                int memberCount,
                Duration durationBetweenValidDatetimes,
                boolean isVector,
                String nwmConfiguration,
                String nwmOutputType,
                TimeLabel timeLabel,
                String nwmSubdirectoryPrefix,
                String nwmLocationLabel,
                Duration durationBetweenReferenceDateTimes,
                boolean isEnsembleLike,
                Duration durationPastMidnight )
    {
        Objects.requireNonNull( durationBetweenValidDatetimes );
        Objects.requireNonNull( nwmConfiguration );
        Objects.requireNonNull( nwmOutputType );
        Objects.requireNonNull( timeLabel );
        Objects.requireNonNull( nwmSubdirectoryPrefix );
        Objects.requireNonNull( nwmLocationLabel );
        Objects.requireNonNull( durationBetweenReferenceDateTimes );
        Objects.requireNonNull( durationPastMidnight );

        if ( blobCount < 1 )
        {
            throw new IllegalArgumentException( "Must have one or more blobs, not "
                                                + blobCount );
        }

        if ( memberCount < 1 )
        {
            throw new IllegalArgumentException( "Must have one or more members, not "
                                                + memberCount );
        }

        if ( memberCount > 1 && !isEnsembleLike )
        {
            throw new IllegalArgumentException( "When there is more than one member, ("
                                                + memberCount
                                                + "), then the data must be "
                                                + "ensemble-like." );
        }

        if ( durationBetweenValidDatetimes.isNegative() )
        {
            throw new IllegalArgumentException( MUST_HAVE_POSITIVE_TIMESTEP_DURATION_NOT
                                                + durationBetweenValidDatetimes );
        }

        if ( durationBetweenValidDatetimes.isZero() )
        {
            throw new IllegalArgumentException( MUST_HAVE_POSITIVE_TIMESTEP_DURATION_NOT
                                                + durationBetweenValidDatetimes );
        }

        if ( durationBetweenReferenceDateTimes.isNegative() )
        {
            throw new IllegalArgumentException( MUST_HAVE_POSITIVE_TIMESTEP_DURATION_NOT
                                                + durationBetweenReferenceDateTimes );
        }

        if ( durationBetweenReferenceDateTimes.isZero() )
        {
            throw new IllegalArgumentException( MUST_HAVE_POSITIVE_TIMESTEP_DURATION_NOT
                                                + durationBetweenReferenceDateTimes );
        }

        if ( durationPastMidnight.isNegative()
             || durationPastMidnight.compareTo( Duration.ofHours( 24 ) ) >= 0 )
        {
            throw new IllegalArgumentException( "Must have duration past midnight of zero through 24 hours, not "
                                                + durationPastMidnight );
        }

        this.blobCount = blobCount;
        this.memberCount = memberCount;
        this.durationBetweenValidDatetimes = durationBetweenValidDatetimes;
        this.isVector = isVector;
        this.nwmConfiguration = nwmConfiguration;
        this.nwmOutputType = nwmOutputType;
        this.timeLabel = timeLabel;
        this.nwmSubdirectoryPrefix = nwmSubdirectoryPrefix;
        this.nwmLocationLabel = nwmLocationLabel;
        this.durationBetweenReferenceDatetimes = durationBetweenReferenceDateTimes;
        this.isEnsembleLike = isEnsembleLike;
        this.durationPastMidnight = durationPastMidnight;
    }

    int getBlobCount()
    {
        return this.blobCount;
    }

    int getMemberCount()
    {
        return this.memberCount;
    }

    Duration getDurationBetweenValidDatetimes()
    {
        return this.durationBetweenValidDatetimes;
    }

    String getNwmConfiguration()
    {
        return this.nwmConfiguration;
    }

    String getNwmOutputType()
    {
        return this.nwmOutputType;
    }

    TimeLabel getTimeLabel()
    {
        return this.timeLabel;
    }

    String getReferenceDatetimeVariable()
    {
        return NwmProfile.REFERENCE_DATETIME_VARIABLE;
    }

    String getFeatureVariable()
    {
        return NwmProfile.FEATURE_VARIABLE;
    }

    String getValidDatetimeVariable()
    {
        return NwmProfile.VALID_DATETIME_VARIABLE;
    }

    String getMemberAttribute()
    {
        return NwmProfile.MEMBER_ATTRIBUTE;
    }

    String getNwmSubdirectoryPrefix()
    {
        return this.nwmSubdirectoryPrefix;
    }

    String getNwmLocationLabel()
    {
        return this.nwmLocationLabel;
    }

    Duration getDurationBetweenReferenceDatetimes()
    {
        return this.durationBetweenReferenceDatetimes;
    }

    boolean isEnsembleLike()
    {
        return this.isEnsembleLike;
    }

    Duration getDurationPastMidnight()
    {
        return this.durationPastMidnight;
    }
}
