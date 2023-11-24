package wres.statistics;

import java.time.Instant;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Consumer.Format;
import wres.statistics.generated.SummaryStatistic;
import wres.statistics.generated.TimeWindow;

/**
 * Factory class for creating statistics messages from POJOs and vice versa.
 *
 * @author James Brown
 */

public class MessageFactory
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MessageFactory.class );

    /**
     * <p>Minimum {@link java.time.Duration}.
     */

    public static final java.time.Duration DURATION_MIN = java.time.Duration.ofSeconds( Long.MIN_VALUE );

    /**
     * <p>Maximum {@link java.time.Duration}.
     */

    public static final java.time.Duration DURATION_MAX = java.time.Duration.ofSeconds( Long.MAX_VALUE, 999_999_999 );

    /**
     * Uncovers a set of declared formats from a description of the outputs.
     *
     * @param outputs the outputs that declare the formats to write
     * @return the declared formats to write
     */

    public static Set<Format> getDeclaredFormats( Outputs outputs )
    {
        Objects.requireNonNull( outputs );

        Set<Format> formats = new HashSet<>();

        if ( outputs.hasPng() )
        {
            formats.add( Format.PNG );
        }

        if ( outputs.hasSvg() )
        {
            formats.add( Format.SVG );
        }

        if ( outputs.hasCsv() )
        {
            formats.add( Format.CSV );
        }

        if ( outputs.hasCsv2() )
        {
            formats.add( Format.CSV2 );
        }

        if ( outputs.hasNetcdf() )
        {
            formats.add( Format.NETCDF );
        }

        if ( outputs.hasNetcdf2() )
        {
            // Only one style of NetCDF in the format options because a single writer writes both
            formats.add( Format.NETCDF );
        }

        if ( outputs.hasProtobuf() )
        {
            formats.add( Format.PROTOBUF );
        }

        return Collections.unmodifiableSet( formats );
    }

    /**
     * Creates a geometry tuple from the input.
     *
     * @param left the left geometry, required
     * @param right the right geometry, required
     * @param baseline the baseline geometry, optional
     * @return the geometry tuple
     * @throws NullPointerException if either the left or right input is null
     */

    public static GeometryTuple getGeometryTuple( Geometry left, Geometry right, Geometry baseline )
    {
        Objects.requireNonNull( left );
        Objects.requireNonNull( right );

        GeometryTuple.Builder builder = GeometryTuple.newBuilder()
                                                     .setLeft( left )
                                                     .setRight( right );

        if ( Objects.nonNull( baseline ) )
        {
            builder.setBaseline( baseline );
        }

        return builder.build();
    }

    /**
     * Creates a geometry group from the input.
     *
     * @param groupName the group name
     * @param singleton the single geometry tuple
     * @return the geometry group
     * @throws NullPointerException if the singleton is null
     */

    public static GeometryGroup getGeometryGroup( String groupName, GeometryTuple singleton )
    {
        Objects.requireNonNull( singleton );

        GeometryGroup.Builder builder = GeometryGroup.newBuilder()
                                                     .addGeometryTuples( singleton );

        if ( Objects.nonNull( groupName ) )
        {
            builder.setRegionName( groupName );
        }

        return builder.build();
    }

    /**
     * Creates a geometry from the input.
     * @param name the name, optional
     * @param description the description, optional
     * @param srid the spatial reference id, optional
     * @param wkt the well-known text string, optional
     * @return the geometry
     */

    public static Geometry getGeometry( String name,
                                        String description,
                                        Integer srid,
                                        String wkt )
    {
        Geometry.Builder builder = Geometry.newBuilder();

        if ( Objects.nonNull( name ) )
        {
            builder.setName( name );
        }

        if ( Objects.nonNull( description ) )
        {
            builder.setDescription( description );
        }

        if ( Objects.nonNull( srid ) )
        {
            builder.setSrid( srid );
        }

        if ( Objects.nonNull( wkt ) )
        {
            builder.setWkt( wkt );
        }

        if ( LOGGER.isTraceEnabled() )
        {
            LOGGER.trace( "Created a new geometry with name '{}', description '{}', srid '{}', and wkt '{}'.",
                          name,
                          description,
                          srid,
                          wkt );
        }

        return builder.build();
    }

    /**
     * Creates a geometry from the input.
     * @param name the name
     * @return geometry
     */

    public static Geometry getGeometry( String name )
    {
        return MessageFactory.getGeometry( name, null, null, null );
    }

    /**
     * Creates a {@link wres.statistics.generated.TimeWindow} from the input. Times on the lower and upper bounds
     * default to {@link Instant#MIN} and {@link Instant#MAX}, respectively. Durations on the lower and upper bounds
     * default to {@link MessageFactory#DURATION_MIN} and {@link MessageFactory#DURATION_MAX}, respectively.
     *
     * @param earliestReferenceTime the earliest reference time, optional
     * @param latestReferenceTime the latest reference time, optional
     * @param earliestValidTime the earliest valid time, optional
     * @param latestValidTime the latest valid time, optional
     * @param earliestLead the earliest lead time, optional
     * @param latestLead the latest lead time, optional
     * @return the time window
     */

    public static TimeWindow getTimeWindow( Instant earliestReferenceTime,
                                            Instant latestReferenceTime,
                                            Instant earliestValidTime,
                                            Instant latestValidTime,
                                            java.time.Duration earliestLead,
                                            java.time.Duration latestLead )
    {
        Instant earliestR = Instant.MIN;
        Instant latestR = Instant.MAX;
        Instant earliestV = Instant.MIN;
        Instant latestV = Instant.MAX;
        java.time.Duration earliestL = MessageFactory.DURATION_MIN;
        java.time.Duration latestL = MessageFactory.DURATION_MAX;

        if ( Objects.nonNull( earliestReferenceTime ) )
        {
            earliestR = earliestReferenceTime;
        }

        if ( Objects.nonNull( latestReferenceTime ) )
        {
            latestR = latestReferenceTime;
        }

        if ( Objects.nonNull( earliestValidTime ) )
        {
            earliestV = earliestValidTime;
        }

        if ( Objects.nonNull( latestValidTime ) )
        {
            latestV = latestValidTime;
        }

        if ( Objects.nonNull( earliestLead ) )
        {
            earliestL = earliestLead;
        }

        if ( Objects.nonNull( latestLead ) )
        {
            latestL = latestLead;
        }

        if ( MessageFactory.LOGGER.isTraceEnabled() )
        {
            MessageFactory.LOGGER.trace(
                    "Created a new time window with an earliest reference time of {}, a latest reference time "
                    + "of {}, an earliest valid time of {}, a latest valid time of {}, an earliest lead duration "
                    + "of {} and a latest lead duration of {}.",
                    earliestR,
                    latestR,
                    earliestV,
                    latestV,
                    earliestL,
                    latestL );
        }

        return TimeWindow.newBuilder()
                         .setEarliestReferenceTime( MessageFactory.parse( earliestR ) )
                         .setLatestReferenceTime( MessageFactory.parse( latestR ) )
                         .setEarliestValidTime( MessageFactory.parse( earliestV ) )
                         .setLatestValidTime( MessageFactory.parse( latestV ) )
                         .setEarliestLeadDuration( MessageFactory.parse( earliestL ) )
                         .setLatestLeadDuration( MessageFactory.parse( latestL ) )
                         .build();
    }

    /**
     * Creates a {@link TimeWindow} from the input. Times on the lower and upper bounds
     * default to {@link Instant#MIN} and {@link Instant#MAX}, respectively.
     *
     * @param earliestValidTime the earliest valid time, optional
     * @param latestValidTime the latest valid time, optional
     * @return the time window
     */

    public static TimeWindow getTimeWindow( Instant earliestValidTime,
                                            Instant latestValidTime )
    {
        return MessageFactory.getTimeWindow( null, null, earliestValidTime, latestValidTime, null, null );
    }

    /**
     * Creates a {@link TimeWindow} from the input. Durations on the lower and upper bounds
     * default to {@link MessageFactory#DURATION_MIN} and {@link MessageFactory#DURATION_MAX}, respectively.
     *
     * @param earliestLead the earliest lead time, optional
     * @param latestLead the latest lead time, optional
     * @return the time window
     */

    public static TimeWindow getTimeWindow( java.time.Duration earliestLead,
                                            java.time.Duration latestLead )
    {
        return MessageFactory.getTimeWindow( null, null, null, null, earliestLead, latestLead );
    }

    /**
     * Creates a {@link TimeWindow} from the input. Times on the lower and upper bounds
     * default to {@link Instant#MIN} and {@link Instant#MAX}, respectively.
     *
     * @param earliestReferenceTime the earliest reference time, optional
     * @param latestReferenceTime the latest reference time, optional
     * @param earliestValidTime the earliest valid time, optional
     * @param latestValidTime the latest valid time, optional
     * @return the time window
     */

    public static TimeWindow getTimeWindow( Instant earliestReferenceTime,
                                            Instant latestReferenceTime,
                                            Instant earliestValidTime,
                                            Instant latestValidTime )
    {
        return MessageFactory.getTimeWindow( earliestReferenceTime,
                                             latestReferenceTime,
                                             earliestValidTime,
                                             latestValidTime,
                                             null,
                                             null );
    }

    /**
     * Creates a {@link TimeWindow} from the input. Times on the lower and upper bounds
     * default to {@link Instant#MIN} and {@link Instant#MAX}, respectively. Durations on the lower and upper bounds
     * default to {@link MessageFactory#DURATION_MIN} and {@link MessageFactory#DURATION_MAX}, respectively.
     *
     * @param earliestReferenceTime the earliest reference time, optional
     * @param latestReferenceTime the latest reference time, optional
     * @param lead the earliest and latest lead time, optional
     * @return the time window
     */

    public static TimeWindow getTimeWindow( Instant earliestReferenceTime,
                                            Instant latestReferenceTime,
                                            java.time.Duration lead )
    {
        return MessageFactory.getTimeWindow( earliestReferenceTime, latestReferenceTime, null, null, lead, lead );
    }

    /**
     * Creates a {@link TimeWindow} from the input. Times on the lower and upper bounds
     * default to {@link Instant#MIN} and {@link Instant#MAX}, respectively. Durations on the lower and upper bounds
     * default to {@link MessageFactory#DURATION_MIN} and {@link MessageFactory#DURATION_MAX}, respectively.
     *
     * @param earliestReferenceTime the earliest reference time, optional
     * @param latestReferenceTime the latest reference time, optional
     * @param earliestLead the earliest lead time, optional
     * @param latestLead the latest lead time, optional
     * @return the time window
     */

    public static TimeWindow getTimeWindow( Instant earliestReferenceTime,
                                            Instant latestReferenceTime,
                                            java.time.Duration earliestLead,
                                            java.time.Duration latestLead )
    {
        return MessageFactory.getTimeWindow( earliestReferenceTime,
                                             latestReferenceTime,
                                             null,
                                             null,
                                             earliestLead,
                                             latestLead );
    }

    /**
     * Creates an empty {@link TimeWindow} in which the times on the lower and upper bounds
     * default to {@link Instant#MIN} and {@link Instant#MAX}, respectively, and the durations on the lower and upper
     * bounds default to {@link MessageFactory#DURATION_MIN} and {@link MessageFactory#DURATION_MAX}, respectively.
     *
     * @return the empty time window
     */

    public static TimeWindow getTimeWindow()
    {
        return MessageFactory.getTimeWindow( null,
                                             null,
                                             null,
                                             null,
                                             null,
                                             null );
    }

    /**
     * Creates a {@link SummaryStatistic} from the inputs.
     * @param name the statistic name, required
     * @param dimension the statistic dimension, required
     * @param probability the optional probability associated with a quantile statistic
     * @return a summary statistic
     * @throws NullPointerException if any required input is null
     * @throws IllegalArgumentException if the probability is outside of the unit interval
     */
    public static SummaryStatistic getSummaryStatistic( SummaryStatistic.StatisticName name,
                                                        SummaryStatistic.StatisticDimension dimension,
                                                        Double probability )
    {
        SummaryStatistic.Builder builder = SummaryStatistic.newBuilder()
                                                           .setStatistic( name )
                                                           .setDimension( dimension );

        if ( Objects.nonNull( probability ) )
        {
            if ( probability < 0.0 || probability > 1.0 )
            {
                throw new IllegalArgumentException( "Cannot create a summary statistic for a quantile with a "
                                                    + "probability of "
                                                    + probability
                                                    + " because the probability is invalid. Please supply a valid "
                                                    + "probability that falls within the unit interval, [0,1]." );
            }

            builder.setProbability( probability );
        }

        return builder.build();
    }

    /**
     * Creates a {@link java.time.Duration} from a {@link Duration}.
     *
     * @param duration the duration to parse
     * @return the duration
     */

    public static java.time.Duration parse( Duration duration )
    {
        Objects.requireNonNull( duration );

        return java.time.Duration.ofSeconds( duration.getSeconds(), duration.getNanos() );
    }

    /**
     * Creates a {@link java.time.Duration} from a {@link Duration}.
     *
     * @param duration the duration to parse
     * @return the duration
     */

    public static Duration parse( java.time.Duration duration )
    {
        Objects.requireNonNull( duration );

        return Duration.newBuilder()
                       .setSeconds( duration.getSeconds() )
                       .setNanos( duration.getNano() )
                       .build();
    }

    /**
     * Creates a {@link Instant} from a {@link Timestamp}.
     *
     * @param timeStamp the time stamp to parse
     * @return the instant
     */

    public static Instant parse( Timestamp timeStamp )
    {
        Objects.requireNonNull( timeStamp );

        return Instant.ofEpochSecond( timeStamp.getSeconds(), timeStamp.getNanos() );
    }

    /**
     * Creates a {@link Timestamp} from a {@link Instant}.
     *
     * @param instant the instant to parse
     * @return the time stamp
     */

    public static Timestamp parse( Instant instant )
    {
        Objects.requireNonNull( instant );

        return Timestamp.newBuilder()
                        .setSeconds( instant.getEpochSecond() )
                        .setNanos( instant.getNano() )
                        .build();
    }

    /**
     * Do not construct.
     */

    private MessageFactory()
    {
    }
}
