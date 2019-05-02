package wres.datamodel.metadata;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.metadata.TimeScale.TimeScaleFunction;
import wres.datamodel.thresholds.OneOrTwoThresholds;

/**
 * A helper class for manipulating metadata.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class MetadataHelper
{

    /**
     * Logger.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger( MetadataHelper.class );

    /**
     * Finds the union of the input, based on the {@link TimeWindow}. All components of the input must be equal, 
     * except the {@link SampleMetadata#getTimeWindow()} and {@link SampleMetadata#getThresholds()}, otherwise an exception is 
     * thrown. See also {@link TimeWindow#unionOf(List)}. No threshold information is represented in the union.
     * 
     * @param input the input metadata
     * @return the union of the input
     * @throws IllegalArgumentException if the input is empty
     * @throws NullPointerException if the input is null
     */

    public static SampleMetadata unionOf( List<SampleMetadata> input )
    {
        String nullString = "Cannot find the union of null metadata.";

        Objects.requireNonNull( input, nullString );

        if ( input.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot find the union of empty input." );
        }
        List<TimeWindow> unionWindow = new ArrayList<>();

        // Test entry
        SampleMetadata test = input.get( 0 );

        // Validate for equivalence with the first entry and add window to list
        for ( SampleMetadata next : input )
        {
            Objects.requireNonNull( next, nullString );

            if ( !next.equalsWithoutTimeWindowOrThresholds( test ) )
            {
                throw new MetadataException( "Only the time window and thresholds can differ when finding the union of "
                                             + "metadata." );
            }
            if ( next.hasTimeWindow() )
            {
                unionWindow.add( next.getTimeWindow() );
            }
        }

        // Remove any threshold information from the result
        test = SampleMetadata.of( test, (OneOrTwoThresholds) null );

        if ( !unionWindow.isEmpty() )
        {
            test = SampleMetadata.of( test, TimeWindow.unionOf( unionWindow ) );
        }
        return test;
    }

    /**
     * <p>Compares two {@link TimeScale} and throws an exception when the desiredTimeScale
     * cannot be derived, either in principle or in practice, from the existingTimeScale.
     * For example, it is not possible, in principle, to obtain the maximum value over 
     * some {@link Duration} from the average value over the same duration. Likewise,
     * it is not currently possible, in practice, to obtain an estimate of a smaller
     * {@link Duration} from a larger {@link Duration}, i.e. to conduct downscaling. 
     * Indeed, downscaling is not currently supported by the application, but is 
     * supportable, in principle.</p> 
     * 
     * <p>In addition to validating the desiredTimeScale
     * against the existingTimeScale, the desiredTimeScale is validated against the
     * time-step of the available data. For example, it is not possible to estimate an
     * average value over some {@link Duration} from data whose time-step is the same as 
     * that {@link Duration}. More generally, the {@link TimeScale#getPeriod()} must be 
     * an integer multiple of the timeStep.</p>
     *
     * @param existingTimeScale the existing time scale
     * @param desiredTimeScale the desired time scale 
     * @param timeStep the time-step of the data
     * @param context optional context information to help clarify warnings
     * @throws RescalingException when the desiredTimeScale cannot be obtained from the 
     *            existingTimeScale and the timeStep, either in principle or in practice
     * @throws NullPointerException if any input is null
     */

    public static void throwExceptionIfChangeOfScaleIsInvalid( TimeScale existingTimeScale,
                                                               TimeScale desiredTimeScale,
                                                               Duration timeStep,
                                                               String... context )
    {
        Objects.requireNonNull( existingTimeScale, "The existing time scale cannot be null." );

        Objects.requireNonNull( desiredTimeScale, "The desired time scale cannot be null." );

        Objects.requireNonNull( timeStep, "The time-step duration cannot be null." );

        // Change of scale required, i.e. not absolutely equal and not instantaneous
        // (which has a more lenient interpretation)
        if ( MetadataHelper.isChangeOfScaleRequired( existingTimeScale, desiredTimeScale, context ) )
        {

            // Timestep cannot be zero
            if ( timeStep.isZero() )
            {
                throw new RescalingException( "The period associated with the time-step cannot be zero." );
            }

            // Timestep cannot be negative
            if ( timeStep.isNegative() )
            {
                throw new RescalingException( "The period associated with the time-step cannot be negative." );
            }
            
            // The desired time scale must be a sensible function in the context of rescaling
            MetadataHelper.throwExceptionIfDesiredFunctionIsUnknown( desiredTimeScale.getFunction() );

            // Downscaling not currently allowed
            MetadataHelper.throwExceptionIfDownscalingRequested( existingTimeScale.getPeriod(),
                                                                 desiredTimeScale.getPeriod() );

            // The desired time scale period must be an integer multiple of the existing time scale period
            MetadataHelper.throwExceptionIfDesiredPeriodDoesNotCommute( existingTimeScale.getPeriod(),
                                                                        desiredTimeScale.getPeriod(),
                                                                        "existing period" );

            // If the existing and desired periods are the same, the function cannot differ
            MetadataHelper.throwExceptionIfPeriodsMatchAndFunctionsDiffer( existingTimeScale, desiredTimeScale, context );

            // If the existing time scale is instantaneous, do not allow accumulations (for now)
            MetadataHelper.throwExceptionIfAccumulatingInstantaneous( existingTimeScale,
                                                                      desiredTimeScale.getFunction() );

            // If the desired function is a total, then the existing function must also be a total
            MetadataHelper.throwExceptionIfAccumulatingNonAccumulations( existingTimeScale.getFunction(),
                                                                         desiredTimeScale.getFunction(),
                                                                         context );

            // The time-step of the data must be less than or equal to the period associated with the desired time scale
            // if rescaling is required
            MetadataHelper.throwExceptionIfDataTimeStepExceedsDesiredPeriod( desiredTimeScale,
                                                                             timeStep );

            // If time-step of the data is equal to the period associated with the desired time scale, then 
            // rescaling is not allowed
            MetadataHelper.throwExceptionIfDataTimeStepMatchesDesiredPeriod( desiredTimeScale,
                                                                             timeStep );

            // The desired time scale period must be an integer multiple of the data time-step
            MetadataHelper.throwExceptionIfDesiredPeriodDoesNotCommute( timeStep,
                                                                        desiredTimeScale.getPeriod(),
                                                                        "data time-step" );

        }

    }

    /**
     * Returns <code>true</code> if a change of time scale is required, otherwise false. A change of scale is required
     * if the two inputs are different, except in one of the following two cases:
     *
     * <ol>
     * <li>The two inputs are both instantaneous according to {@link TimeScale#isInstantaneous()}</li>
     * <li>The only difference is the {@link TimeScale#getFunction()} and the existingTimeScale is
     * {@link TimeScaleFunction#UNKNOWN}</li>
     * </ol>
     *
     * @param existingTimeScale the existing time scale
     * @param desiredTimeScale the desired time scale
     * @param context optional context to clarify any warnings
     * @return true if a change of time scale is required, otherwise false
     * @throws NullPointerException if either input is null
     */

    public static boolean isChangeOfScaleRequired( TimeScale existingTimeScale, TimeScale desiredTimeScale, String...context )
    {
        Objects.requireNonNull( existingTimeScale, "Specify a non-null existing time scale." );

        Objects.requireNonNull( desiredTimeScale, "Specify a non-null desired time scale." );

        boolean different = !existingTimeScale.equals( desiredTimeScale );
        boolean exceptionOne = existingTimeScale.isInstantaneous() && desiredTimeScale.isInstantaneous();
        boolean exceptionTwo = existingTimeScale.getPeriod().equals( desiredTimeScale.getPeriod() )
                               && existingTimeScale.getFunction() == TimeScaleFunction.UNKNOWN;
        
        // Log the second case if the desired time scale has a different function
        if ( exceptionTwo && desiredTimeScale.getFunction() != TimeScaleFunction.UNKNOWN )
        {
            String clarify = MetadataHelper.clarifyWarning( context );

            LOGGER.warn( "The function associated with the desired time scale is a {}, but "
                         + "the function associated with the existing time scale{}is {}. Assuming "
                         + "that the latter is also a {}.",
                         desiredTimeScale.getFunction(),
                         clarify,
                         TimeScaleFunction.UNKNOWN,
                         desiredTimeScale.getFunction() );
        }

        return different && !exceptionOne && !exceptionTwo;
    }

    /**
     * Throws an exception if the desiredFunction is {@link TimeScaleFunction#UNKNOWN}.
     *
     * @param desiredFunction the desired function
     * @throws RescalingException if the desired function is unknown
     */

    private static void throwExceptionIfDesiredFunctionIsUnknown( TimeScaleFunction desiredFunction )
    {
        if ( desiredFunction == TimeScaleFunction.UNKNOWN )
        {
            throw new RescalingException( "The desired time scale function is '" + TimeScaleFunction.UNKNOWN
                                          + "': the function must be known to conduct rescaling." );
        }
    }

    /**
     * Throws an exception if the existingPeriod is larger than the desiredPeriod.
     * 
     * @param existingPeriod the existing period
     * @param desiredPeriod the desired period 
     * @throws RescalingException if the existingPeriod is larger than the desiredPeriod
     */

    private static void throwExceptionIfDownscalingRequested( Duration existingPeriod, Duration desiredPeriod )
    {
        if ( existingPeriod.compareTo( desiredPeriod ) > 0 )
        {
            throw new RescalingException( "Downscaling is not supported: the desired time scale cannot be smaller "
                                          + "than the existing time scale." );
        }
    }

    /**
     * Throws an exception if the desiredPeriod is not an integer multiple of the input period.
     * 
     * @param inputPeriod the input period
     * @param desiredPeriod the desired period 
     * @param periodType a qualifier for the input period type
     * @throws RescalingException if the desiredPeriod is not an integer multiple of the existingPeriod
     */

    private static void throwExceptionIfDesiredPeriodDoesNotCommute( Duration inputPeriod,
                                                                     Duration desiredPeriod,
                                                                     String periodType )
    {
        BigDecimal firstDecimal = BigDecimal.valueOf( inputPeriod.getSeconds() )
                                            .add( BigDecimal.valueOf( inputPeriod.getNano(), 9 ) );
        BigDecimal secondDecimal = BigDecimal.valueOf( desiredPeriod.getSeconds() )
                                             .add( BigDecimal.valueOf( desiredPeriod.getNano(), 9 ) );

        if ( secondDecimal.remainder( firstDecimal ).compareTo( BigDecimal.ZERO ) != 0 )
        {
            throw new RescalingException( "The desired period of " + desiredPeriod
                                          + " is not an integer multiple of the "
                                          + periodType
                                          + " ("
                                          + inputPeriod
                                          + "). If the data has multiple time-steps that "
                                          + "vary by time or feature, it may not be possible to "
                                          + "achieve the desired time scale for all of the data. "
                                          + "In that case, consider removing the desired time "
                                          + "scale and performing an evaluation at the "
                                          + "existing time scale of the data, where possible." );
        }
    }

    /**
     * Throws an exception if the {@link TimeScale#getPeriod()} of the two periods match, and the 
     * {@link TimeScale#getFunction()} do not match, except if the existingTimeScale is
     * {@link TimeScaleFunction#UNKNOWN}, which is allowed (lenient). Cannot change the function without
     * changing the period.
     * 
     * @param existingTimeScale the existing time scale
     * @param desiredTimeScale the desired time scale
     * @param context some optional context information to clarify warnings 
     * @throws RescalingException if the periods match and the functions differ
     */

    private static void throwExceptionIfPeriodsMatchAndFunctionsDiffer( TimeScale existingTimeScale,
                                                                        TimeScale desiredTimeScale,
                                                                        String... context)
    {
        if ( existingTimeScale.getPeriod().equals( desiredTimeScale.getPeriod() )
             && existingTimeScale.getFunction() != desiredTimeScale.getFunction() )
        {
            // If the existing time scale has an unknown function, potentially warn
            if ( existingTimeScale.getFunction() == TimeScaleFunction.UNKNOWN )
            {
                // Warn if the desired time scale has a different function
                if ( desiredTimeScale.getFunction() != TimeScaleFunction.UNKNOWN )
                {
                    String clarify = MetadataHelper.clarifyWarning( context );

                    LOGGER.warn( "The function associated with the desired time scale is "
                                 + "a {}, but the function associated with the existing time "
                                 + "scale{}is {}. Assuming that the latter is also a {}.",
                                 desiredTimeScale.getFunction(),
                                 clarify,
                                 TimeScaleFunction.UNKNOWN,
                                 desiredTimeScale.getFunction() );
                }
            }
            else
            {
                throw new RescalingException( "The periods associated with the existing and desired "
                                              + "time scales are the same, but the time scale functions "
                                              + "are different ["
                                              + existingTimeScale.getFunction()
                                              + ", "
                                              + desiredTimeScale.getFunction()
                                              + "]. The function cannot be "
                                              + "changed without changing the period." );
            }
        }
    }

    /**
     * Throws an exception when attempting to accumulate something an instantaneous value. 
     * TODO: in principle, this might be supported in future, but involves both an integral
     * estimate and a change in units. For example, if the input is precipitation in mm/s
     * then the total might be estimated as the average over the interval, multiplied by 
     * the number of seconds.
     * 
     * @param existingFunction the existing function
     * @param desiredFunction the desired function
     * @throws RescalingException if the desiredFunction is a {@link TimeScaleFunction#TOTAL} and 
     *            the existingFunction {@link TimeScale#isInstantaneous()} returns <code>true</code> 
     */

    private static void throwExceptionIfAccumulatingInstantaneous( TimeScale existingScale,
                                                                   TimeScaleFunction desiredFunction )
    {
        if ( existingScale.isInstantaneous() && desiredFunction == TimeScaleFunction.TOTAL )
        {
            throw new RescalingException( "Cannot accumulate instantaneous values. Change the existing "
                                          + "time scale or change the function associated with the desired "
                                          + "time scale to something other than a '"
                                          + TimeScaleFunction.TOTAL
                                          + "'." );
        }
    }

    /**
     * Throws an exception when attempting to accumulate something that is not already an accumulation.
     * 
     * @param existingFunction the existing function
     * @param desiredFunction the desired function
     * @param context some optional context information to clarify warnings 
     * @throws RescalingException if the desiredFunction is a {@link TimeScaleFunction#TOTAL} and the 
     *            existingFunction is not a {@link TimeScaleFunction#TOTAL} or a {@link TimeScaleFunction#UNKNOWN}
     */

    private static void throwExceptionIfAccumulatingNonAccumulations( TimeScaleFunction existingFunction,
                                                                      TimeScaleFunction desiredFunction,
                                                                      String... context )
    {
        if ( desiredFunction == TimeScaleFunction.TOTAL && existingFunction != TimeScaleFunction.TOTAL )
        {
            if ( existingFunction == TimeScaleFunction.UNKNOWN )
            {
                String clarify = MetadataHelper.clarifyWarning( context );

                LOGGER.warn( "The function associated with the desired time scale is a {}, but "
                             + "the function associated with the existing time scale{}is {}. Assuming "
                             + "that the existing function is a {}.",
                             TimeScaleFunction.TOTAL,
                             clarify,
                             TimeScaleFunction.UNKNOWN,
                             TimeScaleFunction.TOTAL );
            }
            else
            {
                throw new RescalingException( "Cannot accumulate values that are not already accumulations. The "
                                              + "function associated with the existing time scale must be a '"
                                              + TimeScaleFunction.TOTAL
                                              + "', rather than a '"
                                              + existingFunction
                                              + "', or the function associated with the desired time scale must "
                                              + "be changed." );
            }
        }
    }

    /**
     * Throws an exception if the time-step of the data exceeds the desired period and rescaling is required.
     *
     * @param desiredTimeScale the desired time scale
     * @param timeStep the data time-step
     * @throws RescalingException if the timeStep exceeds the desiredPeriod
     */

    private static void
            throwExceptionIfDataTimeStepExceedsDesiredPeriod( TimeScale desiredTimeScale,
                                                              Duration timeStep )
    {
        if ( timeStep.compareTo( desiredTimeScale.getPeriod() ) > 0 )
        {
            throw new RescalingException( "Insufficient data for rescaling: the time-step of the data cannot be "
                                          + "greater than the desired time scale when rescaling is required ["
                                          + timeStep
                                          + ","
                                          + desiredTimeScale.getPeriod()
                                          + "]." );
        }
    }

    /**
     * Throws an exception if the time-step of the data matches the period associated with the desiredTimeScale, and 
     * rescaling is required. This is not allowed, because there is insufficient data for rescaling.
     *
     * @param desiredTimeScale the desired time scale
     * @param timeStep the data time-step
     * @throws RescalingException if the timeStep matches the desired period and rescaling is required
     */

    private static void
            throwExceptionIfDataTimeStepMatchesDesiredPeriod( TimeScale desiredTimeScale,
                                                              Duration timeStep )
    {
        if ( timeStep.equals( desiredTimeScale.getPeriod() ) )
        {
            throw new RescalingException( "Insufficient data for rescaling: the period associated with the desired "
                                          + "time scale matches the time-step of the data ("
                                          + timeStep
                                          + ")." );
        }
    }
    
    /**
     * Clarifies a warning message with some context information, otherwise returns a single-space string.
     * 
     * @param context the optional context
     * @return the clarifying message
     */

    private static String clarifyWarning( String... context )
    {
        String returnMe = " ";
        
        if ( Objects.nonNull( context ) && context.length > 0 )
        {
            StringJoiner joiner = new StringJoiner( " " );
            joiner.add( " of the" );
            Arrays.stream( context ).forEach( joiner::add );
            joiner.add( "data " );
            returnMe = joiner.toString();
        }

        return returnMe;
    }

    /**
     * No argument constructor.
     */

    private MetadataHelper()
    {
    }

}
