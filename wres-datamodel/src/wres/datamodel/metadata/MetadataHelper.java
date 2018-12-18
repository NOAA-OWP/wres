package wres.datamodel.metadata;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
     * Finds the union of the input, based on the {@link TimeWindow}. All components of the input must be equal, 
     * except the {@link SampleMetadata#getTimeWindow()} and {@link SampleMetadata#getThresholds()}, otherwise an exception is 
     * thrown. See also {@link TimeWindow#unionOf(List)}. No threshold information is represented in the union.
     * 
     * @param input the input metadata
     * @return the union of the input
     * @throws MetadataException if the input is invalid
     */

    public static SampleMetadata unionOf( List<SampleMetadata> input )
    {
        String nullString = "Cannot find the union of null metadata.";
        if ( Objects.isNull( input ) )
        {
            throw new MetadataException( nullString );
        }
        if ( input.isEmpty() )
        {
            throw new MetadataException( "Cannot find the union of empty input." );
        }
        List<TimeWindow> unionWindow = new ArrayList<>();

        // Test entry
        SampleMetadata test = input.get( 0 );

        // Validate for equivalence with the first entry and add window to list
        for ( SampleMetadata next : input )
        {
            if ( Objects.isNull( next ) )
            {
                throw new MetadataException( nullString );
            }
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
     * @throws RescalingException when the desiredTimeScale cannot be obtained from the 
     *            existingTimeScale and the timeStep, either in principle or in practice
     * @throws NullPointerException if any input is null
     */

    public static void throwExceptionIfChangeOfScaleIsInvalid( TimeScale existingTimeScale,
                                                               TimeScale desiredTimeScale,
                                                               Duration timeStep )
    {
        Objects.requireNonNull( existingTimeScale, "The existing time scale cannot be null." );

        Objects.requireNonNull( desiredTimeScale, "The desired time scale cannot be null." );

        Objects.requireNonNull( timeStep, "The time-step duration cannot be null." );

        // Timestep cannot be zero
        if ( timeStep.isZero() )
        {
            throw new RescalingException( "The time-step duration cannot be zero for rescaling purposes." );
        }

        // Timestep cannot be negative
        if ( timeStep.isNegative() )
        {
            throw new RescalingException( "The time-step duration cannot be negative for rescaling purposes." );
        }

        // Downscaling not currently allowed
        MetadataHelper.throwExceptionIfDownscalingRequested( existingTimeScale.getPeriod(),
                                                             desiredTimeScale.getPeriod() );

        // The desired time scale period must be an integer multiple of the existing time scale period
        MetadataHelper.throwExceptionIfDesiredPeriodDoesNotCommute( existingTimeScale.getPeriod(),
                                                                    desiredTimeScale.getPeriod(),
                                                                    "existing period" );

        // If the existing and desired periods are the same, the function cannot differ
        MetadataHelper.throwExceptionIfPeriodsMatchAndFunctionsDiffer( existingTimeScale, desiredTimeScale );

        // If the desired function is a total, then the existing function must also be a total
        MetadataHelper.throwExceptionIfAccumulatingNonAccumulations( existingTimeScale.getFunction(),
                                                                     desiredTimeScale.getFunction() );

        // The time-step of the data must be less than or equal to the period associated with the desired time scale
        MetadataHelper.throwExceptionIfDataTimeStepExceedsDesiredPeriod( timeStep, desiredTimeScale.getPeriod() );


        // If time-step of the data is equal to the period associated with the desired time scale, then 
        // rescaling is not allowed
        MetadataHelper.throwExceptionIfDataTimeStepMatchesDesiredPeriodAndRescalingIsRequired( existingTimeScale,
                                                                                               desiredTimeScale,
                                                                                               timeStep );

        // The desired time scale period must be an integer multiple of the data time-step
        MetadataHelper.throwExceptionIfDesiredPeriodDoesNotCommute( timeStep,
                                                                    desiredTimeScale.getPeriod(),
                                                                    "data time-step" );

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
            throw new RescalingException( "The desired period must be an integer multiple of the " + periodType + "." );
        }
    }

    /**
     * Throws an exception if the {@link TimeScale#getPeriod()} of the two periods match, and the 
     * {@link TimeScale#getFunction()} do not match. Cannot change the function without changing the period.
     * 
     * @param existingTimeScale the existing time scale
     * @param desiredTimeScale the desired time scale 
     * @throws RescalingException if the periods match and the functions differ
     */

    private static void throwExceptionIfPeriodsMatchAndFunctionsDiffer( TimeScale existingTimeScale,
                                                                        TimeScale desiredTimeScale )
    {
        if ( existingTimeScale.getPeriod().equals( desiredTimeScale.getPeriod() )
             && existingTimeScale.getFunction() != desiredTimeScale.getFunction() )
        {
            throw new RescalingException( "The periods associated with the existing and desired time scales are the "
                                          + "same, but the time scale functions are different. The function cannot be "
                                          + "changed without changing the period." );
        }
    }

    /**
     * Throws an exception when attempting to accumulate something that is not already an accumulation.
     * 
     * @param existingFunction the existing function
     * @param desiredFunction the desired function
     * @throws RescalingException if the desiredFnction is a {@link TimeScaleFunction#TOTAL} and the existingFunction
     *            is not a {@link TimeScaleFunction#TOTAL}
     */

    private static void throwExceptionIfAccumulatingNonAccumulations( TimeScaleFunction existingFunction,
                                                                      TimeScaleFunction desiredFunction )
    {
        if ( desiredFunction == TimeScaleFunction.TOTAL && existingFunction != TimeScaleFunction.TOTAL )
        {
            throw new RescalingException( "Cannot accumulate values that are not already accumulations. The "
                                          + "function associated with the existing time scale must be a '"
                                          + TimeScaleFunction.TOTAL
                                          + "' or the function associated with the desired time scale must "
                                          + "be changed." );
        }
    }

    /**
     * Throws an exception if the time-step of the data exceeds the desired period.
     * 
     * @param timeStep the data time-step
     * @param desiredPeriod the desired period
     * @throws RescalingException if the timeStep exceeds the desiredPeriod
     */

    private static void throwExceptionIfDataTimeStepExceedsDesiredPeriod( Duration timeStep, Duration desiredPeriod )
    {
        if ( timeStep.compareTo( desiredPeriod ) > 0 )
        {
            throw new RescalingException( "Insufficient data for resclaing: the time-step of the data cannot be "
                                          + "greater than the desired time scale." );
        }
    }

    /**
     * Throws an exception if the time-step of the data matches the period associated with the desiredTimeScale, and 
     * rescaling is required. This is not allowed, because there is insufficient data for rescaling.
     * 
     * @param existingTimeScale the existing time scale
     * @param desiredTimeScale the desired time scale
     * @param timeStep the data time-step
     * @throws RescalingException if the timeStep matches the desired period and rescaling is required
     */

    private static void
            throwExceptionIfDataTimeStepMatchesDesiredPeriodAndRescalingIsRequired( TimeScale existingTimeScale,
                                                                                    TimeScale desiredTimeScale,
                                                                                    Duration timeStep )
    {
        if ( timeStep.equals( desiredTimeScale.getPeriod() ) && !existingTimeScale.equals( desiredTimeScale ) )
        {
            throw new RescalingException( "Insufficient data for rescaling: the period associated with the desired "
                                          + "time scale matches the time-step of the data." );
        }
    }

    /**
     * No argument constructor.
     */

    private MetadataHelper()
    {
    }

}
