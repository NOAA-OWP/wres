package wres.datamodel.scale;

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toList;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.TimeScaleConfig;
import wres.datamodel.scale.ScaleValidationEvent.EventType;
import wres.datamodel.scale.TimeScale.TimeScaleFunction;

/**
 * A helper class for validating scale information.
 * 
 * @author james.brown@hydrosolved.com
 */

public final class ScaleValidationHelper
{

    /**
     * Start of a message
     */

    private static final String MESSAGE_START = "While validating a {0} data source: ";

    /**
     * <p>Checks that the <code>existingTimeScale</code> information is *consistent* with the corresponding declared 
     * information and that the <code>desiredTimeScale</code> is *deliverable* from the information available.
     * 
     * <p>In terms of being deliverable, validates the existing and desired {@link TimeScale} against the time-step of 
     * the data. Whether a <code>desiredTimeScale</code> is deliverable may change as the functionality and 
     * permissiveness of the software changes.
     * 
     * <p>Returns zero or more {@link ScaleValidationEvent} that are {@link EventType#ERROR} or {@link EventType#WARN} 
     * for the caller to handle. A {@link EventType#ERROR} is associated with exceptional behavior, whereas a 
     * {@link EventType#WARN} is information that should be provided to a user, but is not exceptional.
     *
     * @param dataSourceConfig the declared data source
     * @param existingTimeScale the existing time scale, which may originate from the declaration or data or both
     * @param desiredTimeScale the desired time scale, which may originate from the declaration or data/system 
     * @param timeStep the time-step of the data
     * @param dataSource a data source identifier to help clarify the validation message
     * @return a list of validation events
     * @throws NullPointerException if any input is null
     */

    public static List<ScaleValidationEvent> validateScaleInformation( DataSourceConfig dataSourceConfig,
                                                                       TimeScale existingTimeScale,
                                                                       TimeScale desiredTimeScale,
                                                                       Duration timeStep,
                                                                       String dataSource )
    {
        Objects.requireNonNull( dataSourceConfig, "The project declaration cannot be null." );

        Objects.requireNonNull( existingTimeScale, "The existing time scale cannot be null." );

        Objects.requireNonNull( desiredTimeScale, "The desired time scale cannot be null." );

        Objects.requireNonNull( timeStep, "The time-step duration cannot be null." );

        Objects.requireNonNull( dataSource, "The data source identifier cannot be null." );

        List<ScaleValidationEvent> allEvents = new ArrayList<>();

        // Check for consistency with the declaration
        ScaleValidationEvent consistency =
                ScaleValidationHelper.validateConsistency( dataSourceConfig, existingTimeScale, dataSource );

        allEvents.add( consistency );

        // Change of scale required, i.e. not absolutely equal and not instantaneous
        // (which has a more lenient interpretation)
        if ( ScaleValidationHelper.isChangeOfScaleRequired( existingTimeScale, desiredTimeScale ) )
        {

            // Timestep cannot be zero
            if ( timeStep.isZero() )
            {
                String message = MessageFormat.format( MESSAGE_START
                                                       + "The period associated with the time-step cannot be zero.",
                                                       dataSource );

                allEvents.add( ScaleValidationEvent.error( message ) );
            }

            // Timestep cannot be negative
            if ( timeStep.isNegative() )
            {
                String message = MessageFormat.format( MESSAGE_START
                                                       + "The period associated with the time-step cannot be "
                                                       + "negative.",
                                                       dataSource );

                allEvents.add( ScaleValidationEvent.error( message ) );
            }

            // The desired time scale must be a sensible function in the context of rescaling
            allEvents.add( ScaleValidationHelper.checkIfDesiredFunctionIsUnknown( desiredTimeScale.getFunction(),
                                                                                  dataSource ) );

            // Downscaling not currently allowed
            allEvents.add( ScaleValidationHelper.checkIfDownscalingRequested( existingTimeScale.getPeriod(),
                                                                              desiredTimeScale.getPeriod(),
                                                                              dataSource ) );

            // The desired time scale period must be an integer multiple of the existing time scale period
            allEvents.add( ScaleValidationHelper.checkIfDesiredPeriodDoesNotCommute( existingTimeScale.getPeriod(),
                                                                                     desiredTimeScale.getPeriod(),
                                                                                     dataSource,
                                                                                     "existing period" ) );

            // If the existing and desired periods are the same, the function cannot differ
            allEvents.add( ScaleValidationHelper.checkIfPeriodsMatchAndFunctionsDiffer( existingTimeScale,
                                                                                        desiredTimeScale,
                                                                                        dataSource,
                                                                                        dataSource ) );

            // If the existing time scale is instantaneous, do not allow accumulations (for now)
            allEvents.add( ScaleValidationHelper.checkIfAccumulatingInstantaneous( existingTimeScale,
                                                                                   desiredTimeScale.getFunction(),
                                                                                   dataSource ) );

            // If the desired function is a total, then the existing function must also be a total
            allEvents.add( ScaleValidationHelper.checkIfAccumulatingNonAccumulation( existingTimeScale.getFunction(),
                                                                                     desiredTimeScale.getFunction(),
                                                                                     dataSource ) );

            // The time-step of the data must be less than or equal to the period associated with the desired time scale
            // if rescaling is required
            allEvents.add( ScaleValidationHelper.checkIfDataTimeStepExceedsDesiredPeriod( desiredTimeScale,
                                                                                          timeStep,
                                                                                          dataSource ) );

            // If time-step of the data is equal to the period associated with the desired time scale, then 
            // rescaling is not allowed
            allEvents.add( ScaleValidationHelper.checkIfDataTimeStepMatchesDesiredPeriod( desiredTimeScale,
                                                                                          timeStep,
                                                                                          dataSource ) );

            // The desired time scale period must be an integer multiple of the data time-step
            allEvents.add( ScaleValidationHelper.checkIfDesiredPeriodDoesNotCommute( timeStep,
                                                                                     desiredTimeScale.getPeriod(),
                                                                                     dataSource,
                                                                                     "data time-step" ) );

        }

        // Filter all events that are not passes and return an immutable collection of them
        return allEvents.stream()
                        .filter( a -> a.getEventType() != EventType.PASS )
                        .collect( collectingAndThen( toList(), Collections::unmodifiableList ) );
    }

    /**
     * Returns <code>true</code> if the input contains a {@link ScaleValidationEvent} whose 
     * {@link ScaleValidationEvent#getEventType()} is the prescribed type, otherwise <code>false</code>.
     * 
     * @param events the events to check
     * @param eventType the event type to check
     * @return true if the input has one or more validation events with the prescribed eventType, otherwise false
     * @throws NullPointerException if either input is null
     */

    public static boolean hasEvent( Collection<ScaleValidationEvent> events, EventType eventType )
    {
        Objects.requireNonNull( events );

        Objects.requireNonNull( eventType );

        return events.stream().anyMatch( a -> a.getEventType() == eventType );
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
     * @return true if a change of time scale is required, otherwise false
     * @throws NullPointerException if either input is null
     */

    static boolean isChangeOfScaleRequired( TimeScale existingTimeScale, TimeScale desiredTimeScale )
    {
        Objects.requireNonNull( existingTimeScale, "Specify a non-null existing time scale." );

        Objects.requireNonNull( desiredTimeScale, "Specify a non-null desired time scale." );

        boolean different = !existingTimeScale.equals( desiredTimeScale );
        boolean exceptionOne = existingTimeScale.isInstantaneous() && desiredTimeScale.isInstantaneous();
        boolean exceptionTwo = existingTimeScale.getPeriod().equals( desiredTimeScale.getPeriod() )
                               && existingTimeScale.getFunction() == TimeScaleFunction.UNKNOWN;

        return different && !exceptionOne && !exceptionTwo;
    }

    /**
     * Checks for consistency between the data source declaration and the proposed time scale information.
     * 
     * @param dataSourceConfig the data source declaration
     * @param existingTimeScale the existing time scale
     * @param dataSource the data source identifier
     * @return a validation event 
     */

    private static ScaleValidationEvent validateConsistency( DataSourceConfig dataSourceConfig,
                                                             TimeScale existingTimeScale,
                                                             String dataSource )
    {
        TimeScaleConfig existingTimeScaleConfig = dataSourceConfig.getExistingTimeScale();

        if ( Objects.nonNull( existingTimeScaleConfig ) )
        {
            TimeScale declaredExistingTimeScale = TimeScale.of( existingTimeScaleConfig );
            if ( !declaredExistingTimeScale.equals( existingTimeScale ) )
            {
                if ( existingTimeScale.isInstantaneous() && declaredExistingTimeScale.isInstantaneous() )
                {
                    String declaredString = "[" + declaredExistingTimeScale.getPeriod()
                                            + ","
                                            + declaredExistingTimeScale.getFunction()
                                            + "]";
                    String existingString = "[" + existingTimeScale.getPeriod()
                                            + ","
                                            + existingTimeScale.getFunction()
                                            + "]";


                    String message = MessageFormat.format( MESSAGE_START
                                                           + "The existing time scale in the project declaration is "
                                                           + "{1} and the existing time scale associated with the "
                                                           + "data is {2}. This discrepancy is allowed because both "
                                                           + "are recognized by the system as ''INSTANTANEOUS''.",
                                                           dataSource,
                                                           declaredString,
                                                           existingString );

                    return ScaleValidationEvent.warn( message );
                }
                else
                {
                    String message = MessageFormat.format( MESSAGE_START
                                                           + "The existing time scale in the project declaration is "
                                                           + "{1} and the existing time scale associated with the "
                                                           + "data is {2}. This inconsistency is not allowed. Fix "
                                                           + "the declaration of the source.",
                                                           dataSource,
                                                           declaredExistingTimeScale,
                                                           existingTimeScale );

                    return ScaleValidationEvent.error( message );
                }
            }

            String message = MessageFormat.format( "The existing time scale in the project declaration of {1} is "
                                                   + "consistent with the existing time scale associated with the data "
                                                   + "of {2}.",
                                                   dataSource,
                                                   declaredExistingTimeScale,
                                                   existingTimeScale );

            return ScaleValidationEvent.pass( message );
        }

        String message = MessageFormat.format( "The existing time scale in the project declaration is NULL. The "
                                               + "existing time scale associated with the data is {1}.",
                                               dataSource,
                                               existingTimeScale );

        return ScaleValidationEvent.pass( message );
    }

    /**
     * Checks whether the desiredFunction is {@link TimeScaleFunction#UNKNOWN}, which is not allowed. If so,
     * returns a {@link ScaleValidationEvent} that is {@link EventType#ERROR}, otherwise {@link EventType#PASS}.
     *
     * @param desiredFunction the desired function
     * @param dataSource a data source identifier to help clarify the validation mesage
     * @return a validation event
     */

    private static ScaleValidationEvent checkIfDesiredFunctionIsUnknown( TimeScaleFunction desiredFunction,
                                                                         String dataSource )
    {
        if ( desiredFunction == TimeScaleFunction.UNKNOWN )
        {
            String message = MessageFormat.format( MESSAGE_START +
                                                   "The desired time scale function is ''{1}''"
                                                   + ": the function must be known to conduct rescaling.",
                                                   dataSource,
                                                   TimeScaleFunction.UNKNOWN );

            return ScaleValidationEvent.error( message );
        }

        return ScaleValidationEvent.pass( "The desired function is not unknown and is, therefore, acceptable." );
    }

    /**
     * Checks whether the existingPeriod is larger than the desiredPeriod, which is not allowed. If so, returns
     * a {@link ScaleValidationEvent} that is {@link EventType#ERROR}, otherwise {@link EventType#PASS}.
     * 
     * @param existingPeriod the existing period
     * @param desiredPeriod the desired period
     * @param dataSource a data source identifier to help clarify the validation mesage
     * @return a validation event
     */

    private static ScaleValidationEvent
            checkIfDownscalingRequested( Duration existingPeriod, Duration desiredPeriod, String dataSource )
    {
        if ( existingPeriod.compareTo( desiredPeriod ) > 0 )
        {
            String message = MessageFormat.format( MESSAGE_START +
                                                   "Downscaling is not supported: the desired time scale of ''{1}'' "
                                                   + "cannot be smaller than the existing time scale of ''{2}''.",
                                                   dataSource,
                                                   desiredPeriod,
                                                   existingPeriod );

            return ScaleValidationEvent.error( message );
        }

        String message =
                MessageFormat.format( "The existing period of ''{0}'' is not larger than the desired period of "
                                      + "''{1}'' and is, therefore, acceptable.",
                                      existingPeriod,
                                      desiredPeriod );

        return ScaleValidationEvent.pass( message );
    }

    /**
     * Checks whether both periods are positive and whether the desiredPeriod is not an integer multiple of the input 
     * period. If not an integer multiple, returns a {@link ScaleValidationEvent} that is {@link EventType#ERROR}, 
     * otherwise {@link EventType#PASS}.  
     * 
     * @param inputPeriod the input period
     * @param desiredPeriod the desired period 
     * @param dataSource a data source identifier to help clarify the validation mesage
     * @param periodType a qualifier for the input period type
     * @return a validation event
     */

    private static ScaleValidationEvent checkIfDesiredPeriodDoesNotCommute( Duration inputPeriod,
                                                                            Duration desiredPeriod,
                                                                            String dataSource,
                                                                            String periodType )
    {
        BigDecimal firstDecimal = BigDecimal.valueOf( inputPeriod.getSeconds() )
                                            .add( BigDecimal.valueOf( inputPeriod.getNano(), 9 ) );
        BigDecimal secondDecimal = BigDecimal.valueOf( desiredPeriod.getSeconds() )
                                             .add( BigDecimal.valueOf( desiredPeriod.getNano(), 9 ) );

        boolean isOneZero = Duration.ZERO.equals( inputPeriod ) || Duration.ZERO.equals( desiredPeriod );

        if ( isOneZero || secondDecimal.remainder( firstDecimal ).compareTo( BigDecimal.ZERO ) != 0 )
        {
            String message = MessageFormat.format( MESSAGE_START +
                                                   "The desired period of ''{1}''"
                                                   + " is not an integer multiple of the {2}"
                                                   + ", which is ''{3}''. If the data has multiple time-steps that "
                                                   + "vary by time or feature, it may not be possible to "
                                                   + "achieve the desired time scale for all of the data. "
                                                   + "In that case, consider removing the desired time "
                                                   + "scale and performing an evaluation at the "
                                                   + "existing time scale of the data, where possible.",
                                                   dataSource,
                                                   desiredPeriod,
                                                   periodType,
                                                   inputPeriod );

            return ScaleValidationEvent.error( message );
        }

        String message = MessageFormat.format( "The desired period of ''{0}'' is an integer multiple of the {1} of "
                                               + "''{2}'' and is, therefore, acceptable.",
                                               desiredPeriod,
                                               periodType,
                                               inputPeriod );

        return ScaleValidationEvent.pass( message );
    }

    /**
     * <p>Checks whether the {@link TimeScale#getPeriod()} of the two periods match. Returns a validation event as 
     * follows:
     * 
     * <ol>
     * <li>If the {@link TimeScale#getPeriod()} match and the {@link TimeScale#getFunction()} do not match and the 
     * the existingTimeScale is {@link TimeScaleFunction#UNKNOWN}, returns a {@link ScaleValidationEvent} that is 
     * a {@link EventType#WARN}, which assumes, leniently, that the desiredTimeScale can be achieved.</li>
     * <li>If the {@link TimeScale#getPeriod()} match and the {@link TimeScale#getFunction()} do not match and the 
     * the existingTimeScale is not a {@link TimeScaleFunction#UNKNOWN}, returns a {@link ScaleValidationEvent} that is 
     * a {@link EventType#ERROR}.</li>
     * <li>Otherwise, returns a {@link ScaleValidationEvent} that is a {@link EventType#PASS}.</li>
     * </ol>
     * 
     * @param existingTimeScale the existing time scale
     * @param desiredTimeScale the desired time scale
     * @param dataSource a data source identifier to help clarify the validation mesage
     * @param context some optional context information to clarify warnings 
     * @returns a validation event
     */

    private static ScaleValidationEvent checkIfPeriodsMatchAndFunctionsDiffer( TimeScale existingTimeScale,
                                                                               TimeScale desiredTimeScale,
                                                                               String dataSource,
                                                                               String context )
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
                    String clarify = ScaleValidationHelper.clarifyWarning( context );

                    String message = MessageFormat.format( MESSAGE_START +
                                                           "The function associated with the desired time scale is "
                                                           + "a ''{1}'', but the function associated with the existing "
                                                           + "time scale{2}is ''{3}''. Assuming that the latter is also "
                                                           + "a ''{4}''.",
                                                           dataSource,
                                                           desiredTimeScale.getFunction(),
                                                           clarify,
                                                           TimeScaleFunction.UNKNOWN,
                                                           desiredTimeScale.getFunction() );

                    return ScaleValidationEvent.warn( message );
                }
            }
            else
            {
                String message = MessageFormat.format( MESSAGE_START +
                                                       "The period associated with the existing and desired "
                                                       + "time scales is ''{1}'', but the time scale function "
                                                       + "associated with the existing time scale is ''{2}'', which "
                                                       + "differs from the function associated with the desired time "
                                                       + "scale, namely ''{3}''. This is not allowed. The function "
                                                       + "cannot be changed without changing the period.",
                                                       dataSource,
                                                       existingTimeScale.getPeriod(),
                                                       existingTimeScale.getFunction(),
                                                       desiredTimeScale.getFunction() );

                return ScaleValidationEvent.error( message );
            }
        }

        String message =
                MessageFormat.format( "No attempt was made to change the time scale functions without changing "
                                      + "the period.",
                                      existingTimeScale.getPeriod(),
                                      desiredTimeScale.getPeriod(),
                                      existingTimeScale.getFunction(),
                                      desiredTimeScale.getFunction() );

        return ScaleValidationEvent.pass( message );
    }

    /**
     * <p>Checks whether attempting to accumulate a quantity that is instantaneous, which is not allowed. If so, returns
     * a {@link ScaleValidationEvent} that is {@link EventType#ERROR}, otherwise {@link EventType#PASS}. 
     * 
     * <p>TODO: in principle, this might be supported in future, but involves both an integral
     * estimate and a change in units. For example, if the input is precipitation in mm/s
     * then the total might be estimated as the average over the interval, multiplied by 
     * the number of seconds.
     * 
     * @param existingFunction the existing function
     * @param desiredFunction the desired function
     * @param dataSource a data source identifier to help clarify the validation mesage
     * @return a validation event
     */

    private static ScaleValidationEvent checkIfAccumulatingInstantaneous( TimeScale existingScale,
                                                                          TimeScaleFunction desiredFunction,
                                                                          String dataSource )
    {
        if ( existingScale.isInstantaneous() && desiredFunction == TimeScaleFunction.TOTAL )
        {
            String message = MessageFormat.format( MESSAGE_START
                                                   + "Cannot accumulate instantaneous values. Change the existing "
                                                   + "time scale or change the function associated with the desired "
                                                   + "time scale to something other than a ''{1}''.",
                                                   dataSource,
                                                   TimeScaleFunction.TOTAL );

            return ScaleValidationEvent.error( message );
        }

        return ScaleValidationEvent.pass( "Not attempting to accumulate an instantaneous value." );

    }

    /**
     * <p>Checks whether attempting to accumulate something that is not already an accumulation.
     * 
     * <ol>
     * <li>If the desired function is a {@link TimeScaleFunction.TOTAL} and the existing function is a
     * {@link TimeScaleFunction.UNKNOWN}, returns a {@link ScaleValidationEvent} that is 
     * a {@link EventType#WARN}, which assumes, leniently, that the existing function is a 
     * {@link TimeScaleFunction.TOTAL}.</li>
     * <li>If the desired function is a {@link TimeScaleFunction.TOTAL} and the existing function is not
     * {@link TimeScaleFunction.UNKNOWN}, returns a {@link ScaleValidationEvent} that is 
     * a {@link EventType#ERROR}.</li>
     * <li>Otherwise, returns a {@link ScaleValidationEvent} that is a {@link EventType#PASS}.</li>
     * </ol>
     * 
     * @param existingFunction the existing function
     * @param desiredFunction the desired function
     * @param dataSource a data source identifier to help clarify the validation mesage
     * @returns a validation event
     */

    private static ScaleValidationEvent checkIfAccumulatingNonAccumulation( TimeScaleFunction existingFunction,
                                                                            TimeScaleFunction desiredFunction,
                                                                            String dataSource )
    {
        if ( desiredFunction == TimeScaleFunction.TOTAL && existingFunction != TimeScaleFunction.TOTAL )
        {
            if ( existingFunction == TimeScaleFunction.UNKNOWN )
            {
                String clarify = ScaleValidationHelper.clarifyWarning( dataSource );

                String message =
                        MessageFormat.format( MESSAGE_START
                                              + "The function associated with the desired time scale is a ''{1}'', but "
                                              + "the function associated with the existing time scale{2}is ''{3}''. "
                                              + "Assuming that the existing function is a ''{4}''.",
                                              dataSource,
                                              TimeScaleFunction.TOTAL,
                                              clarify,
                                              TimeScaleFunction.UNKNOWN,
                                              TimeScaleFunction.TOTAL );

                return ScaleValidationEvent.warn( message );
            }
            else
            {
                String message =
                        MessageFormat.format( MESSAGE_START
                                              + "Cannot accumulate values that are not already accumulations. The "
                                              + "function associated with the existing time scale must be a ''{1}'', "
                                              + "rather than a ''{2}'', or the function associated with the desired "
                                              + "time scale must be changed.",
                                              dataSource,
                                              TimeScaleFunction.TOTAL,
                                              existingFunction );

                return ScaleValidationEvent.error( message );
            }
        }

        String message = MessageFormat.format( "Did not detect an attempt to accumulate something that is not an "
                                               + "accumulation. Found an existing function of ''{0}'', and a desired "
                                               + "function of ''{1}''",
                                               existingFunction,
                                               desiredFunction );

        return ScaleValidationEvent.pass( message );
    }

    /**
     * Checks whether the time-step of the data exceeds the desired period and rescaling is required. If so, returns a
     * {@link ScaleValidationEvent} that is {@link EventType#ERROR}, otherwise {@link EventType#PASS}. 
     *
     * @param desiredTimeScale the desired time scale
     * @param timeStep the data time-step
     * @param dataSource a data source identifier to help clarify the validation mesage
     * @return a validation event
     */

    private static ScaleValidationEvent checkIfDataTimeStepExceedsDesiredPeriod( TimeScale desiredTimeScale,
                                                                                 Duration timeStep,
                                                                                 String dataSource )
    {
        if ( timeStep.compareTo( desiredTimeScale.getPeriod() ) > 0 )
        {
            String message =
                    MessageFormat.format( MESSAGE_START
                                          + "Insufficient data for rescaling: The time-step of the data is ''{1}'' and "
                                          + "the period associated with the desired time scale is ''{2}''. The "
                                          + "time-step of the data cannot be greater than the desired time scale when "
                                          + "rescaling is required.",
                                          dataSource,
                                          timeStep,
                                          desiredTimeScale.getPeriod() );

            return ScaleValidationEvent.error( message );
        }

        String message = MessageFormat.format( "The desired time scale has a period of ''{0}'', which is greater "
                                               + "than or equal to the data time-step of ''{1}''.",
                                               desiredTimeScale.getPeriod(),
                                               timeStep );

        return ScaleValidationEvent.pass( message );
    }

    /**
     * Checks whether the time-step of the data matches the period associated with the desiredTimeScale and rescaling 
     * is required. If so, returns a {@link ScaleValidationEvent} that is {@link EventType#ERROR}, 
     * otherwise {@link EventType#PASS}. 
     *
     * @param desiredTimeScale the desired time scale
     * @param timeStep the data time-step
     * @param dataSource a data source identifier to help clarify the validation mesage
     * @throws RescalingException if the timeStep matches the desired period and rescaling is required
     */

    private static ScaleValidationEvent checkIfDataTimeStepMatchesDesiredPeriod( TimeScale desiredTimeScale,
                                                                                 Duration timeStep,
                                                                                 String dataSource )
    {
        if ( timeStep.equals( desiredTimeScale.getPeriod() ) )
        {
            String message = MessageFormat.format( MESSAGE_START
                                                   + "Insufficient data for rescaling: the period associated with the "
                                                   + "desired time scale matches the time-step of the data ({1}).",
                                                   dataSource,
                                                   timeStep );

            return ScaleValidationEvent.error( message );
        }

        String message =
                MessageFormat.format( "The desired time scale has a period of ''{0}'', which is not equal to the "
                                      + "data time-step of '{1}'.",
                                      desiredTimeScale.getPeriod(),
                                      timeStep );

        return ScaleValidationEvent.pass( message );
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

    private ScaleValidationHelper()
    {
    }

}
