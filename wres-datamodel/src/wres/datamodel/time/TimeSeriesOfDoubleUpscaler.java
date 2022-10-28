package wres.datamodel.time;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.DoubleUnaryOperator;
import java.util.function.ToDoubleFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.measure.Unit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.datamodel.scale.TimeScaleOuter;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.EvaluationStage;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.datamodel.MissingValues;
import wres.datamodel.Units;
import wres.datamodel.messages.EvaluationStatusMessage;

/**
 * <p>A minimal implementation of a {@link TimeSeriesUpscaler} for a {@link TimeSeries} comprised of {@link Double} 
 * values. An upscaled value is produced from a collection of values that fall within an interval that ends at a 
 * prescribed time. The interval has the same width as the period associated with the desired {@link TimeScaleOuter}. If 
 * the events are not evenly spaced within the interval, that interval is skipped and logged. If any event value is 
 * non-finite, then the upscaled event value is {@link MissingValues#DOUBLE}. The interval is right-closed, 
 * i.e. <code>(end-period,end]</code>. Thus, for example, when upscaling a sequence of instantaneous values 
 * (0Z,6Z,12Z,18Z,0Z] to form an average that ends at 0Z and spans a period of PT24H, the four-point average is taken 
 * for the values at 6Z, 12Z, 18Z and 0Z and not the five-point average. Indeed, if these values represented an average 
 * over PT1H, rather than instantaneous values, then the five-point average would consider a PT25H period.
 * 
 * @author James Brown
 */

public class TimeSeriesOfDoubleUpscaler implements TimeSeriesUpscaler<Double>
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( TimeSeriesOfDoubleUpscaler.class );

    /** Function that returns a double value or {@link MissingValues#DOUBLE} if the
     * input is not finite. */
    private static final DoubleUnaryOperator RETURN_DOUBLE_OR_MISSING =
            a -> MissingValues.isMissingValue( a ) ? MissingValues.DOUBLE: a;

    /** Lenient means that upscaling can proceed when values that match the {@link MissingValues#DOUBLE} are encountered
     * or when the values to upscale are spaced irregularly over the interval (e.g., because data is implicitly 
     * missing). */
    private final boolean isLenient;

    /** A map of declared unit aliases to help with unit conversion when unit conversion is required as part of 
     * upscaling. */
    private final Map<String, String> unitAliases;

    /**
     * Creates an instance that enforces strict upscaling or no leniency.
     * 
     * @see #of(boolean)
     * @return an instance of the upscaler
     */

    public static TimeSeriesOfDoubleUpscaler of()
    {
        return new TimeSeriesOfDoubleUpscaler( false, Collections.emptyMap() );
    }

    /**
     * Creates an instance with a prescribed leniency. Lenient upscaling means that missing data does not prevent 
     * upscaling and that irregularly spaced data (which could indicate implicitly missing values) does not prevent 
     * upscaling. Data is explicitly missing if it matches the {@link MissingValues#DOUBLE}.
     * 
     * @param isLenient is {@code true} to enforce lenient upscaling, {@code false} otherwise
     * @return an instance of the ensemble upscaler
     */

    public static TimeSeriesOfDoubleUpscaler of( boolean isLenient )
    {
        return new TimeSeriesOfDoubleUpscaler( isLenient, Collections.emptyMap() );
    }

    /**
     * Creates an instance with a prescribed leniency and a map of unit aliases to use when upscaling also requires 
     * unit conversion. Lenient upscaling means that missing data does not prevent upscaling and that irregularly 
     * spaced data (which could indicate implicitly missing values) does not prevent upscaling. Data is explicitly 
     * missing if it matches the {@link MissingValues#DOUBLE}.
     * 
     * @param isLenient is {@code true} to enforce lenient upscaling, {@code false} otherwise
     * @param unitAliases a map of declared unit aliases
     * @return an instance of the ensemble upscaler
     * @throws NullPointerException if the unitAliases is null
     */

    public static TimeSeriesOfDoubleUpscaler of( boolean isLenient, Map<String, String> unitAliases )
    {
        return new TimeSeriesOfDoubleUpscaler( isLenient, unitAliases );
    }

    @Override
    public RescaledTimeSeriesPlusValidation<Double> upscale( TimeSeries<Double> timeSeries,
                                                             TimeScaleOuter desiredTimeScale,
                                                             String desiredUnit )
    {
        return this.upscale( timeSeries, desiredTimeScale, Collections.emptySortedSet(), desiredUnit );
    }

    @Override
    public RescaledTimeSeriesPlusValidation<Double> upscale( TimeSeries<Double> timeSeries,
                                                             TimeScaleOuter desiredTimeScale,
                                                             SortedSet<Instant> endsAt,
                                                             String desiredUnitString )
    {
        Objects.requireNonNull( desiredTimeScale );
        Objects.requireNonNull( desiredUnitString );

        String existingUnitString = timeSeries.getMetadata()
                                              .getUnit();

        boolean scaleAndUnitChange = false;
        Unit<?> existingUnit = null;
        Unit<?> desiredUnit = null;

        // Only formalize the units when a unit conversion is needed since the unit may not be recognized
        if ( !existingUnitString.equals( desiredUnitString ) )
        {
            existingUnit = Units.getUnit( existingUnitString, this.getUnitAliases() );
            desiredUnit = Units.getUnit( desiredUnitString, this.getUnitAliases() );

            // When performing an upscaling that involves time integration of the units, there is an intermediate step
            // to form the time average
            scaleAndUnitChange = Units.isSupportedTimeIntegralConversion( existingUnit, desiredUnit );
        }

        TimeScaleOuter timeScaleToUse = desiredTimeScale;
        if ( scaleAndUnitChange )
        {
            timeScaleToUse = TimeScaleOuter.of( desiredTimeScale.getPeriod(), TimeScaleFunction.MEAN );
        }

        // Rescale
        ToDoubleFunction<SortedSet<Event<Double>>> upscaler = this.getDoubleUpscaler( timeScaleToUse.getFunction() );
        RescaledTimeSeriesPlusValidation<Double> rescaled = RescalingHelper.upscale( timeSeries,
                                                                                     upscaler::applyAsDouble,
                                                                                     timeScaleToUse,
                                                                                     existingUnit,
                                                                                     desiredUnit,
                                                                                     endsAt,
                                                                                     this.isLenient() );

        // Special handling when accumulating a volumetric flow because this creates a volume, so finalize that part
        if ( scaleAndUnitChange )
        {
            rescaled = this.doTimeIntegralConversion( rescaled,
                                                      existingUnit,
                                                      desiredUnit,
                                                      desiredUnitString,
                                                      desiredTimeScale );
        }

        return rescaled;
    }

    /**
     * @return {@code true} if lenient upscaling is required, {@code false} otherwise.
     */

    private boolean isLenient()
    {
        return this.isLenient;
    }

    /**
     * @return the declared unit aliases
     */
    private Map<String, String> getUnitAliases()
    {
        return this.unitAliases;
    }

    /**
     * Performs a time-integration step on the rescaled time-series values, which represent mean averages over the scale 
     * period, and then converts the units to time integrated units.
     * 
     * @param toIntegrate the time-series to integrate
     * @param existingUnit the existing measurement unit
     * @param desiredUnit the desired measurement unit
     * @param desiredUnitString the declared desired unit string
     * @param desiredTimeScale the desired time scale
     * @return a time-series in volume units
     */

    private RescaledTimeSeriesPlusValidation<Double>
            doTimeIntegralConversion( RescaledTimeSeriesPlusValidation<Double> toIntegrate,
                                      Unit<?> existingUnit,
                                      Unit<?> desiredUnit,
                                      String desiredUnitString,
                                      TimeScaleOuter desiredTimeScale )
    {
        TimeSeries<Double> seriesToIntegrate = toIntegrate.getTimeSeries();
        List<EvaluationStatusMessage> validationEvents = new ArrayList<>( toIntegrate.getValidationEvents() );

        if ( desiredTimeScale.getFunction() != TimeScaleFunction.TOTAL )
        {
            String message =
                    MessageFormat.format( "Attempted to convert a time-dependent unit of ''{0}'', which "
                                          + "represents a ''{1}'' over the time scale period, to a time integral unit "
                                          + "of ''{2}'', which represents a ''{3}'' over the time scale "
                                          + "period. This is not allowed. The desired time scale function "
                                          + "must be a ''{4}'' to apply this conversion.",
                                          existingUnit,
                                          seriesToIntegrate.getTimeScale()
                                                           .getFunction(),
                                          desiredUnit,
                                          desiredTimeScale.getFunction(),
                                          TimeScaleFunction.TOTAL );

            EvaluationStatusMessage error = EvaluationStatusMessage.error( EvaluationStage.RESCALING, message );
            validationEvents.add( error );

            // This throws the error message wrapped as an exception, along with any other messages that came before it
            RescalingHelper.checkForRescalingErrorsAndThrowExceptionIfRequired( validationEvents,
                                                                                seriesToIntegrate.getMetadata() );
        }

        String message = MessageFormat.format( "Detected a conversion from a time-dependent unit of ''{0}'', which "
                                               + "represents a ''{1}'' over the time scale period, to a time integral "
                                               + "unit of ''{2}'', which represents a ''{3}'' over the time scale "
                                               + "period. This is allowed.",
                                               existingUnit,
                                               seriesToIntegrate.getTimeScale()
                                                                .getFunction(),
                                               desiredUnit,
                                               TimeScaleFunction.TOTAL );

        EvaluationStatusMessage status = EvaluationStatusMessage.debug( EvaluationStage.RESCALING, message );
        validationEvents.add( status );

        // Create a conversion function
        UnaryOperator<Double> converter = Units.getTimeIntegralConverter( seriesToIntegrate.getTimeScale(),
                                                                          existingUnit,
                                                                          desiredUnit );

        // Create a converted time-series
        TimeSeries<Double> volumeSeries = TimeSeriesSlicer.transform( seriesToIntegrate, converter );

        // Update the units and time scale function
        Duration existingScalePeriod = desiredTimeScale.getPeriod();
        TimeScaleOuter newDesiredTimeScale = TimeScaleOuter.of( existingScalePeriod, TimeScaleFunction.TOTAL );
        TimeSeriesMetadata updated = volumeSeries.getMetadata()
                                                 .toBuilder()
                                                 .setUnit( desiredUnitString )
                                                 .setTimeScale( newDesiredTimeScale )
                                                 .build();

        TimeSeries<Double> adjustedVolumeSeries = TimeSeries.of( updated, volumeSeries.getEvents() );

        return RescaledTimeSeriesPlusValidation.of( adjustedVolumeSeries, validationEvents );
    }

    /**
     * Returns a function that corresponds to a {@link TimeScaleFunction}, additionally wrapped by 
     * {@link #RETURN_DOUBLE_OR_MISSING} so that missing input produces missing output.
     * 
     * @param function The nominated function
     * @return a function for upscaling
     * @throws UnsupportedOperationException if the nominated function is not recognized
     */

    private ToDoubleFunction<SortedSet<Event<Double>>> getDoubleUpscaler( TimeScaleFunction function )
    {
        return events -> {

            double upscaled;

            SortedSet<Event<Double>> eventsToUse = events;

            if ( this.isLenient() )
            {
                eventsToUse = eventsToUse.stream()
                                         .filter( next -> Double.isFinite( next.getValue() ) )
                                         .collect( Collectors.toCollection( TreeSet::new ) );
            }

            // No data to upscale
            if ( eventsToUse.isEmpty() )
            {
                LOGGER.debug( "While attempting to upscale a collection of events, discovered no events to upscale." );

                return MissingValues.DOUBLE;
            }

            switch ( function )
            {
                case MAXIMUM:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Event::getValue )
                                          .max()
                                          .getAsDouble();
                    break;
                case MEAN:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Event::getValue )
                                          .average()
                                          .getAsDouble();
                    break;
                case MINIMUM:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Event::getValue )
                                          .min()
                                          .getAsDouble();
                    break;
                case TOTAL:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Event::getValue )
                                          .sum();
                    break;
                default:
                    throw new UnsupportedOperationException( "Could not create an upscaling function for the "
                                                             + "function identifier '"
                                                             + function
                                                             + "'." );

            }

            return RETURN_DOUBLE_OR_MISSING.applyAsDouble( upscaled );
        };
    }

    /**
     * Hidden constructor.
     * 
     * @param isLenient is true if the lenient upscaling is required, false otherwise
     * @param a map of declared unit aliases, possibly empty
     * @throws NullPointerException if the unitAliases is null
     */

    private TimeSeriesOfDoubleUpscaler( boolean isLenient, Map<String, String> unitAliases )
    {
        Objects.requireNonNull( unitAliases );

        this.isLenient = isLenient;
        this.unitAliases = unitAliases;
    }

}
