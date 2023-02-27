package wres.datamodel.time;

import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.DoubleUnaryOperator;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.measure.Unit;
import wres.datamodel.Ensemble;
import wres.datamodel.Ensemble.Labels;
import wres.datamodel.messages.EvaluationStatusMessage;
import wres.datamodel.MissingValues;
import wres.datamodel.units.Units;
import wres.datamodel.scale.TimeScaleOuter;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.EvaluationStage;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * <p>A minimal implementation of a {@link TimeSeriesUpscaler} for a {@link TimeSeries} comprised of {@link Ensemble} 
 * values. Makes the same assumptions as the {@link TimeSeriesOfDoubleUpscaler}, but additionally requires that every
 * {@link Ensemble} contains the same number of ensemble members.
 * 
 * @author James Brown
 */

public class TimeSeriesOfEnsembleUpscaler implements TimeSeriesUpscaler<Ensemble>
{
    /** Function that returns a double value or {@link MissingValues#DOUBLE} if the
     * input is not finite. */
    private static final DoubleUnaryOperator RETURN_DOUBLE_OR_MISSING =
            a -> MissingValues.isMissingValue( a ) ? MissingValues.DOUBLE : a;

    /** A map of declared unit aliases to help with unit conversion when unit conversion is required as part of 
     * upscaling. */
    private final Map<String, String> unitAliases;

    /**
     * Lenient means that upscaling can proceed when values that match the {@link MissingValues#DOUBLE} are encountered
     * or when the values to upscale are spaced irregularly over the interval (e.g., because data is implicitly 
     * missing).
     */

    private final boolean isLenient;

    /**
     * Creates an instance that enforces strict upscaling or no leniency.
     * 
     * @see #of(boolean)
     * @return an instance of the ensemble upscaler
     */

    public static TimeSeriesOfEnsembleUpscaler of()
    {
        return new TimeSeriesOfEnsembleUpscaler( false, Collections.emptyMap() );
    }

    /**
     * Creates an instance with a prescribed leniency. Lenient upscaling means that missing data does not prevent 
     * upscaling and that irregularly spaced data (which could indicate implicitly missing values) does not prevent 
     * upscaling. Data is explicitly missing if it matches the {@link MissingValues#DOUBLE}.
     * 
     * @param isLenient is {@code true} to enforce lenient upscaling, {@code false} otherwise
     * @return an instance of the ensemble upscaler
     */

    public static TimeSeriesOfEnsembleUpscaler of( boolean isLenient )
    {
        return new TimeSeriesOfEnsembleUpscaler( isLenient, Collections.emptyMap() );
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

    public static TimeSeriesOfEnsembleUpscaler of( boolean isLenient, Map<String, String> unitAliases )
    {
        return new TimeSeriesOfEnsembleUpscaler( isLenient, unitAliases );
    }

    @Override
    public RescaledTimeSeriesPlusValidation<Ensemble> upscale( TimeSeries<Ensemble> timeSeries,
                                                               TimeScaleOuter desiredTimeScale,
                                                               String desiredUnit )
    {
        return this.upscale( timeSeries, desiredTimeScale, Collections.emptySortedSet(), desiredUnit );
    }

    @Override
    public RescaledTimeSeriesPlusValidation<Ensemble> upscale( TimeSeries<Ensemble> timeSeries,
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

        // Only formalize the units when a unit conversion is needed
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
        Function<SortedSet<Event<Ensemble>>, Ensemble> upscaler =
                this.getEnsembleUpscaler( timeScaleToUse.getFunction() );
        RescaledTimeSeriesPlusValidation<Ensemble> rescaled = RescalingHelper.upscale( timeSeries,
                                                                                       upscaler,
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
     * Returns the ensemble upscaler used by this instance.
     * 
     * @param function the time-scale function
     * @return the ensemble upscaler
     */

    private Function<SortedSet<Event<Ensemble>>, Ensemble> getEnsembleUpscaler( TimeScaleFunction function )
    {
        ToDoubleFunction<List<Double>> upscaler = this.getTraceUpscaler( function );

        return events -> {

            int memberCount = this.getMemberCountAndValidateConstant( events );

            Labels labels = events.last()
                                  .getValue()
                                  .getLabels();

            double[] upscaled = new double[memberCount];

            for ( int i = 0; i < memberCount; i++ )
            {
                int nextIndex = i;
                List<Double> doubles = events.stream()
                                             .map( next -> next.getValue().getMembers()[nextIndex] )
                                             .collect( Collectors.toList() );

                double nextUpscaled = upscaler.applyAsDouble( doubles );
                upscaled[i] = nextUpscaled;
            }

            return Ensemble.of( upscaled, labels );
        };
    }

    /**
     * Returns the number of ensemble members associated with every ensemble in the set of events.
     * 
     * @param events the events
     * @return the number of ensemble members
     * @throws UnsupportedOperationException if the number of ensemble members is not fixed
     */
    private int getMemberCountAndValidateConstant( SortedSet<Event<Ensemble>> events )
    {
        // A map of ensemble members per valid time organized by label or index
        Set<Integer> counts =
                events.stream()
                      .map( next -> next.getValue()
                                        .size() )
                      .collect( Collectors.toSet() );

        // No labels, so check for a constant number of ensemble members
        if ( counts.size() > 1 )
        {
            throw new UnsupportedOperationException( "Encountered a collection of ensembles to upscale that contained "
                                                     + "a varying number of ensemble members, which is not supported. "
                                                     + "The ensemble member counts were: "
                                                     + counts
                                                     + "." );
        }

        return counts.stream()
                     .findAny()
                     .orElse( 0 );
    }

    /**
     * Returns a function that corresponds to a {@link TimeScaleFunction}, additionally wrapped by 
     * {@link #RETURN_DOUBLE_OR_MISSING} so that missing input produces missing output.
     * 
     * @param function The nominated function
     * @return a function for upscaling
     * @throws UnsupportedOperationException if the nominated function is not recognized
     */

    private ToDoubleFunction<List<Double>> getTraceUpscaler( TimeScaleFunction function )
    {
        return events -> {

            double upscaled;

            List<Double> eventsToUse = events;

            if ( this.isLenient() )
            {
                eventsToUse = eventsToUse.stream()
                                         .filter( Double::isFinite )
                                         .collect( Collectors.toList() );
            }

            switch ( function )
            {
                case MAXIMUM:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Double::valueOf )
                                          .max()
                                          .getAsDouble();
                    break;
                case MEAN:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Double::valueOf )
                                          .average()
                                          .getAsDouble();
                    break;
                case MINIMUM:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Double::valueOf )
                                          .min()
                                          .getAsDouble();
                    break;
                case TOTAL:
                    upscaled = eventsToUse.stream()
                                          .mapToDouble( Double::valueOf )
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

    private RescaledTimeSeriesPlusValidation<Ensemble>
            doTimeIntegralConversion( RescaledTimeSeriesPlusValidation<Ensemble> toIntegrate,
                                      Unit<?> existingUnit,
                                      Unit<?> desiredUnit,
                                      String desiredUnitString,
                                      TimeScaleOuter desiredTimeScale )
    {
        TimeSeries<Ensemble> seriesToIntegrate = toIntegrate.getTimeSeries();
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

        // Create a conversion function for the ensemble members
        UnaryOperator<Double> converter = Units.getTimeIntegralConverter( seriesToIntegrate.getTimeScale(),
                                                                          existingUnit,
                                                                          desiredUnit );
        // Create an ensemble conversion function
        UnaryOperator<Ensemble> ensembleConverter = TimeSeriesSlicer.getEnsembleTransformer( converter );

        // Create a converted time-series
        UnaryOperator<TimeSeriesMetadata> metaMapper =
                RescalingHelper.getMetadataMapper( seriesToIntegrate.getMetadata(),
                                                   desiredTimeScale,
                                                   desiredUnitString );

        TimeSeries<Ensemble> volumeSeries = TimeSeriesSlicer.transform( seriesToIntegrate,
                                                                        ensembleConverter,
                                                                        metaMapper );

        return RescaledTimeSeriesPlusValidation.of( volumeSeries, validationEvents );
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
     * Hidden constructor.
     * 
     * @param isLenient is true if the lenient upscaling is required, false otherwise
     * @param a map of declared unit aliases, possibly empty
     * @throws NullPointerException if the unitAliases is null
     */

    private TimeSeriesOfEnsembleUpscaler( boolean isLenient, Map<String, String> unitAliases )
    {
        Objects.requireNonNull( unitAliases );

        this.isLenient = isLenient;
        this.unitAliases = unitAliases;
    }

}
