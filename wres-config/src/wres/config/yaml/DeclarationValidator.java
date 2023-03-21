package wres.config.yaml;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.yaml.components.DataType;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimeScale;
import wres.config.yaml.components.TimeScaleLenience;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusLevel;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.TimeScale.TimeScaleFunction;

/**
 * <p>Performs high-level validation of an {@link EvaluationDeclaration}. This represents the highest of three levels
 * of validation, namely:
 *
 * <ol>
 * <li>1. Validation that the YAML string is valid YAML, which is performed by
 * {@link DeclarationFactory#from(String)}.</li>
 * <li>2. Validation that the declaration is compatible with the declaration schema, which is performed by
 * {@link DeclarationFactory#from(String)}; and</li>
 * <li>3. Validation that the declaration is internally consistent and reasonable (here).</li>
 * </ol>
 *
 * <p>Unlike lower levels of validation, which are invariant to the subject of the declaration (i.e., an evaluation),
 * this class validates that the evaluation instructions are coherent (e.g., that different pieces of declaration are
 * mutually consistent) and that the declaration as a whole appears to form a reasonable evaluation. Where possible,
 * the schema declares the validation constraints on each of the individual declaration blocks within it. However, in
 * some cases, that validation is sufficiently complex that it is delegated here.
 *
 * <p>Two types of validation events are reported, namely warnings and errors. It is the responsibility of the caller
 * to iterate and act upon these events, but it is expected that warnings will be logged and, finally, an exception
 * thrown that reports on all errors collectively, in order to avoid drip-feeding information.
 *
 * @author James Brown
 */
public class DeclarationValidator
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DeclarationValidator.class );
    /** Upper bound (inclusive) for an instantaneous duration. */
    public static final Duration INSTANTANEOUS_DURATION = Duration.ofSeconds( 60 );
    /** Re-used string. */
    private static final String OBSERVED = "observed";
    /** Re-used string. */
    private static final String PREDICTED = "predicted";
    /** Re-used string. */
    private static final String BASELINE = "baseline";
    /** Re-used string. */
    private static final String VALID_DATES = "'valid_dates'";
    /** Re-used string. */
    private static final String REFERENCE_DATES = "'reference_dates'";
    /** Re-used string. */
    private static final String SOURCE = " source '";
    /** Re-used string. */
    private static final String AGAIN = "again.";
    /** Re-used string. */
    private static final String THE_TIME_SCALE_ASSOCIATED_WITH_THE = "The time scale associated with the ";

    /**
     * Validates the declaration and additionally notifies any events discovered by logging warnings and aggregating
     * errors into an exception. For raw validation, see {@link #validate(EvaluationDeclaration)}.
     *
     * @see #validate(EvaluationDeclaration)
     * @param declaration the declaration to validate
     * @throws DeclarationValidationException if validation errors were encountered
     * @return the valid declaration
     */
    public static EvaluationDeclaration validateAndNotify( EvaluationDeclaration declaration )
    {
        List<EvaluationStatus.EvaluationStatusEvent> events = DeclarationValidator.validate( declaration );

        // Any warnings? Push to log for now, but see #61930 (logging isn't for users)
        if ( LOGGER.isWarnEnabled() )
        {
            List<EvaluationStatus.EvaluationStatusEvent> warnEvents =
                    events.stream()
                          .filter( a -> a.getStatusLevel()
                                        == EvaluationStatus.EvaluationStatusEvent.StatusLevel.WARN )
                          .toList();
            if ( !warnEvents.isEmpty() )
            {
                StringJoiner message = new StringJoiner( System.lineSeparator() );
                String spacer = "    - ";
                warnEvents.forEach( e -> message.add( spacer + e.getEventMessage() ) );

                LOGGER.warn( "Encountered {} warnings when validating the declared evaluation: {}{}",
                             warnEvents.size(),
                             System.lineSeparator(),
                             message );
            }
        }

        // Errors?
        List<EvaluationStatus.EvaluationStatusEvent> errorEvents =
                events.stream()
                      .filter( a -> a.getStatusLevel()
                                    == EvaluationStatus.EvaluationStatusEvent.StatusLevel.ERROR )
                      .toList();
        if ( !errorEvents.isEmpty() )
        {
            StringJoiner message = new StringJoiner( System.lineSeparator() );
            String spacer = "    - ";
            errorEvents.forEach( e -> message.add( spacer + e.getEventMessage() ) );

            throw new DeclarationValidationException( "Encountered "
                                                      + errorEvents.size()
                                                      + " errors in the declared evaluation, which must be fixed:"
                                                      + System.lineSeparator() +
                                                      message );
        }

        return declaration;
    }

    /**
     * Validates the declaration. The validation events are returned in the order they were discovered, reading from
     * the top of the declaration to the bottom. Delegates to the caller to notify about any validation events
     * encountered. For default notification handling, see {@link #validateAndNotify(EvaluationDeclaration)}.
     *
     * @see #validateAndNotify(EvaluationDeclaration)
     * @param declaration the declaration
     * @return the validation events in the order they were discovered
     * @throws NullPointerException if the declaration is null
     */
    public static List<EvaluationStatusEvent> validate( EvaluationDeclaration declaration )
    {
        Objects.requireNonNull( declaration );

        // Insertion order
        List<EvaluationStatusEvent> datasets = DeclarationValidator.validateDatasets( declaration );
        List<EvaluationStatusEvent> events = new ArrayList<>( datasets );

        if ( LOGGER.isDebugEnabled() )
        {
            long warnCount = events.stream().filter( next -> next.getStatusLevel() == StatusLevel.WARN ).count();

            long errorCount = events.stream().filter( next -> next.getStatusLevel() == StatusLevel.ERROR ).count();

            LOGGER.debug( "Encountered {} validation messages, including {} warnings and {} errors.",
                          events.size(),
                          warnCount,
                          errorCount );
        }
        return Collections.unmodifiableList( events );
    }

    /**
     * Validates the dataset declaration.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> validateDatasets( EvaluationDeclaration declaration )
    {
        // Data types are defined for all datasets
        List<EvaluationStatusEvent> typesDefined = DeclarationValidator.typesAreDefined( declaration );
        List<EvaluationStatusEvent> events = new ArrayList<>( typesDefined );
        // Data types are consistent with other declaration
        List<EvaluationStatusEvent> typesConsistent = DeclarationValidator.typesAreConsistent( declaration );
        events.addAll( typesConsistent );
        // Ensembles cannot be present on both left and right sides
        List<EvaluationStatusEvent> ensembles = DeclarationValidator.ensembleOnOneSideOnly( declaration );
        events.addAll( ensembles );
        // Variable must be declared in some circumstances
        List<EvaluationStatusEvent> variables = DeclarationValidator.variablesDeclaredIfRequired( declaration );
        events.addAll( variables );
        // When obtaining data from web services, dates must be defined
        List<EvaluationStatusEvent> services = DeclarationValidator.datesPresentForWebServices( declaration );
        events.addAll( services );
        // Check that the sources are valid
        List<EvaluationStatusEvent> sources = DeclarationValidator.sourcesAreValid( declaration );
        events.addAll( sources );
        // Check that the time scales are valid
        List<EvaluationStatusEvent> timeScales = DeclarationValidator.timeScalesAreValid( declaration );
        events.addAll( timeScales );

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the data type is defined for each dataset.
     *
     * @param declaration the declaration
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> typesAreDefined( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();
        EvaluationStatusEvent.Builder eventBuilder =
                EvaluationStatusEvent.newBuilder().setStatusLevel( StatusLevel.ERROR );
        String start = "The data type was undefined for the ";
        String middle = " data, which means that it was not declared and could not be inferred from the other "
                        + "declaration. Please add the 'type' declaration for the ";
        String end = " data.";

        if ( Objects.isNull( declaration.left()
                                        .type() ) )
        {
            String message = start + OBSERVED + middle + OBSERVED + end;
            EvaluationStatusEvent event = eventBuilder.setEventMessage( message ).build();
            events.add( event );
        }
        if ( Objects.isNull( declaration.right()
                                        .type() ) )
        {
            String message = start + PREDICTED + middle + PREDICTED + end;
            EvaluationStatusEvent event = eventBuilder.setEventMessage( message ).build();
            events.add( event );
        }
        if ( DeclarationValidator.hasBaseline( declaration )
             && Objects.isNull( declaration.baseline()
                                           .dataset()
                                           .type() ) )
        {
            String message = start + BASELINE + middle + BASELINE + end;
            EvaluationStatusEvent event = eventBuilder.setEventMessage( message ).build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the data types are consistent with the other declaration supplied.
     *
     * @param declaration the declaration
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> typesAreConsistent( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // If there is no ensemble data present, there cannot be ensemble-like declaration
        if ( DeclarationValidator.doesNotHaveThisDataType( DataType.ENSEMBLE_FORECASTS, declaration ) )
        {
            Set<String> ensembleDeclaration = DeclarationFactory.getEnsembleDeclaration( declaration );
            if ( !ensembleDeclaration.isEmpty() )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The declaration does not contain any datasets with a "
                                                                 + "data type of "
                                                                 + DataType.ENSEMBLE_FORECASTS
                                                                 + ", but some of the declaration is designed for this "
                                                                 + "data type: "
                                                                 + ensembleDeclaration
                                                                 + ". Please remove this ensemble declaration or "
                                                                 + "correct the data types." )
                                               .build();
                events.add( event );
            }
        }

        // If there are no forecasts present, there cannot be forecast-like declaration
        if ( DeclarationValidator.doesNotHaveThisDataType( DataType.ENSEMBLE_FORECASTS, declaration )
             && DeclarationValidator.doesNotHaveThisDataType( DataType.SINGLE_VALUED_FORECASTS, declaration ) )
        {
            Set<String> forecastDeclaration = DeclarationFactory.getForecastDeclaration( declaration );
            if ( !forecastDeclaration.isEmpty() )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage(
                                                       "The declaration does not contain any datasets with a data "
                                                       + "type of "
                                                       + DataType.ENSEMBLE_FORECASTS
                                                       + " or "
                                                       + DataType.SINGLE_VALUED_FORECASTS
                                                       + ", but some of the declaration is designed for these data "
                                                       + "types: "
                                                       + forecastDeclaration
                                                       + ". Please remove this ensemble declaration or correct the "
                                                       + "data types." )
                                               .build();
                events.add( event );
            }
        }

        // If there are no analyses datasets present, there cannot be declaration for these datasets
        if ( DeclarationValidator.doesNotHaveThisDataType( DataType.ANALYSES, declaration )
             && DeclarationFactory.hasAnalysisDurations( declaration ) )
        {
            EvaluationStatusEvent event = EvaluationStatusEvent.newBuilder()
                                                               .setStatusLevel( StatusLevel.ERROR )
                                                               .setEventMessage(
                                                                       "The declaration does not contain any "
                                                                       + "datasets with a data type of "
                                                                       + DataType.ANALYSES
                                                                       + ", but some of the declaration is "
                                                                       + "designed for this data type: "
                                                                       + "analysis_durations. Please remove this "
                                                                       + "ensemble declaration or correct the data "
                                                                       + "types." )
                                                               .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that ensemble forecasts are not declared on both left and right sides.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> ensembleOnOneSideOnly( EvaluationDeclaration declaration )
    {
        if ( declaration.right().type() == DataType.ENSEMBLE_FORECASTS
             && declaration.left().type() == DataType.ENSEMBLE_FORECASTS )
        {
            EvaluationStatusEvent event = EvaluationStatusEvent.newBuilder()
                                                               .setStatusLevel( StatusLevel.ERROR )
                                                               .setEventMessage( "Cannot evaluate ensemble forecasts "
                                                                                 + "against ensemble forecasts. Please "
                                                                                 + "change the data type for the "
                                                                                 + "observed or predicted data and try "
                                                                                 + AGAIN )
                                                               .build();

            return List.of( event );
        }

        return Collections.emptyList();
    }

    /**
     * Checks that all variables are declared when required.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> variablesDeclaredIfRequired( EvaluationDeclaration declaration )
    {
        String messageStart = "Discovered a data source for the '";
        String messageMiddle = "' data with an interface shorthand of ";
        String messageEnd =
                ", which requires the 'variable' to be declared. Please declare the 'variable' and " + "try "
                + AGAIN;

        EvaluationStatusEvent.Builder eventBuilder =
                EvaluationStatusEvent.newBuilder().setStatusLevel( StatusLevel.ERROR );

        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Check the observed data
        if ( DeclarationValidator.variableIsNotDeclared( declaration, DatasetOrientation.LEFT ) )
        {
            if ( DeclarationValidator.hasSourceInterface( declaration.left().sources(), SourceInterface.USGS_NWIS ) )
            {
                String message = messageStart + OBSERVED + messageMiddle + SourceInterface.USGS_NWIS + messageEnd;
                EvaluationStatusEvent event = eventBuilder.setEventMessage( message ).build();
                events.add( event );
            }

            if ( DeclarationValidator.hasSourceInterface( declaration.left().sources(), SourceInterface.WRDS_NWM ) )
            {
                String message = messageStart + OBSERVED + messageMiddle + SourceInterface.WRDS_NWM + messageEnd;
                EvaluationStatusEvent event = eventBuilder.setEventMessage( message ).build();
                events.add( event );
            }
        }

        // Check the predicted data
        if ( DeclarationValidator.variableIsNotDeclared( declaration, DatasetOrientation.RIGHT ) )
        {
            if ( DeclarationValidator.hasSourceInterface( declaration.right().sources(), SourceInterface.USGS_NWIS ) )
            {
                String message = messageStart + PREDICTED + messageMiddle + SourceInterface.USGS_NWIS + messageEnd;
                EvaluationStatusEvent event = eventBuilder.setEventMessage( message ).build();
                events.add( event );
            }

            if ( DeclarationValidator.hasSourceInterface( declaration.right().sources(), SourceInterface.WRDS_NWM ) )
            {
                String message = messageStart + PREDICTED + messageMiddle + SourceInterface.WRDS_NWM + messageEnd;
                EvaluationStatusEvent event = eventBuilder.setEventMessage( message ).build();
                events.add( event );
            }
        }

        // Check the baseline data
        if ( DeclarationValidator.variableIsNotDeclared( declaration, DatasetOrientation.BASELINE ) )
        {
            if ( DeclarationValidator.hasSourceInterface( declaration.baseline().dataset().sources(),
                                                          SourceInterface.USGS_NWIS ) )
            {
                String message = messageStart + BASELINE + messageMiddle + SourceInterface.USGS_NWIS + messageEnd;
                EvaluationStatusEvent event = eventBuilder.setEventMessage( message ).build();
                events.add( event );
            }

            if ( DeclarationValidator.hasSourceInterface( declaration.baseline().dataset().sources(),
                                                          SourceInterface.WRDS_NWM ) )
            {
                String message = messageStart + BASELINE + messageMiddle + SourceInterface.WRDS_NWM + messageEnd;
                EvaluationStatusEvent event = eventBuilder.setEventMessage( message ).build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that dates are available to constrain requests to web services.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> datesPresentForWebServices( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Some web services declared for left sources?
        if ( DeclarationValidator.hasWebSources( declaration, DatasetOrientation.LEFT ) )
        {
            DataType type = declaration.left()
                                       .type();

            List<EvaluationStatusEvent> leftEvents = DeclarationValidator.datesPresentForWebServices( declaration,
                                                                                                      type,
                                                                                                      OBSERVED );
            events.addAll( leftEvents );
        }

        // Some web services declared for right sources?
        if ( DeclarationValidator.hasWebSources( declaration, DatasetOrientation.RIGHT ) )
        {
            DataType type = declaration.right()
                                       .type();

            List<EvaluationStatusEvent> rightEvents = DeclarationValidator.datesPresentForWebServices( declaration,
                                                                                                       type,
                                                                                                       PREDICTED );
            events.addAll( rightEvents );
        }

        if ( DeclarationValidator.hasWebSources( declaration, DatasetOrientation.BASELINE ) )
        {
            DataType type = declaration.baseline()
                                       .dataset()
                                       .type();

            List<EvaluationStatusEvent> baselineEvents = DeclarationValidator.datesPresentForWebServices( declaration,
                                                                                                          type,
                                                                                                          BASELINE );
            events.addAll( baselineEvents );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that all declared sources are valid.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> sourcesAreValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events =
                new ArrayList<>( DeclarationValidator.sourcesAreValid( declaration.left().sources(),
                                                                       declaration.left().type(),
                                                                       OBSERVED ) );
        List<EvaluationStatusEvent> rightEvents =
                DeclarationValidator.sourcesAreValid( declaration.right().sources(),
                                                      declaration.right().type(),
                                                      PREDICTED );
        events.addAll( rightEvents );

        if ( DeclarationValidator.hasBaseline( declaration ) )
        {
            List<EvaluationStatusEvent> baselineEvents =
                    DeclarationValidator.sourcesAreValid( declaration.baseline().dataset().sources(),
                                                          declaration.baseline().dataset()
                                                                     .type(),
                                                          BASELINE );
            events.addAll( baselineEvents );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that all declared time scales are valid.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> timeScalesAreValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Left sources
        for ( Source nextSource : declaration.left()
                                             .sources() )
        {
            String orientation = OBSERVED + SOURCE + nextSource.uri() + "'";
            List<EvaluationStatusEvent> next = DeclarationValidator.timeScaleIsValid( nextSource.timeScale(),
                                                                                      orientation );
            events.addAll( next );

            // Source timescale must be consistent with desired/evaluation timescale
            List<EvaluationStatusEvent> scaleEvents =
                    DeclarationValidator.sourceTimeScaleConsistentWithEvaluationTimeScale( nextSource.timeScale(),
                                                                                           declaration.timeScale(),
                                                                                           orientation );
            events.addAll( scaleEvents );
        }

        // Right sources
        for ( Source nextSource : declaration.right()
                                             .sources() )
        {
            String orientation = PREDICTED + SOURCE + nextSource.uri() + "'";
            List<EvaluationStatusEvent> next = DeclarationValidator.timeScaleIsValid( nextSource.timeScale(),
                                                                                      orientation );
            events.addAll( next );

            // Source timescale must be consistent with desired/evaluation timescale
            List<EvaluationStatusEvent> scaleEvents =
                    DeclarationValidator.sourceTimeScaleConsistentWithEvaluationTimeScale( nextSource.timeScale(),
                                                                                           declaration.timeScale(),
                                                                                           orientation );
            events.addAll( scaleEvents );
        }

        // Baseline sources, if needed
        if ( DeclarationValidator.hasBaseline( declaration ) )
        {
            for ( Source nextSource : declaration.baseline()
                                                 .dataset()
                                                 .sources() )
            {
                String orientation = BASELINE + SOURCE + nextSource.uri() + "'";
                List<EvaluationStatusEvent> next = DeclarationValidator.timeScaleIsValid( nextSource.timeScale(),
                                                                                          orientation );
                events.addAll( next );

                // Source timescale must be consistent with desired/evaluation timescale
                List<EvaluationStatusEvent> scaleEvents =
                        DeclarationValidator.sourceTimeScaleConsistentWithEvaluationTimeScale( nextSource.timeScale(),
                                                                                               declaration.timeScale(),
                                                                                               orientation );
                events.addAll( scaleEvents );
            }
        }

        // There are extra constraints on the evaluation timescale, so check those
        List<EvaluationStatusEvent> nonInstantaneous =
                DeclarationValidator.evaluationTimeScaleIsValid( declaration.timeScale() );
        events.addAll( nonInstantaneous );

        // Warn if rescaling is lenient
        if ( declaration.rescaleLenience() != TimeScaleLenience.NONE )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "Discovered a rescaling leniency of "
                                                             + declaration.rescaleLenience()
                                                             + ". Care should be exercised when performing lenient "
                                                             + "rescaling, as it implies that a rescaled value will "
                                                             + "be computed even when a majority of data is missing. "
                                                             + "Care is especially needed when performing lenient "
                                                             + "rescaling of model predictions because the missing "
                                                             + "data is unlikely to be missing at random." )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the timescale is not instantaneous.
     * @param timeScale the timescale
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> evaluationTimeScaleIsValid( TimeScale timeScale )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( timeScale ) )
        {
            // Basic elements are valid
            List<EvaluationStatusEvent> evaluationEvents = DeclarationValidator.timeScaleIsValid( timeScale,
                                                                                                  "evaluation" );
            events.addAll( evaluationEvents );

            wres.statistics.generated.TimeScale timeScaleInner = timeScale.timeScale();

            // Cannot be instantaneous
            if ( DeclarationValidator.isInstantaneous( timeScaleInner ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The evaluation 'time_scale' is prescriptive and "
                                                                 + "cannot be instantaneous. Please remove the "
                                                                 + "evaluation 'time_scale' or increase it and try "
                                                                 + AGAIN )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the source timescale is consistent with the evaluation timescale.
     * @param sourceScale the source timescale
     * @param evaluationScale the desired timescale
     * @param context the source context to help with  messaging
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> sourceTimeScaleConsistentWithEvaluationTimeScale( TimeScale sourceScale,
                                                                                                 TimeScale evaluationScale,
                                                                                                 String context )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.isNull( sourceScale ) || Objects.isNull( evaluationScale ) )
        {
            LOGGER.debug( "Not checking the consistency of the source time scale and the evaluation time scale for {} "
                          + "because one or both of the time scales were missing.", context );

            return Collections.emptyList();
        }

        wres.statistics.generated.TimeScale sourceScaleInner = sourceScale.timeScale();
        wres.statistics.generated.TimeScale evaluationScaleInner = evaluationScale.timeScale();

        // If the desired scale is a sum, the existing scale must be instantaneous or the function must be a sum or mean
        if ( evaluationScale.timeScale().getFunction() == TimeScaleFunction.TOTAL
             && !DeclarationValidator.isInstantaneous( sourceScaleInner )
             && sourceScaleInner.getFunction() != TimeScaleFunction.MEAN
             && sourceScaleInner.getFunction() != TimeScaleFunction.TOTAL )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The evaluation 'time_scale' requires a total, but the "
                                                             + "time scale associated with the "
                                                             + context
                                                             + " does not have a supported time scale function from "
                                                             + "which to compute this total. Please change the "
                                                             + "evaluation 'time_scale' and try again." )
                                           .build();
            events.add( event );
        }

        // When the period is present for both timescales, the evaluation timescale period must be an integer multiple
        // of the source timescale period and cannot be smaller
        if ( sourceScaleInner.hasPeriod() && evaluationScaleInner.hasPeriod() )
        {
            Duration sourceDuration = Duration.ofSeconds( sourceScaleInner.getPeriod()
                                                                          .getSeconds(),
                                                          sourceScaleInner.getPeriod()
                                                                          .getNanos() );

            Duration evaluationDuration = Duration.ofSeconds( evaluationScaleInner.getPeriod()
                                                                                  .getSeconds(),
                                                              evaluationScaleInner.getPeriod()
                                                                                  .getNanos() );

            // No downscaling
            if ( evaluationDuration.compareTo( sourceDuration ) <= 0 )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH_THE
                                                                 + context
                                                                 + " is smaller than the evaluation 'time_scale', "
                                                                 + "which is not allowed. Please increase the "
                                                                 + "evaluation 'time_scale' and try again." )
                                               .build();
                events.add( event );
            }

            // No upscaling unless the period is an integer multiple
            if ( evaluationDuration.toMillis() % sourceDuration.toMillis() != 0 )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH_THE
                                                                 + context
                                                                 + " is not exactly divisible by the evaluation "
                                                                 + "'time_scale', which is not allowed. Please change "
                                                                 + "the evaluation 'time_scale' to an integer multiple "
                                                                 + "of every source time scale." )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * @param timeScale the timescale to test
     * @return whether the timescale is instantaneous
     */
    private static boolean isInstantaneous( wres.statistics.generated.TimeScale timeScale )
    {
        if ( timeScale.hasPeriod() )
        {
            Duration duration = Duration.ofSeconds( timeScale.getPeriod()
                                                             .getSeconds(),
                                                    timeScale.getPeriod()
                                                             .getNanos() );

            if ( duration.compareTo( INSTANTANEOUS_DURATION ) <= 0 )
            {
                return true;
            }
        }

        return timeScale.getStartDay() == timeScale.getEndDay()
               && timeScale.getStartMonth() == timeScale.getEndMonth();
    }

    /**
     * Checks that the timescale is valid.
     * @param timeScale the timescale
     * @param orientation the orientation of the timescale to provide context in any error message
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> timeScaleIsValid( TimeScale timeScale,
                                                                 String orientation )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Timescale is present?
        if ( Objects.isNull( timeScale ) )
        {
            LOGGER.debug( "Encountered a missing time scale with orientation {}.", orientation );
            return Collections.emptyList();
        }

        LOGGER.debug( "Discovered a timescale to validate for '{}': {}.", orientation, timeScale );
        wres.statistics.generated.TimeScale timeScaleInner = timeScale.timeScale();

        // Function must be present
        if ( timeScaleInner.getFunction() == wres.statistics.generated.TimeScale.TimeScaleFunction.UNKNOWN )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH_THE
                                                             + orientation
                                                             + " does not have a valid time scale function, which is "
                                                             + "required. Please add the time scale function." )
                                           .build();
            events.add( event );
        }

        // The timescale period must be present, whether explicitly or as a fully-defined season
        if ( !timeScaleInner.hasPeriod()
             && ( Math.min( timeScaleInner.getStartDay(), timeScaleInner.getStartMonth() ) == 0
                  || Math.min( timeScaleInner.getEndDay(), timeScaleInner.getEndMonth() ) == 0 ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH_THE
                                                             + orientation
                                                             + " does not have a valid time period. Either declare "
                                                             + "the 'period' and 'unit' or declare a fully-"
                                                             + "specified time scale season or declare the upper or "
                                                             + "lower bound of the season, together with the 'period' "
                                                             + "and 'unit'." )
                                           .build();
            events.add( event );
        }

        // The season month-days must be complete for each month or day that has been defined
        if ( Math.min( timeScaleInner.getStartDay(), timeScaleInner.getStartMonth() ) == 0
             && Math.max( timeScaleInner.getStartDay(), timeScaleInner.getStartMonth() ) > 0 )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH_THE
                                                             + orientation
                                                             + " is not properly declared. When including either "
                                                             + "a 'minimum_day' or a 'minimum_month', both must be "
                                                             + "present." )
                                           .build();
            events.add( event );
        }
        if ( Math.min( timeScaleInner.getEndDay(), timeScaleInner.getEndMonth() ) == 0
             && Math.max( timeScaleInner.getEndDay(), timeScaleInner.getEndMonth() ) > 0 )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH_THE
                                                             + orientation
                                                             + " is not properly declared. When including either "
                                                             + "a 'maximum_day' or a 'maximum_month', both must be "
                                                             + "present." )
                                           .build();
            events.add( event );
        }

        // If the season declaration is incomplete, then a period must be present
        if ( !timeScaleInner.hasPeriod() && ( timeScaleInner.getStartDay() == 0 || timeScaleInner.getEndDay() == 0
                                              || timeScaleInner.getStartMonth() == 0
                                              || timeScaleInner.getEndMonth() == 0 ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH_THE
                                                             + orientation
                                                             + " is not properly declared. The time scale period must "
                                                             + "be declared explicitly or a time scale season "
                                                             + "fully defined, else a valid combination of the two." )
                                           .build();
            events.add( event );
        }

        // If the season is fully defined, the period cannot be defined
        if ( Math.min( timeScaleInner.getStartDay(), timeScaleInner.getStartMonth() ) > 0
             && Math.max( timeScaleInner.getEndDay(), timeScaleInner.getEndMonth() ) > 0 )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH_THE
                                                             + orientation
                                                             + " is not properly declared. The period cannot be "
                                                             + "declared alongside a fully defined season. Please "
                                                             + "remove the 'period' and 'unit' or remove the  time "
                                                             + "scale season and try again." )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that dates are available to constrain requests to web services.
     * @param declaration the declaration
     * @param type the data type
     * @param orientation the orientation of the data
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> datesPresentForWebServices( EvaluationDeclaration declaration,
                                                                           DataType type,
                                                                           String orientation )
    {
        EvaluationStatusEvent.Builder eventBuilder =
                EvaluationStatusEvent.newBuilder().setStatusLevel( StatusLevel.ERROR );

        List<EvaluationStatusEvent> events = new ArrayList<>();

        String messageStart = "Discovered '";
        String messageStartOuter = "' data sources that have a data type of '";
        String messageMiddleInner = "' and use web services, but the ";
        String messageMiddleOuter =
                " were incomplete or undefined, which is not allowed. Please declare a complete " + "pair of ";
        String messageMiddleFinal = " when acquiring ";
        String messageEnd = " data from web services.";

        // Unknown type with both reference times and valid times incomplete
        if ( Objects.isNull( type )
             && DeclarationValidator.isTimeIntervalIncomplete( declaration.referenceDates() )
             && DeclarationValidator.isTimeIntervalIncomplete( declaration.validDates() ) )
        {
            EvaluationStatusEvent event = eventBuilder.setEventMessage( messageStart + orientation
                                                                        + messageStartOuter + "null"
                                                                        + messageMiddleInner + REFERENCE_DATES + " and "
                                                                        + VALID_DATES + messageMiddleOuter
                                                                        + REFERENCE_DATES + " or " + VALID_DATES
                                                                        + messageMiddleFinal + "time-series"
                                                                        + messageEnd ).build();
            events.add( event );
        }
        // Forecasts with incomplete reference times
        else if ( Objects.nonNull( type )
                  && type.isForecastType()
                  && DeclarationValidator.isTimeIntervalIncomplete( declaration.referenceDates() ) )
        {
            EvaluationStatusEvent event = eventBuilder.setEventMessage( messageStart
                                                                        + orientation + messageStartOuter + type
                                                                        + messageMiddleInner + REFERENCE_DATES
                                                                        + messageMiddleOuter + REFERENCE_DATES
                                                                        + messageMiddleFinal + type + messageEnd )
                                                      .build();
            events.add( event );
        }
        // Non-forecasts with incomplete valid times
        else if ( DeclarationValidator.isTimeIntervalIncomplete( declaration.validDates() ) )
        {
            EvaluationStatusEvent event = eventBuilder.setEventMessage( messageStart + orientation + messageStartOuter
                                                                        + type + messageMiddleInner + VALID_DATES
                                                                        + messageMiddleOuter + VALID_DATES
                                                                        + messageMiddleFinal + type + messageEnd )
                                                      .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that all declared sources are valid.
     * @param sources the source to validate
     * @param type the data type
     * @param orientation the orientation of the sources
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> sourcesAreValid( List<Source> sources,
                                                                DataType type,
                                                                String orientation )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();
        if ( Objects.nonNull( sources ) )
        {
            // Warn about time zone offset
            if ( sources.stream().anyMatch( next -> Objects.nonNull( next.timeZoneOffset() ) ) )
            {
                EvaluationStatusEvent event =
                        EvaluationStatusEvent.newBuilder()
                                             .setStatusLevel( StatusLevel.WARN )
                                             .setEventMessage( "Discovered one or more '"
                                                               + orientation
                                                               + "' data sources for which a time zone was declared. "
                                                               + "This information is generally not required and will "
                                                               + "be ignored if the data source itself contains a time "
                                                               + "zone." )
                                             .build();
                events.add( event );
            }

            // Check that each source interface is consistent with the data type
            List<EvaluationStatusEvent> interfaceEvents
                    = DeclarationValidator.interfacesAreConsistentWithTheDataType( sources, type, orientation );
            events.addAll( interfaceEvents );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that all declared sources are valid.
     * @param sources the source to validate
     * @param orientation the orientation of the sources
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> interfacesAreConsistentWithTheDataType( List<Source> sources,
                                                                                       DataType type,
                                                                                       String orientation )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();
        if ( Objects.nonNull( sources ) )
        {
            // Check whether the data type is contained in the data types supported by each source interface. If not,
            // then further check whether the data type is forecast-like and, in that case, add an error event
            // because forecasts of different types are unlike each other and unlike non-forecasts. If the data type is
            // a non-forecast, check whether any of the supported interface data types are non-forecasts. If not, add an
            // error because non-forecasts are unlike forecasts, otherwise add a warning because non-forecasts are
            // broadly similar, even if the exact type differs
            List<SourceInterface> interfaces = sources.stream()
                                                      .map( Source::api )
                                                      .filter( Objects::nonNull )
                                                      .toList();

            // Iterate through the interfaces and compare the implied data types to the declared or inferred type for
            // this side of data
            for ( SourceInterface nextInterface : interfaces )
            {
                Set<DataType> types = nextInterface.getDataTypes();

                if ( !types.contains( type ) )
                {
                    // Either the declared data type is a forecast or the set of types does not contain any
                    // observation-like data
                    if ( type.isForecastType() || types.stream()
                                                       .allMatch( DataType::isForecastType ) )
                    {
                        EvaluationStatusEvent event =
                                EvaluationStatusEvent.newBuilder()
                                                     .setStatusLevel( StatusLevel.ERROR )
                                                     .setEventMessage(
                                                             "When inspecting the interfaces associated with the "
                                                             + orientation + " data, discovered an interface of "
                                                             + nextInterface
                                                             + ", which admits the data types "
                                                             + types
                                                             + ", but the declared or inferred data type for the "
                                                             + orientation
                                                             + " data was "
                                                             + type
                                                             + ", which is inconsistent with the interface. "
                                                             + "Please correct the interface or declare the "
                                                             + "correct data type for the "
                                                             + orientation
                                                             + " data." )
                                                     .build();
                        events.add( event );
                    }
                    else
                    {
                        EvaluationStatusEvent event =
                                EvaluationStatusEvent.newBuilder()
                                                     .setStatusLevel( StatusLevel.WARN )
                                                     .setEventMessage(
                                                             "When inspecting the interfaces associated with the "
                                                             + orientation + " data, discovered an interface of "
                                                             + nextInterface
                                                             + ", which admits the data types "
                                                             + types
                                                             + ", but the declared or inferred data type for the "
                                                             + orientation
                                                             + " data was "
                                                             + type
                                                             + ", which is inconsistent with the interface. "
                                                             + "However, since the data type is observation-like "
                                                             + "and the interface supports other observation-like "
                                                             + "data types, the declaration is permitted. If the "
                                                             + "interface and/or data type is wrong, please "
                                                             + "correct them as needed." )
                                                     .build();
                        events.add( event );
                    }
                }
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * @param declaration the declaration
     * @return whether web sources are declared for the dataset with the prescribed orientation
     */
    private static boolean hasWebSources( EvaluationDeclaration declaration, DatasetOrientation orientation )
    {
        if ( orientation == DatasetOrientation.LEFT )
        {
            return DeclarationValidator.hasSourceInterface( declaration.left().sources(),
                                                            SourceInterface.USGS_NWIS,
                                                            SourceInterface.WRDS_AHPS,
                                                            SourceInterface.WRDS_NWM );
        }
        else if ( orientation == DatasetOrientation.RIGHT )
        {
            return DeclarationValidator.hasSourceInterface( declaration.right().sources(),
                                                            SourceInterface.USGS_NWIS,
                                                            SourceInterface.WRDS_AHPS,
                                                            SourceInterface.WRDS_NWM );
        }
        else if ( DeclarationValidator.hasBaseline( declaration ) && orientation == DatasetOrientation.BASELINE )
        {
            return DeclarationValidator.hasSourceInterface( declaration.baseline().dataset().sources(),
                                                            SourceInterface.USGS_NWIS,
                                                            SourceInterface.WRDS_AHPS,
                                                            SourceInterface.WRDS_NWM );
        }

        return false;
    }

    /**
     * @param declaration the declaration
     * @return whether the variable is defined for the dataset with the prescribed orientation
     */
    private static boolean variableIsNotDeclared( EvaluationDeclaration declaration, DatasetOrientation orientation )
    {
        if ( orientation == DatasetOrientation.LEFT )
        {
            return Objects.isNull( declaration.left().variable() );
        }
        else if ( orientation == DatasetOrientation.RIGHT )
        {
            return Objects.isNull( declaration.right().variable() );
        }
        else
        {
            return DeclarationValidator.hasBaseline( declaration ) && Objects.isNull( declaration.baseline()
                                                                                                 .dataset()
                                                                                                 .variable() );
        }
    }

    /**
     * @param timeInterval the time interval
     * @return whether the time interval is fully defined
     */
    private static boolean isTimeIntervalIncomplete( TimeInterval timeInterval )
    {
        return Objects.isNull( timeInterval ) || Objects.isNull( timeInterval.minimum() ) || Objects.isNull(
                timeInterval.maximum() );
    }

    /**
     * @param api the api to find
     * @param sources the sources to check
     * @return whether any source contains the designated api
     */
    private static boolean hasSourceInterface( List<Source> sources, SourceInterface... api )
    {
        Set<SourceInterface> apis = Arrays.stream( api ).collect( Collectors.toSet() );
        return Objects.nonNull( sources ) && sources.stream().map( Source::api ).anyMatch( apis::contains );
    }

    /**
     * @param declaration the declaration
     * @return whether the declaration contains a baseline dataset
     */
    private static boolean hasBaseline( EvaluationDeclaration declaration )
    {
        return Objects.nonNull( declaration.baseline() );
    }

    /**
     * @param type the data type
     * @param declaration the declaration
     * @return whether the declaration has the data type on any side
     */

    private static boolean doesNotHaveThisDataType( DataType type, EvaluationDeclaration declaration )
    {
        return declaration.left().type() != type
               && declaration.right().type() != type
               && ( !DeclarationValidator.hasBaseline( declaration )
                    || declaration.baseline().dataset().type() != type );
    }

    /**
     * Do not construct.
     */
    private DeclarationValidator()
    {
    }
}
