package wres.config.yaml;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.MetricConstants;
import wres.config.yaml.components.AnalysisDurations;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.Season;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.SpatialMask;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimeScale;
import wres.config.yaml.components.TimeScaleLenience;
import wres.config.yaml.components.UnitAlias;
import wres.config.yaml.components.Metric;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusLevel;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.statistics.generated.GeometryTuple;

import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.geom.Geometry;

/**
 * <p>Performs high-level validation of an {@link EvaluationDeclaration}. This represents the highest of three levels
 * of validation, namely:
 *
 * <ol>
 * <li>1. Validation that the YAML string is valid YAML, which is performed by
 * {@link DeclarationFactory#from(String)}.</li>
 * <li>2. Validation that the declaration is compatible with the declaration schema, which is performed by
 * {@link DeclarationFactory#from(String)}; and</li>
 * <li>3. Validation that the declaration is internally consistent and reasonable (i.e., "business logic", here).</li>
 * </ol>
 *
 * <p>Unlike lower levels of validation, which are concerned with "well-posed" declaration, this class validates
 * the "business logic" of an evaluation, i.e., that the evaluation instructions are coherent (e.g., that different
 * pieces of declaration are mutually consistent) and that the declaration as a whole appears to form a reasonable
 * evaluation.
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
     * @throws DeclarationException if validation errors were encountered
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

            throw new DeclarationException( "Encountered "
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
        // Check that the unit aliases are valid
        List<EvaluationStatusEvent> unitAliases = DeclarationValidator.unitAliasesAreValid( declaration );
        events.addAll( unitAliases );
        // Check that all date intervals are valid
        List<EvaluationStatusEvent> dates = DeclarationValidator.timeIntervalsAreValid( declaration );
        events.addAll( dates );
        // Check that the season is valid
        List<EvaluationStatusEvent> season = DeclarationValidator.seasonIsValid( declaration );
        events.addAll( season );
        // Check that the time pools are valid
        List<EvaluationStatusEvent> pools = DeclarationValidator.timePoolsAreValid( declaration );
        events.addAll( pools );
        // Check that the spatial mask is valid
        List<EvaluationStatusEvent> mask = DeclarationValidator.spatialMaskIsValid( declaration );
        events.addAll( mask );
        // Check that the feature declaration is valid
        List<EvaluationStatusEvent> features = DeclarationValidator.featuresAreValid( declaration );
        events.addAll( features );
        // Check that the metrics declaration is valid
        List<EvaluationStatusEvent> metrics = DeclarationValidator.metricsAreValid( declaration );
        events.addAll( metrics );
        // Check that the output formats declaration is valid
        List<EvaluationStatusEvent> outputs = DeclarationValidator.outputFormatsAreValid( declaration );
        events.addAll( outputs );

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

        // If there are no analyses datasets present, there cannot be declaration for analyses
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

        // Cannot have probability classifier thresholds without ensemble forecasts
        if ( DeclarationValidator.doesNotHaveThisDataType( DataType.ENSEMBLE_FORECASTS, declaration )
             && DeclarationValidator.hasThresholdsOfThisType( ThresholdType.PROBABILITY_CLASSIFIER, declaration ) )
        {
            EvaluationStatusEvent event = EvaluationStatusEvent.newBuilder()
                                                               .setStatusLevel( StatusLevel.ERROR )
                                                               .setEventMessage(
                                                                       "The declaration contains one or more "
                                                                       + "'classifier_thresholds', but these "
                                                                       + "thresholds are only allowed for "
                                                                       + "ensemble forecasts. Please correct the "
                                                                       + "data types or remove the "
                                                                       + "'classifier_thresholds'." )
                                                               .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * @param thresholdType the threshold type
     * @param declaration the declaration
     * @return whether the declaration contains any thresholds of this type
     */

    private static boolean hasThresholdsOfThisType( ThresholdType thresholdType, EvaluationDeclaration declaration )
    {
        if ( thresholdType == ThresholdType.PROBABILITY )
        {
            return !declaration.probabilityThresholds()
                               .isEmpty()
                   || declaration.thresholdSets()
                                 .stream()
                                 .anyMatch( next -> next.type() == ThresholdType.PROBABILITY )
                   || declaration.metrics()
                                 .stream()
                                 .map( Metric::parameters )
                                 .filter( Objects::nonNull )
                                 .anyMatch( next -> Objects.nonNull( next.probabilityThresholds() ) );
        }
        else if ( thresholdType == ThresholdType.PROBABILITY_CLASSIFIER )
        {
            return !declaration.classifierThresholds()
                               .isEmpty()
                   || declaration.thresholdSets()
                                 .stream()
                                 .anyMatch( next -> next.type() == ThresholdType.PROBABILITY_CLASSIFIER )
                   || declaration.metrics()
                                 .stream()
                                 .map( Metric::parameters )
                                 .filter( Objects::nonNull )
                                 .anyMatch( next -> Objects.nonNull( next.classifierThresholds() ) );
        }
        else if ( thresholdType == ThresholdType.VALUE )
        {
            return !declaration.valueThresholds()
                               .isEmpty()
                   || declaration.thresholdSets()
                                 .stream()
                                 .anyMatch( next -> next.type() == ThresholdType.VALUE )
                   || declaration.metrics()
                                 .stream()
                                 .map( Metric::parameters )
                                 .filter( Objects::nonNull )
                                 .anyMatch( next -> Objects.nonNull( next.valueThresholds() ) );
        }
        else
        {
            throw new IllegalArgumentException( "Unrecognized threshold type: " + thresholdType + "." );
        }
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
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Check for source interfaces that require a variable
        EvaluationStatusEvent.Builder eventBuilder =
                EvaluationStatusEvent.newBuilder().setStatusLevel( StatusLevel.ERROR );

        String messageStart = "Discovered a data source for the '";
        String messageMiddle = "' data with an interface shorthand of ";
        String messageEnd = ", which requires the 'variable' to be declared. Please declare the 'variable' and try "
                            + AGAIN;

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
     * Checks that the unit aliases are valid.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> unitAliasesAreValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( declaration.unitAliases() ) )
        {
            Set<UnitAlias> unitAliases = declaration.unitAliases();

            // Group by alias and filter groups without duplicates
            Map<String, List<UnitAlias>> duplicates = unitAliases.stream()
                                                                 .collect( Collectors.groupingBy( UnitAlias::alias ) )
                                                                 .entrySet()
                                                                 .stream()
                                                                 .filter( next -> next.getValue().size() > 1 )
                                                                 .collect( Collectors.toMap( Map.Entry::getKey,
                                                                                             Map.Entry::getValue ) );

            // Duplicates are not allowed
            if ( !duplicates.isEmpty() )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "When validating the declared unit aliases, "
                                                                 + "discovered the same alias associated with multiple "
                                                                 + "units, which is not allowed. Please remove the "
                                                                 + "following duplicates and try again:  "
                                                                 + duplicates
                                                                 + "." )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the date intervals are valid.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> timeIntervalsAreValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events
                = new ArrayList<>( DeclarationValidator.timeIntervalIsValid( declaration.referenceDates(),
                                                                             REFERENCE_DATES ) );
        List<EvaluationStatusEvent> validDates = DeclarationValidator.timeIntervalIsValid( declaration.referenceDates(),
                                                                                           VALID_DATES );
        events.addAll( validDates );
        List<EvaluationStatusEvent> analysisDurations
                = DeclarationValidator.analysisDurationsAreValid( declaration.analysisDurations() );
        events.addAll( analysisDurations );

        // Lead times
        LeadTimeInterval leadTimes = declaration.leadTimes();
        if ( Objects.nonNull( leadTimes )
             && Objects.nonNull( leadTimes.minimum() )
             && Objects.nonNull( leadTimes.maximum() )
             && leadTimes.maximum() <= declaration.leadTimes()
                                                  .minimum() )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The 'lead_times' interval is invalid because the "
                                                             + "'minimum' value is greater than or equal to the "
                                                             + "'maximum' value. Please adjust the 'minimum' to occur "
                                                             + "before the 'maximum' and try "
                                                             + AGAIN )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the season declaration is valid.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> seasonIsValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();
        Season season = declaration.season();
        if ( Objects.nonNull( season ) )
        {
            if ( season.minimum()
                       .isAfter( season.maximum() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( "The lower bound of the 'season' is later than the "
                                                                 + "upper bound. Although it is valid to wrap a season "
                                                                 + "around a calendar year end, it is unusual. Please "
                                                                 + "check your declaration and, if needed, adjust it." )
                                               .build();
                events.add( event );
            }
            else if ( season.minimum()
                            .equals( season.maximum() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The lower and upper bounds of the season refer to "
                                                                 + "the same day and month, which is not allowed. A "
                                                                 + "season must span a non-zero time interval. Please "
                                                                 + "correct the 'season' declaration and try "
                                                                 + AGAIN )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the time pools are valid.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> timePoolsAreValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> validPools = DeclarationValidator.timePoolIsValid( declaration.validDatePools(),
                                                                                       declaration.validDates(),
                                                                                       "valid_date_pools",
                                                                                       "valid_dates" );
        List<EvaluationStatusEvent> events = new ArrayList<>( validPools );

        List<EvaluationStatusEvent> referencePools = DeclarationValidator.timePoolIsValid( declaration.validDatePools(),
                                                                                           declaration.validDates(),
                                                                                           "reference_date_pools",
                                                                                           "reference_dates" );
        events.addAll( referencePools );

        List<EvaluationStatusEvent> leadTimePools =
                DeclarationValidator.leadTimePoolIsValid( declaration.leadTimePools(),
                                                          declaration.leadTimes() );
        events.addAll( leadTimePools );

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the spatial mask is valid.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> spatialMaskIsValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        SpatialMask mask = declaration.spatialMask();
        if ( Objects.nonNull( mask ) && Objects.nonNull( mask.wkt() ) )
        {
            WKTReader reader = new WKTReader();
            String wkt = mask.wkt();

            try
            {
                Geometry geometry = reader.read( wkt );
                LOGGER.debug( "Read the wkt string {} into a geometry, {}.", wkt, geometry );
            }
            catch ( ParseException e )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The 'wkt' string associated with the 'spatial_mask' "
                                                                 + "could not be parsed into a geometry. Please fix "
                                                                 + "the 'wkt' string and try again." )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the feature declaration is valid.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> featuresAreValid( EvaluationDeclaration declaration )
    {
        // Features are declared when needed
        List<EvaluationStatusEvent> featuresPresent =
                DeclarationValidator.checkFeaturesPresentWhenSourcesNeed( declaration );
        List<EvaluationStatusEvent> events = new ArrayList<>( featuresPresent );
        // Check that a baseline data source is present when some features declare a baseline feature
        List<EvaluationStatusEvent> baseline =
                DeclarationValidator.checkBaselinePresentWhenFeaturesIncludeBaseline( declaration );
        events.addAll( baseline );

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the metrics declaration is valid.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> metricsAreValid( EvaluationDeclaration declaration )
    {
        // Time-series metrics require single-valued forecasts
        List<EvaluationStatusEvent> singleValued =
                DeclarationValidator.checkSingleValuedForecastsForTimeSeriesMetrics( declaration );
        List<EvaluationStatusEvent> events = new ArrayList<>( singleValued );
        // Baseline defined for metrics that require one
        List<EvaluationStatusEvent> baselinePresent =
                DeclarationValidator.checkBaselinePresentForMetricsThatNeedIt( declaration );
        events.addAll( baselinePresent );
        // When categorical metrics are declared, there must be event thresholds as a minimum
        List<EvaluationStatusEvent> categoricalMetrics =
                DeclarationValidator.checkEventThresholdsForCategoricalMetrics( declaration );
        events.addAll( categoricalMetrics );
        // Warning when non-score metrics are combined with date pools for legacy CSV
        List<EvaluationStatusEvent> legacyCsv =
                DeclarationValidator.checkMetricsForLegacyCsvAndDatePools( declaration );
        events.addAll( legacyCsv );

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the output formats declaration is valid.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> outputFormatsAreValid( EvaluationDeclaration declaration )
    {
        // Warn about deprecated types
        List<EvaluationStatusEvent> deprecated =
                DeclarationValidator.checkForDeprecatedOutputFormats( declaration );
        List<EvaluationStatusEvent> events = new ArrayList<>( deprecated );
        // Validate NetCDF, which has cumbersome requirements
        List<EvaluationStatusEvent> netcdf = DeclarationValidator.validateNetcdfOutput( declaration );
        events.addAll( netcdf );

        return Collections.unmodifiableList( events );
    }

    /**
     * Warns about deprecated output formats.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> checkForDeprecatedOutputFormats( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        Formats formats = declaration.formats();
        if ( Objects.nonNull( formats ) )
        {
            String start = "The declaration requested '";
            String middle =
                    "' format, which has been marked deprecated and will be removed from a future version of the "
                    + "software without warning. It is recommended that you substitute this format with the '";
            String end = "' format.";
            if ( Objects.nonNull( formats.csvFormat() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( start + "csv" + middle + "csv2" + end )
                                               .build();
                events.add( event );
            }

            if ( Objects.nonNull( formats.netcdfFormat() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( start + "netcdf" + middle + "netcdf2" + end )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Validates the NetCDF output declaration.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> validateNetcdfOutput( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Do not allow both legacy netcdf and netcdf2 together
        Formats formats = declaration.formats();
        if ( Objects.nonNull( formats ) )
        {
            if ( Objects.nonNull( formats.netcdfFormat() ) && Objects.nonNull( formats.netcdf2Format() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The 'output_formats' includes both 'netcdf' and "
                                                                 + "'netcdf2', which is not allowed. One of these format "
                                                                 + "options must be removed and it is recommended "
                                                                 + "that you remove the 'netcdf' option." )
                                               .build();
                events.add( event );
            }

            // Do not allow legacy netcdf together with feature groups
            if ( Objects.nonNull( formats.netcdfFormat() ) && Objects.nonNull( declaration.featureGroups() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage(
                                                       "The 'output_formats' includes 'netcdf', which does not "
                                                       + "support 'feature_groups'. Please replace the 'netcdf'"
                                                       + "option with 'netcdf2', which does support "
                                                       + "'feature_groups'." )
                                               .build();
                events.add( event );
            }

            // Warn about netcdf2 when feature groups are declared
            if ( Objects.nonNull( formats.netcdf2Format() ) && Objects.nonNull( declaration.featureGroups() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage(
                                                       "The 'output_formats' includes 'netcdf2', which supports "
                                                       + "'feature_groups', but the group statistics are "
                                                       + "repeated across every member of the group." )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that single-valued forecasts are present when declaring time-series metrics.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> checkSingleValuedForecastsForTimeSeriesMetrics( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        Set<MetricConstants> metrics =
                DeclarationValidator.getSingleValuedTimeSeriesMetrics( declaration );

        if ( declaration.right()
                        .type() != DataType.SINGLE_VALUED_FORECASTS
             && !metrics.isEmpty() )

        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The declared or inferred data 'type' for the 'predicted' "
                                                             + "dataset is "
                                                             + declaration.right()
                                                                          .type()
                                                             + ", but the following metrics require single-valued "
                                                             + "forecasts: "
                                                             + metrics
                                                             + ". Please remove these metrics or correct the data "
                                                             + "'type' to 'single valued forecasts'." )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that a baseline is declared when metrics are included that require it.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> checkBaselinePresentForMetricsThatNeedIt( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        Set<MetricConstants> metrics = declaration.metrics()
                                                  .stream()
                                                  .map( Metric::name )
                                                  .filter( next -> next
                                                                   == MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE )
                                                  .collect( Collectors.toSet() );

        if ( !DeclarationValidator.hasBaseline( declaration ) && !metrics.isEmpty() )

        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The declaration includes metrics that require an "
                                                             + "explicit 'baseline' dataset, but no baseline dataset "
                                                             + "was found. Please remove the following metrics from "
                                                             + "the declaration or add a baseline dataset and try "
                                                             + "again: "
                                                             + metrics
                                                             + "." )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that event thresholds are defined when categorical metrics are included.
     * @param declaration the declaration
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> checkEventThresholdsForCategoricalMetrics( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Categorical metrics are present
        Predicate<MetricConstants> filter = next -> next.isInGroup( MetricConstants.SampleDataGroup.DICHOTOMOUS )
                                                    || next.isInGroup( MetricConstants.SampleDataGroup.MULTICATEGORY );
        Set<MetricConstants> metrics = declaration.metrics()
                                                  .stream()
                                                  .map( Metric::name )
                                                  .filter( filter )
                                                  .collect( Collectors.toSet() );
        LOGGER.debug( "Discovered the following categorical metrics to validate against other declaration: {}.",
                      metrics );

        if ( !metrics.isEmpty() )
        {
            // No event thresholds
            if ( !DeclarationValidator.hasThresholdsOfThisType( ThresholdType.PROBABILITY, declaration )
                 && !DeclarationValidator.hasThresholdsOfThisType( ThresholdType.VALUE, declaration ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The declaration includes metrics that require either "
                                                                 + "'probability_thresholds' or 'value_thresholds' "
                                                                 + "but none were found. Please remove the following "
                                                                 + "metrics or add the required thresholds and try "
                                                                 + "again: "
                                                                 + metrics
                                                                 + "." )
                                               .build();
                events.add( event );
            }

            // Ensembles, but no decision/classifier thresholds: warn
            if ( declaration.right().type() == DataType.ENSEMBLE_FORECASTS
                 && !DeclarationValidator.hasThresholdsOfThisType( ThresholdType.PROBABILITY_CLASSIFIER, declaration ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( "The declaration includes ensemble forecasts and "
                                                                 + "metrics for categorical datasets, but does not "
                                                                 + "include any 'classifier_thresholds'. This is "
                                                                 + "allowed, but the following metrics will be "
                                                                 + "computed for the ensemble average only: "
                                                                 + metrics
                                                                 + ". If you want to calculate these metrics using the "
                                                                 + "forecast probabilities, please add "
                                                                 + "'classifier_thresholds' to help classify the "
                                                                 + "forecast probabilities into categorical outcomes." )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Warns if non-score metrics are required alongside the legacy CSV statistics format and pooling windows.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> checkMetricsForLegacyCsvAndDatePools( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Legacy CSV declared
        if ( Objects.nonNull( declaration.formats() ) && Objects.nonNull( declaration.formats()
                                                                                     .csvFormat() ) )
        {
            // Non-score metrics
            Predicate<MetricConstants> filter =
                    next -> !next.isInGroup( MetricConstants.StatisticType.DOUBLE_SCORE )
                            && !next.isInGroup( MetricConstants.StatisticType.DURATION_SCORE );
            Set<MetricConstants> metrics = declaration.metrics()
                                                      .stream()
                                                      .map( Metric::name )
                                                      .filter( filter )
                                                      .collect( Collectors.toSet() );

            if ( Objects.nonNull( declaration.validDatePools() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( "Some of the declared metrics cannot be written to "
                                                                 + "the legacy CSV format because the format does not "
                                                                 + "support these metrics in combination with "
                                                                 + "'valid_date_pools'. Please consider using the CSV2 "
                                                                 + "format instead: "
                                                                 + metrics
                                                                 + "." )
                                               .build();
                events.add( event );
            }

            if ( Objects.nonNull( declaration.referenceDatePools() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( "Some of the declared metrics cannot be written to "
                                                                 + "the legacy CSV format because the format does not "
                                                                 + "support these metrics in combination with "
                                                                 + "'reference_date_pools'. Please consider using the "
                                                                 + "CSV2 format instead: "
                                                                 + metrics
                                                                 + "." )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * @param declaration the declaration
     * @return the declared metrics in the specified group, if any
     */

    private static Set<MetricConstants> getSingleValuedTimeSeriesMetrics( EvaluationDeclaration declaration )
    {
        return declaration.metrics()
                          .stream()
                          .map( Metric::name )
                          .filter( next -> next.isInGroup( MetricConstants.SampleDataGroup.SINGLE_VALUED_TIME_SERIES ) )
                          .collect( Collectors.toSet() );
    }

    /**
     * Checks that features are declared when the sources need them.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> checkFeaturesPresentWhenSourcesNeed( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // No features declared
        if ( Objects.isNull( declaration.features() )
             && Objects.isNull( declaration.featureGroups() )
             && Objects.isNull( declaration.featureService() ) )
        {
            EvaluationStatusEvent.Builder eventBuilder
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR );

            String start = "No geospatial features were defined, but web sources were declared for the '";
            String middle = "' dataset, which requires features to be defined.";
            String end = "Please add some geospatial features to the declaration (e.g., 'features', 'feature_groups' "
                         + "or 'feature_service') and try again.";

            // Web services require features
            if ( DeclarationValidator.hasWebSources( declaration, DatasetOrientation.LEFT ) )
            {
                eventBuilder.setEventMessage( start + OBSERVED + middle + end );
                EvaluationStatusEvent event = eventBuilder.build();
                events.add( event );
            }
            if ( DeclarationValidator.hasWebSources( declaration, DatasetOrientation.RIGHT ) )
            {
                eventBuilder.setEventMessage( start + PREDICTED + middle + end );
                EvaluationStatusEvent event = eventBuilder.build();
                events.add( event );
            }
            if ( DeclarationValidator.hasWebSources( declaration, DatasetOrientation.BASELINE ) )
            {
                eventBuilder.setEventMessage( start + BASELINE + middle + end );
                EvaluationStatusEvent event = eventBuilder.build();
                events.add( event );
            }

            // Other source interfaces that require features to be defined
            List<EvaluationStatusEvent> nwmSources =
                    DeclarationValidator.checkForNwmSourcesWhenNoFeatures( declaration );

            events.addAll( nwmSources );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that no National Water Model sources are present when features are undefined.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> checkForNwmSourcesWhenNoFeatures( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        EvaluationStatusEvent.Builder eventBuilder
                = EvaluationStatusEvent.newBuilder()
                                       .setStatusLevel( StatusLevel.ERROR );

        // Features are required for some source interfaces
        String start = "No geospatial features were defined, but source interfaces that require features were declared "
                       + "for the '";
        String middle = "' dataset: ";
        String end = "Please add some geospatial features to the declaration (e.g., 'features', 'feature_groups' or "
                     + "'feature_service') and try again.";

        Set<SourceInterface> leftRequireFeatures =
                DeclarationValidator.getSourceInterfacesThatBeginWithNwm( declaration.left()
                                                                                     .sources() );
        if ( !leftRequireFeatures.isEmpty() )
        {
            eventBuilder.setEventMessage( start + OBSERVED + middle + leftRequireFeatures + end );
            EvaluationStatusEvent event = eventBuilder.build();
            events.add( event );
        }
        Set<SourceInterface> rightRequireFeatures =
                DeclarationValidator.getSourceInterfacesThatBeginWithNwm( declaration.right()
                                                                                     .sources() );
        if ( !rightRequireFeatures.isEmpty() )
        {
            eventBuilder.setEventMessage( start + PREDICTED + middle + rightRequireFeatures + end );
            EvaluationStatusEvent event = eventBuilder.build();
            events.add( event );
        }

        if ( DeclarationValidator.hasBaseline( declaration ) )
        {
            Set<SourceInterface> baselineRequireFeatures =
                    DeclarationValidator.getSourceInterfacesThatBeginWithNwm( declaration.baseline()
                                                                                         .dataset()
                                                                                         .sources() );
            if ( !baselineRequireFeatures.isEmpty() )
            {
                eventBuilder.setEventMessage( start + BASELINE + middle + baselineRequireFeatures + end );
                EvaluationStatusEvent event = eventBuilder.build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that a baseline source is present when features include a baseline.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> checkBaselinePresentWhenFeaturesIncludeBaseline( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // No baseline present, then cannot declare baseline features
        if ( !DeclarationValidator.hasBaseline( declaration ) )
        {
            boolean singletonHasBaseline = Objects.nonNull( declaration.features() )
                                           && declaration.features()
                                                         .geometries()
                                                         .stream()
                                                         .anyMatch( GeometryTuple::hasBaseline );

            boolean groupHasBaseline = Objects.nonNull( declaration.featureGroups() )
                                       && declaration.featureGroups()
                                                     .geometryGroups()
                                                     .stream()
                                                     .flatMap( next -> next.getGeometryTuplesList()
                                                                           .stream() )
                                                     .anyMatch( GeometryTuple::hasBaseline );
            if ( singletonHasBaseline || groupHasBaseline )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The declaration contains one or more geospatial "
                                                                 + "features for a baseline dataset but no baseline "
                                                                 + "dataset is defined. Please add a baseline dataset "
                                                                 + "or remove the baseline features and try again." )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * @param sources the sources to search
     * @return a set of source interfaces that begin with the specified characters
     */

    private static Set<SourceInterface> getSourceInterfacesThatBeginWithNwm( List<Source> sources )
    {
        return sources.stream()
                      .map( Source::sourceInterface )
                      .filter( next -> Objects.nonNull( next ) && next.toString()
                                                                      .toLowerCase()
                                                                      .startsWith( "nwm" ) )
                      .collect( Collectors.toSet() );
    }

    /**
     * Checks that the time pools are valid.
     * @param pools the time pools
     * @param interval interval the interval
     * @param poolName the pool name to help with messaging
     * @param intervalName the interval name to help with messaging
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> timePoolIsValid( TimePools pools,
                                                                TimeInterval interval,
                                                                String poolName,
                                                                String intervalName )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( pools ) )
        {
            // Time interval must be fully declared
            if ( Objects.isNull( interval )
                 || Objects.isNull( interval.minimum() )
                 || Objects.isNull( interval.maximum() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The declaration included '"
                                                                 + poolName
                                                                 + "', which requires the '"
                                                                 + intervalName
                                                                 + "' to be fully declared. Please remove the '"
                                                                 + poolName
                                                                 + "' or fully declare the '"
                                                                 + intervalName
                                                                 + "' and try again." )
                                               .build();
                events.add( event );
            }
            // The time pools declaration must produce at least one pool
            else
            {
                // Create the elements necessary to increment them
                ChronoUnit periodUnits = pools.unit();
                Duration period = Duration.of( pools.period(), periodUnits );
                Instant start = interval.minimum();
                Instant end = interval.maximum();

                if ( start.plus( period )
                          .isAfter( end ) )
                {
                    EvaluationStatusEvent event
                            = EvaluationStatusEvent.newBuilder()
                                                   .setStatusLevel( StatusLevel.ERROR )
                                                   .setEventMessage( "The declaration requested '"
                                                                     + poolName
                                                                     + "', but none could be produced because the "
                                                                     + "'minimum' time associated with the '"
                                                                     + intervalName
                                                                     + "' plus the 'period' associated with the '"
                                                                     + poolName
                                                                     + "' is later than the 'maximum' time associated "
                                                                     + "with the '"
                                                                     + intervalName
                                                                     + "'. Please adjust the '"
                                                                     + poolName
                                                                     + "' and/or the '"
                                                                     + intervalName
                                                                     + "' and try again. " )
                                                   .build();
                    events.add( event );
                }
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the lead time pools are valid.
     * @param pools the time pools
     * @param interval interval the interval
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> leadTimePoolIsValid( TimePools pools,
                                                                    LeadTimeInterval interval )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( pools ) )
        {
            // Time interval must be fully declared
            if ( Objects.isNull( interval )
                 || Objects.isNull( interval.minimum() )
                 || Objects.isNull( interval.maximum() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The declaration included lead_time_pools', which "
                                                                 + "requires the 'lead_times' to be fully declared. "
                                                                 + "Please remove the 'lead_time_pools' or fully "
                                                                 + "declare the 'lead_times' and try again." )
                                               .build();
                events.add( event );
            }
            // The time pools declaration must produce at least one pool
            else
            {
                // Create the elements necessary to increment them
                ChronoUnit periodUnits = pools.unit();
                Duration period = Duration.of( pools.period(), periodUnits );
                Duration start = Duration.of( interval.minimum(), interval.unit() );
                Duration end = Duration.of( interval.maximum(), interval.unit() );

                if ( start.plus( period )
                          .compareTo( end ) > 0 )
                {
                    EvaluationStatusEvent event
                            = EvaluationStatusEvent.newBuilder()
                                                   .setStatusLevel( StatusLevel.ERROR )
                                                   .setEventMessage( "The declaration requested 'lead_time_pools', but "
                                                                     + "none could be produced because the "
                                                                     + "'minimum' time associated with the "
                                                                     + "'lead_times' plus the 'period' associated with "
                                                                     + " the 'lead_time-pools' is later than the "
                                                                     + "'maximum' time associated with the "
                                                                     + "'lead_times'. Please adjust the "
                                                                     + "'lead_time_pools' and/or the 'lead_times' and "
                                                                     + "try again. " )
                                                   .build();
                    events.add( event );
                }
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the date interval is valid.
     * @param timeInterval the time interval
     * @param orientation the orientation
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> timeIntervalIsValid( TimeInterval timeInterval, String orientation )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( timeInterval )
             && Objects.nonNull( timeInterval.minimum() )
             && Objects.nonNull( timeInterval.maximum() )
             && ( timeInterval.maximum()
                              .isBefore( timeInterval.minimum() )
                  || timeInterval.minimum()
                                 .equals( timeInterval.maximum() ) ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The "
                                                             + orientation
                                                             + " interval is invalid because the 'minimum' value is "
                                                             + "greater than or equal to the 'maximum' value. Please "
                                                             + "adjust the 'minimum' to occur before the 'maximum' and "
                                                             + "try " + AGAIN )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the analysis durations are valid.
     * @param analysisDurations the analysis durations
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> analysisDurationsAreValid( AnalysisDurations analysisDurations )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( analysisDurations )
             && Objects.nonNull( analysisDurations.minimumExclusive() )
             && Objects.nonNull( analysisDurations.maximum() )
             && ( analysisDurations.maximum() < analysisDurations.minimumExclusive() ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The 'analysis_durations' interval is invalid because the "
                                                             + "'maximum' value is less than the 'minimum_exclusive' "
                                                             + "value. Please adjust the analysis durations to form a "
                                                             + "valid time interval and try "
                                                             + AGAIN )
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
        if ( Objects.nonNull( sources ) && Objects.nonNull( type ) )
        {
            // Check whether the data type is contained in the data types supported by each source interface. If not,
            // then further check whether the data type is forecast-like and, in that case, add an error event
            // because forecasts of different types are unlike each other and unlike non-forecasts. If the data type is
            // a non-forecast, check whether any of the supported interface data types are non-forecasts. If not, add an
            // error because non-forecasts are unlike forecasts, otherwise add a warning because non-forecasts are
            // broadly similar, even if the exact type differs
            List<SourceInterface> interfaces = sources.stream()
                                                      .map( Source::sourceInterface )
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
        return Objects.nonNull( sources )
               && sources.stream()
                         .map( Source::sourceInterface )
                         .anyMatch( apis::contains );
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