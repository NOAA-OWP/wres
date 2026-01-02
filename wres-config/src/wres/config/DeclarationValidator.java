package wres.config;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.StringJoiner;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import tools.jackson.databind.JsonNode;
import com.networknt.schema.Schema;
import com.networknt.schema.Error;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tika.mime.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.components.AnalysisTimes;
import wres.config.components.BaselineDataset;
import wres.config.components.CovariateDataset;
import wres.config.components.CovariatePurpose;
import wres.config.components.CrossPairMethod;
import wres.config.components.CrossPairScope;
import wres.config.components.DataType;
import wres.config.components.Dataset;
import wres.config.components.DatasetOrientation;
import wres.config.components.EvaluationDeclaration;
import wres.config.components.EventDetection;
import wres.config.components.EventDetectionCombination;
import wres.config.components.EventDetectionDataset;
import wres.config.components.EventDetectionParameters;
import wres.config.components.FeatureAuthority;
import wres.config.components.Formats;
import wres.config.components.GeneratedBaseline;
import wres.config.components.GeneratedBaselines;
import wres.config.components.LeadTimeInterval;
import wres.config.components.MetricParameters;
import wres.config.components.SamplingUncertainty;
import wres.config.components.Season;
import wres.config.components.Source;
import wres.config.components.SourceInterface;
import wres.config.components.Threshold;
import wres.config.components.ThresholdSource;
import wres.config.components.ThresholdType;
import wres.config.components.TimeInterval;
import wres.config.components.TimePools;
import wres.config.components.TimeScale;
import wres.config.components.TimeScaleLenience;
import wres.config.components.UnitAlias;
import wres.config.components.Metric;
import wres.statistics.generated.EvaluationStatus;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent.StatusLevel;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.SummaryStatistic;
import wres.statistics.generated.TimeScale.TimeScaleFunction;
import wres.statistics.generated.GeometryTuple;

/**
 * <p>Validates a declared evaluation. In general, there are three levels of validation, namely:
 *
 * <ol>
 * <li>1. Validation that the declaration string is a valid string in the expected serialization format (YAML), which
 *        is performed by {@link DeclarationFactory#from(String)}.</li>
 * <li>2. Validation that the declaration is compatible with the declaration schema, which is performed here using
 *        {@link DeclarationValidator#validate(JsonNode, Schema)}; and</li>
 * <li>3. Validation that the declaration is internally consistent and reasonable (i.e., "business logic"), which is
 *        also performed here using {@link DeclarationValidator#validate(EvaluationDeclaration, boolean, boolean)}.</li>
 * </ol>
 *
 * <p>In summary, this class performs two levels of validation, namely validation against the schema and validation of
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
    private static final String COVARIATE = "covariate";
    /** Re-used string. */
    private static final String VALID_DATES = "'valid_dates'";
    /** Re-used string. */
    private static final String REFERENCE_DATES = "'reference_dates'";
    /** Re-used string. */
    private static final String AGAIN = "again.";
    /** Re-used string. */
    private static final String THE_TIME_SCALE_ASSOCIATED_WITH = "The time scale associated with ";
    /** Re-used string. */
    private static final String THE_EVALUATION_DECLARED = "The evaluation declared ";
    /** Re-used string. */
    private static final String WHEN_INSPECTING_THE_INTERFACES_ASSOCIATED_WITH_THE =
            "When inspecting the interfaces associated with the '";
    /** Re-used string. */
    private static final String DATA_DISCOVERED_AN_INTERFACE_OF = "' data, discovered an interface of '";
    /** Re-used string. */
    private static final String WHICH_ADMITS_THE_DATA_TYPES = "', which admits the data types ";
    /** Re-used string. */
    private static final String THE_EVALUATION_REQUESTED_THE_SAMPLING_UNCERTAINTY =
            "The evaluation requested the 'sampling_uncertainty' ";
    /** Re-used string. */
    private static final String TRY_AGAIN = "try again.";
    /** Re-used string. */
    private static final String AND_TRY_AGAIN = "and " + TRY_AGAIN;
    /** Re-used string. */
    private static final String DISCOVERED_ONE_OR_MORE = "Discovered one or more '";

    /**
     * Performs validation against the schema, followed by "business-logic" validation if there are no schema
     * validation errors. First, reads the declaration string, then calls {@link #validate(JsonNode, Schema)},
     * then finally calls {@link #validate(EvaluationDeclaration, boolean)}. This method is intended for a caller that
     * wants to validate the declaration without performing any subsequent activities, such as executing an evaluation.
     * Optionally, omit the validation of data sources, which may be necessary when building a declaration from posted
     * data.
     *
     * @param yaml a declaration string
     * @param omitSources is true to omit validation of data sources
     * @return an ordered list of validation events encountered
     * @throws NullPointerException if the input string is null
     * @throws IOException if the schema could not be read
     */

    public static List<EvaluationStatusEvent> validate( String yaml,
                                                        boolean omitSources ) throws IOException
    {
        Objects.requireNonNull( yaml );

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "Encountered the following declaration string:{}{}{}{}",
                         System.lineSeparator(),
                         "---",
                         System.lineSeparator(),
                         yaml.trim() );
        }

        List<EvaluationStatusEvent> oldString = DeclarationValidator.validateAgainstLegacyXmlDeclarationString( yaml );

        if ( !oldString.isEmpty() )
        {
            LOGGER.debug( "Encountered an old declaration string." );
            return oldString;
        }

        // Get the declaration node
        JsonNode declaration;

        try
        {
            declaration = DeclarationFactory.deserialize( yaml );
        }
        catch ( IOException e )
        {
            EvaluationStatusEvent error =
                    EvaluationStatusEvent.newBuilder()
                                         .setStatusLevel( StatusLevel.ERROR )
                                         .setEventMessage( "Encountered invalid YAML: please fix the YAML and try "
                                                           + "again. The cause of the error was: \n"
                                                           + e.getCause()
                                                              .getMessage() )
                                         .build();

            return List.of( error );
        }

        // Get the schema
        Schema schema = DeclarationFactory.getSchema();

        // Validate against the schema
        Set<EvaluationStatusEvent> schemaEvents = DeclarationValidator.validate( declaration, schema );
        List<EvaluationStatusEvent> events = new ArrayList<>( schemaEvents );

        // No schema validation errors? Then proceed to business logic, which requires deserialization and hence no
        // schema validation errors: see #57969
        if ( schemaEvents.isEmpty() )
        {
            try
            {
                EvaluationDeclaration deserialized = DeclarationFactory.deserialize( declaration );

                // Validate against business logic
                List<EvaluationStatusEvent> businessEvents = DeclarationValidator.validate( deserialized, omitSources );
                events.addAll( businessEvents );
            }
            // Catch exceptions here that can occur during deserialization
            catch ( DeclarationException e )
            {
                EvaluationStatusEvent error =
                        EvaluationStatusEvent.newBuilder()
                                             .setStatusLevel( StatusLevel.ERROR )
                                             .setEventMessage( "Failed to deserialize and fully validate the "
                                                               + "declaration because it contains errors: "
                                                               + "please fix the declaration and try "
                                                               + "again. The cause of the error was: \n"
                                                               + e.getCause()
                                                                  .getMessage() )
                                             .build();

                return List.of( error );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Performs a comprehensive validation of the supplied declaration, including all data sources. This is equivalent
     * to {@link #validate(String, boolean)} with {@code omitSources} set to {@code false}.
     *
     * @see #validate(String, boolean)
     * @param yaml a declaration string
     * @return an ordered list of validation events encountered
     * @throws NullPointerException if the input string is null
     * @throws IOException if the schema could not be read
     */

    public static List<EvaluationStatusEvent> validate( String yaml ) throws IOException
    {
        return DeclarationValidator.validate( yaml, false );
    }

    /**
     * Validates against the legacy XML declaration string, which was removed in v7.0, and returns an error if the
     * string is old. See GitHub #487.
     * @param test the test string
     * @return an error if the string is unsupported, else an empty list
     * @throws IOException if the string format cannot be detected
     * @deprecated
     */

    @Deprecated( forRemoval = true, since = "v7.0" )
    public static List<EvaluationStatusEvent> validateAgainstLegacyXmlDeclarationString( String test )
            throws IOException
    {
        // Is this old-style XML declaration? If so, return an error as this cannot be validated upfront.
        MediaType detectedMediaType = DeclarationUtilities.getMediaType( test );

        if ( DeclarationUtilities.isLegacyXmlDeclarationString( detectedMediaType, test ) )
        {
            EvaluationStatusEvent error =
                    EvaluationStatusEvent.newBuilder()
                                         .setStatusLevel( StatusLevel.ERROR )
                                         .setEventMessage( "Encountered an XML declaration string. The XML "
                                                           + "declaration language has been removed and support "
                                                           + "for this language has now ended. Please restate your "
                                                           + "evaluation with the new (YAML) declaration language and "
                                                           + TRY_AGAIN )
                                         .build();

            return List.of( error );
        }

        return List.of();
    }

    /**
     * Performs validation of a declaration node against the schema.
     *
     * @param declaration the declaration
     * @param schema the schema
     * @return the unique schema validation errors encountered
     * @throws NullPointerException if either input is null
     */

    public static Set<EvaluationStatusEvent> validate( JsonNode declaration, Schema schema )
    {
        Objects.requireNonNull( declaration );
        Objects.requireNonNull( schema );

        List<Error> errors = schema.validate( declaration );

        LOGGER.debug( "Validated a declaration string against the schema, which produced {} errors.",
                      errors.size() );

        // Map the errors to evaluation status events
        List<EvaluationStatusEvent> events =
                errors.stream()
                      .map( next -> EvaluationStatusEvent.newBuilder()
                                                         .setStatusLevel( EvaluationStatusEvent.StatusLevel.ERROR )
                                                         .setEventMessage( next.toString() )
                                                         .build() )
                      .toList();

        // Identify unique errors and sort them by message
        Comparator<EvaluationStatusEvent> comparator = Comparator.comparing( EvaluationStatusEvent::getEventMessage );
        SortedSet<EvaluationStatusEvent> sorted = new TreeSet<>( comparator );
        sorted.addAll( events );

        return Collections.unmodifiableSortedSet( sorted );
    }

    /**
     * Performs business-logic validation and, optionally, notifies any events discovered by logging warnings and
     * aggregating errors into an exception. For raw business-logic validation, see
     * {@link #validate(EvaluationDeclaration, boolean)}.
     *
     * @see #validate(EvaluationDeclaration, boolean)
     * @param declaration the declaration to validate
     * @param omitSources is true to omit validation of data sources
     * @param notify is true to notify of any events encountered, false to remain silent
     * @throws DeclarationException if validation errors were encountered
     * @return the valid declaration
     */
    public static List<EvaluationStatusEvent> validate( EvaluationDeclaration declaration,
                                                        boolean omitSources,
                                                        boolean notify )
    {
        List<EvaluationStatus.EvaluationStatusEvent> events = DeclarationValidator.validate( declaration, omitSources );

        if ( !notify )
        {
            LOGGER.debug( "Encountered {} validation events, which will not be notified.", events.size() );
            return events;
        }

        // Notify
        DeclarationValidator.notify( events );

        return events;
    }

    /**
     * Validates the declaration. The validation events are returned in the order they were discovered, reading from
     * the top of the declaration to the bottom. Delegates to the caller to notify about any validation events
     * encountered. For default notification handling, see {@link #validate(EvaluationDeclaration, boolean, boolean)}.
     *
     * @see #validate(EvaluationDeclaration, boolean, boolean)
     * @param declaration the declaration
     * @return the validation events in the order they were discovered
     * @throws NullPointerException if the declaration is null
     */
    public static List<EvaluationStatusEvent> validate( EvaluationDeclaration declaration )
    {
        return DeclarationValidator.validate( declaration, false );
    }

    /**
     * Validates the declaration. The validation events are returned in the order they were discovered, reading from
     * the top of the declaration to the bottom. Delegates to the caller to notify about any validation events
     * encountered. For default notification handling, see {@link #validate(EvaluationDeclaration, boolean, boolean)}.
     *
     * @see #validate(EvaluationDeclaration, boolean, boolean)
     * @param declaration the declaration
     * @param omitSources is true to omit validation of data sources
     * @return the validation events in the order they were discovered
     * @throws NullPointerException if the declaration is null
     */
    public static List<EvaluationStatusEvent> validate( EvaluationDeclaration declaration, boolean omitSources )
    {
        Objects.requireNonNull( declaration );

        // Check that the datasets are valid
        List<EvaluationStatusEvent> datasets = DeclarationValidator.validateDatasets( declaration, omitSources );
        List<EvaluationStatusEvent> events = new ArrayList<>( datasets );
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
        // Check that the event detection is valid
        List<EvaluationStatusEvent> eventDetection = DeclarationValidator.eventDetectionIsValid( declaration );
        events.addAll( eventDetection );
        // Check that the feature declaration is valid
        List<EvaluationStatusEvent> features = DeclarationValidator.featuresAreValid( declaration );
        events.addAll( features );
        // Check that the metrics declaration is valid
        List<EvaluationStatusEvent> metrics = DeclarationValidator.metricsAreValid( declaration );
        events.addAll( metrics );
        // Check that any summary statistics are valid
        List<EvaluationStatusEvent> summaryStatistics = DeclarationValidator.summaryStatisticsAreValid( declaration );
        events.addAll( summaryStatistics );
        // Check that any thresholds are valid
        List<EvaluationStatusEvent> thresholds = DeclarationValidator.thresholdsAreValid( declaration );
        events.addAll( thresholds );
        // Check that the sampling uncertainty declaration is valid
        List<EvaluationStatusEvent> samplingUncertainty =
                DeclarationValidator.samplingUncertaintyIsValid( declaration );
        events.addAll( samplingUncertainty );
        // Check that the output formats declaration is valid
        List<EvaluationStatusEvent> outputs = DeclarationValidator.outputFormatsAreValid( declaration );
        events.addAll( outputs );

        if ( LOGGER.isDebugEnabled() )
        {
            long warnCount = events.stream()
                                   .filter( next -> next.getStatusLevel() == StatusLevel.WARN )
                                   .count();

            long errorCount = events.stream()
                                    .filter( next -> next.getStatusLevel() == StatusLevel.ERROR )
                                    .count();

            LOGGER.debug( "Encountered {} validation messages, including {} warnings and {} errors.",
                          events.size(),
                          warnCount,
                          errorCount );
        }
        return Collections.unmodifiableList( events );
    }

    /**
     * Validates the declaration for any information that has been clarified by reading and ingesting data sources, such
     * as the data types and variables to evaluate. Performs default notification handling for any events encountered.
     *
     * @see #validate(EvaluationDeclaration, boolean)
     * @param declaration the declaration
     * @throws NullPointerException if the declaration is null
     */
    public static void validatePostDataIngest( EvaluationDeclaration declaration )
    {
        // Data types are defined for all datasets
        List<EvaluationStatusEvent> typesDefined = DeclarationValidator.typesAreDefined( declaration );
        List<EvaluationStatusEvent> events = new ArrayList<>( typesDefined );

        // Data types are consistent with other declaration
        List<EvaluationStatusEvent> typesConsistent = DeclarationValidator.typesAreValid( declaration );
        events.addAll( typesConsistent );
        // Covariates must have unique variable names
        List<EvaluationStatusEvent> covariateVariables
                = DeclarationValidator.covariateVariableNamesAreUnique( declaration );
        events.addAll( covariateVariables );
        // Ensembles cannot be present on both left and right sides
        List<EvaluationStatusEvent> ensembles = DeclarationValidator.ensembleOnOneSideOnly( declaration );
        events.addAll( ensembles );

        // Event detection does not use forecast data
        List<EvaluationStatusEvent> eventDetection =
                DeclarationValidator.eventDetectionDoesNotUseForecastData( declaration );
        events.addAll( eventDetection );

        // No need to check any sources because this is a post-ingest validation

        // Check that the metrics declaration is valid
        List<EvaluationStatusEvent> metrics = DeclarationValidator.metricsAreValid( declaration );
        events.addAll( metrics );

        List<EvaluationStatusEvent> finalEvents = Collections.unmodifiableList( events );

        // Notify any events encountered
        DeclarationValidator.notify( finalEvents );
    }

    /**
     * Performs default notification of validation events, logging warnings and aggregating errors into an exception.
     * @param events the validation events
     * @throws DeclarationException if validation errors are encountered
     */

    private static void notify( List<EvaluationStatusEvent> events )
    {
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

                LOGGER.warn( "Encountered {} warning(s) when validating the declared evaluation: {}{}",
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
                                            + " error(s) in the declared evaluation, which must be fixed:"
                                            + System.lineSeparator() +
                                            message );
        }
    }

    /**
     * Validates the dataset declaration.
     * @param declaration the declaration
     * @param omitSources is true to omit validation of data sources
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> validateDatasets( EvaluationDeclaration declaration,
                                                                 boolean omitSources )
    {
        // Required datasets are present
        List<EvaluationStatusEvent> events = new ArrayList<>();
        if ( !omitSources )
        {
            List<EvaluationStatusEvent> datasetsPresent =
                    DeclarationValidator.requiredDatasetsArePresent( declaration );
            events.addAll( datasetsPresent );

            // Check that the sources are valid
            List<EvaluationStatusEvent> sources = DeclarationValidator.sourcesAreValid( declaration );
            events.addAll( sources );
        }

        // Data types are valid
        List<EvaluationStatusEvent> typesValid = DeclarationValidator.typesAreValid( declaration );
        events.addAll( typesValid );
        // Ensembles cannot be present on both left and right sides
        List<EvaluationStatusEvent> ensembles = DeclarationValidator.ensembleOnOneSideOnly( declaration );
        events.addAll( ensembles );
        // Generated baseline is consistent with other declaration
        List<EvaluationStatusEvent> baseline = DeclarationValidator.generatedBaselineIsValid( declaration );
        events.addAll( baseline );
        // Variable must be declared in some circumstances
        List<EvaluationStatusEvent> variables = DeclarationValidator.variablesDeclaredIfRequired( declaration );
        events.addAll( variables );
        // Covariates must have unique variable names
        List<EvaluationStatusEvent> covariateVariables
                = DeclarationValidator.covariateVariableNamesAreUnique( declaration );
        events.addAll( covariateVariables );
        // Covariate filters must be logical
        List<EvaluationStatusEvent> covariateFilters = DeclarationValidator.covariateFiltersAreValid( declaration );
        events.addAll( covariateFilters );
        // When obtaining data from web services, dates must be defined
        List<EvaluationStatusEvent> services = DeclarationValidator.declarationValidForWebServices( declaration );
        events.addAll( services );
        // Check that the time scales are valid
        List<EvaluationStatusEvent> timeScales = DeclarationValidator.timeScalesAreValid( declaration );
        events.addAll( timeScales );
        // Check that any time-zone offsets are consistent
        List<EvaluationStatusEvent> timeZoneOffsets = DeclarationValidator.timeZoneOffsetsAreValid( declaration );
        events.addAll( timeZoneOffsets );
        // Check that any measurement units are consistent
        List<EvaluationStatusEvent> units = DeclarationValidator.unitsAreValid( declaration );
        events.addAll( units );

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that each of the required datasets is present.
     *
     * @param declaration the declaration
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> requiredDatasetsArePresent( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.isNull( declaration.left() ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The declaration does not contain an 'observed' dataset, "
                                                             + "which is required. Please add an 'observed' dataset "
                                                             + AND_TRY_AGAIN )
                                           .build();
            events.add( event );
        }

        if ( Objects.isNull( declaration.right() ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The declaration does not contain a 'predicted' dataset, "
                                                             + "which is required. Please add a 'predicted' dataset "
                                                             + AND_TRY_AGAIN )
                                           .build();
            events.add( event );
        }

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
            EvaluationStatusEvent event = eventBuilder.setEventMessage( message )
                                                      .build();
            events.add( event );
        }
        if ( Objects.isNull( declaration.right()
                                        .type() ) )
        {
            String message = start + PREDICTED + middle + PREDICTED + end;
            EvaluationStatusEvent event = eventBuilder.setEventMessage( message )
                                                      .build();
            events.add( event );
        }
        if ( DeclarationUtilities.hasBaseline( declaration )
             && Objects.isNull( declaration.baseline()
                                           .dataset()
                                           .type() ) )
        {
            String message = start + BASELINE + middle + BASELINE + end;
            EvaluationStatusEvent event = eventBuilder.setEventMessage( message )
                                                      .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the data types are valid.
     * @param declaration the declaration
     * @return any status events encountered
     */
    private static List<EvaluationStatusEvent> typesAreValid( EvaluationDeclaration declaration )
    {
        // Data types are consistent with other declaration
        List<EvaluationStatusEvent> typesConsistent = DeclarationValidator.typesAreConsistent( declaration );
        List<EvaluationStatusEvent> events = new ArrayList<>( typesConsistent );

        // Data types for covariates are not forecast-like
        List<EvaluationStatusEvent> covariateTypes = DeclarationValidator.covariateTypesAreValid( declaration );
        events.addAll( covariateTypes );

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
            Set<String> ensembleDeclaration = DeclarationUtilities.getEnsembleDeclaration( declaration );
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
            Set<String> forecastDeclaration = DeclarationUtilities.getForecastDeclaration( declaration );
            if ( !forecastDeclaration.isEmpty() )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The declaration does not contain any datasets with a "
                                                                 + "data type of "
                                                                 + DataType.ENSEMBLE_FORECASTS
                                                                 + " or "
                                                                 + DataType.SINGLE_VALUED_FORECASTS
                                                                 + ", but some of the declaration is designed for these "
                                                                 + "data types: "
                                                                 + forecastDeclaration
                                                                 + ". Please remove this declaration or correct the "
                                                                 + "data types." )
                                               .build();
                events.add( event );
            }
        }

        // If there are no analyses datasets present, there cannot be declaration for analyses
        if ( DeclarationValidator.doesNotHaveThisDataType( DataType.ANALYSES, declaration )
             && DeclarationUtilities.hasAnalysisTimes( declaration ) )
        {
            EvaluationStatusEvent event = EvaluationStatusEvent.newBuilder()
                                                               .setStatusLevel( StatusLevel.ERROR )
                                                               .setEventMessage(
                                                                       "The declaration does not contain any "
                                                                       + "datasets with a data type of "
                                                                       + DataType.ANALYSES
                                                                       + ", but some of the declaration is "
                                                                       + "designed for this data type: "
                                                                       + "analysis_times. Please remove this "
                                                                       + "declaration or correct the data "
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

        // Generated baseline declared, but the type is a forecast type
        if ( DeclarationUtilities.hasGeneratedBaseline( declaration.baseline() )
             && Objects.nonNull( declaration.baseline()
                                            .dataset()
                                            .type() )
             && declaration.baseline()
                           .dataset()
                           .type()
                           .isForecastType() )
        {
            EvaluationStatusEvent event =
                    EvaluationStatusEvent.newBuilder()
                                         .setStatusLevel( StatusLevel.ERROR )
                                         .setEventMessage( "The declaration contains a 'baseline' with a 'method' of '"
                                                           + declaration.baseline()
                                                                        .generatedBaseline()
                                                                        .method()
                                                           + "', which requires observation-like data sources, but the "
                                                           + "data 'type' is '"
                                                           + declaration.baseline()
                                                                        .dataset()
                                                                        .type()
                                                           + "'. Please change the data 'type' and try again." )
                                         .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the data types are valid for covariates.
     * @param declaration the declaration
     * @return any status events encountered
     */
    private static List<EvaluationStatusEvent> covariateTypesAreValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        Set<DataType> covariateTypes = declaration.covariates()
                                                  .stream()
                                                  .map( c -> c.dataset().type() )
                                                  .collect( Collectors.toSet() );

        // Different observation-like types are allowed

        if ( covariateTypes.contains( DataType.ENSEMBLE_FORECASTS )
             || covariateTypes.contains( DataType.SINGLE_VALUED_FORECASTS ) )
        {
            EvaluationStatusEvent event =
                    EvaluationStatusEvent.newBuilder()
                                         .setStatusLevel( StatusLevel.ERROR )
                                         .setEventMessage( "Discovered a forecast data 'type' for one or more "
                                                           + "'covariate' datasets, which is not allowed. The "
                                                           + "'covariate' datasets must all be observation-like." )
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
        Set<Threshold> thresholds = DeclarationUtilities.getInbandThresholds( declaration );

        return declaration.thresholdSources()
                          .stream()
                          .anyMatch( n -> n.type() == thresholdType )
               || thresholds.stream()
                            .anyMatch( n -> n.type() == thresholdType );
    }

    /**
     * Checks that ensemble forecasts are not declared on both left and right sides.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> ensembleOnOneSideOnly( EvaluationDeclaration declaration )
    {
        if ( Objects.nonNull( declaration.right() )
             && declaration.right()
                           .type() == DataType.ENSEMBLE_FORECASTS
             && Objects.nonNull( declaration.left() )
             && declaration.left()
                           .type() == DataType.ENSEMBLE_FORECASTS )
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
     * Checks that a generated baseline is valid
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> generatedBaselineIsValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();
        if ( DeclarationUtilities.hasGeneratedBaseline( declaration.baseline() ) )
        {
            BaselineDataset baseline = declaration.baseline();
            GeneratedBaseline generatedBaseline = baseline.generatedBaseline();

            Set<String> ensembleDeclaration = DeclarationUtilities.getEnsembleDeclaration( declaration );

            // Persistence not allowed for ensemble-like evaluation
            if ( generatedBaseline.method() == GeneratedBaselines.PERSISTENCE
                 && !ensembleDeclaration.isEmpty() )
            {
                EvaluationStatusEvent event =
                        EvaluationStatusEvent.newBuilder()
                                             .setStatusLevel( StatusLevel.ERROR )
                                             .setEventMessage( "Cannot declare a 'persistence' baseline for an "
                                                               + "evaluation that contains ensemble forecasts. Please "
                                                               + "remove the 'persistence' baseline or remove the "
                                                               + "ensemble declaration and try again. The following "
                                                               + "ensemble declaration was "
                                                               + "discovered: "
                                                               + ensembleDeclaration )
                                             .build();

                events.add( event );
            }
            // Climatology is valid
            else if ( generatedBaseline.method() == GeneratedBaselines.CLIMATOLOGY )
            {
                List<EvaluationStatusEvent> climatology =
                        DeclarationValidator.generatedClimatologyIsValid( declaration );
                events.addAll( climatology );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that a generated climatological baseline is valid
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> generatedClimatologyIsValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        BaselineDataset baseline = declaration.baseline();
        if ( DeclarationValidator.hasNonDefaultDates( baseline.generatedBaseline() ) )
        {
            Instant minimum = declaration.baseline()
                                         .generatedBaseline()
                                         .minimumDate();
            Instant maximum = declaration.baseline()
                                         .generatedBaseline()
                                         .maximumDate();

            // Maximum is not after minimum
            if ( !maximum.isAfter( minimum ) )
            {
                EvaluationStatusEvent event =
                        EvaluationStatusEvent.newBuilder()
                                             .setStatusLevel( StatusLevel.ERROR )
                                             .setEventMessage( ( "Discovered a climatological baseline whose "
                                                                 + "'maximum_date' of "
                                                                 + maximum
                                                                 + " is not later than the "
                                                                 + "'minimum_date' of "
                                                                 + minimum
                                                                 + ", which is not allowed. Please adjust one "
                                                                 + "or both of these dates to form a valid "
                                                                 + "interval and try again." ) )
                                             .build();
                events.add( event );
            }
            // Period is at least one year
            else if ( Duration.between( minimum, maximum )
                              .compareTo( Duration.ofDays( 365 ) ) <= 0 )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "Discovered a climatological baseline whose "
                                                                 + "'minimum_date' of "
                                                                 + minimum
                                                                 + " and 'maximum_date' of "
                                                                 + maximum
                                                                 + " span an interval of less than 365 days. "
                                                                 + "This is not allowed because climatology is "
                                                                 + "generated by resampling the same valid "
                                                                 + "times in one or more historical years of "
                                                                 + "record, which requires at least one year "
                                                                 + "of data. Please adjust the 'minimum_date' "
                                                                 + "and/or the 'maximum_date' so that the "
                                                                 + "interval between them spans at least 365 "
                                                                 + "days and try again." )
                                               .build();
                events.add( event );
            }
        }
        // Data service and valid dates span less than 365 days
        else if ( baseline.dataset()
                          .sources()
                          .stream()
                          .anyMatch( n -> Objects.nonNull( n.uri() )
                                          && DeclarationValidator.isWebSource( n ) )
                  // Valid dates are present and span less than 365 days
                  && Objects.nonNull( declaration.validDates() )
                  && Objects.nonNull( declaration.validDates()
                                                 .minimum() )
                  && Objects.nonNull( declaration.validDates()
                                                 .maximum() )
                  && Duration.between( declaration.validDates()
                                                  .minimum(), declaration.validDates()
                                                                         .maximum() )
                             .compareTo( Duration.ofDays( 365 ) ) <= 0 )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Discovered an evaluation with a climatological "
                                                             + "baseline whose source data originates from a "
                                                             + "web service and whose 'valid_dates' span an "
                                                             + "interval of less than 365 days. This is not "
                                                             + "allowed because climatology is generated by "
                                                             + "resampling the same valid times in one or more "
                                                             + "historical years of record, which requires at "
                                                             + "least one year of data. Please adjust the "
                                                             + "'minimum' and/or the 'maximum' associated with "
                                                             + "the 'valid_dates' so that the interval between "
                                                             + "them spans at least 365 days and try again." )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * @param baseline the generated baseline to test
     * @return whether the date constraints on the baseline are non-default
     */

    private static boolean hasNonDefaultDates( GeneratedBaseline baseline )
    {
        Instant minimum = baseline.minimumDate();
        Instant maximum = baseline.maximumDate();

        return Objects.nonNull( minimum )
               && Objects.nonNull( maximum )
               && ( minimum != Instant.MIN
                    || maximum != Instant.MAX );
    }

    /**
     * Checks that all variables are declared when required.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> variablesDeclaredIfRequired( EvaluationDeclaration declaration )
    {
        // Check for source interfaces that require a variable
        List<EvaluationStatusEvent> observed =
                DeclarationValidator.variablesDeclaredIfRequired( declaration.left(),
                                                                  DatasetOrientation.LEFT );

        List<EvaluationStatusEvent> events = new ArrayList<>( observed );

        List<EvaluationStatusEvent> predicted =
                DeclarationValidator.variablesDeclaredIfRequired( declaration.right(),
                                                                  DatasetOrientation.RIGHT );

        events.addAll( predicted );

        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            List<EvaluationStatusEvent> baseline =
                    DeclarationValidator.variablesDeclaredIfRequired( declaration.baseline()
                                                                                 .dataset(),
                                                                      DatasetOrientation.BASELINE );

            events.addAll( baseline );
        }

        for ( CovariateDataset covariate : declaration.covariates() )
        {
            List<EvaluationStatusEvent> covariateEvents =
                    DeclarationValidator.variablesDeclaredIfRequired( covariate.dataset(),
                                                                      DatasetOrientation.COVARIATE );

            events.addAll( covariateEvents );
        }

        // Variable names are additionally required for every covariate when there are two or more as this helps to
        // disambiguate the time-series to which the covariate conditions apply, post-ingest
        if ( declaration.covariates()
                        .size() > 1
             && declaration.covariates()
                           .stream()
                           .anyMatch( c -> Objects.isNull( c.dataset()
                                                            .variable() )
                                           || Objects.isNull( c.dataset()
                                                               .variable()
                                                               .name() ) ) )
        {
            EvaluationStatusEvent error =
                    EvaluationStatusEvent.newBuilder()
                                         .setStatusLevel( StatusLevel.ERROR )
                                         .setEventMessage(
                                                 "When declaring two or more 'covariates', the 'name' of each "
                                                 + "'variable' must be declared explicitly, but one or more "
                                                 + "of the 'covariates' had no declared 'variable' and "
                                                 + "'name'. Please clarify the 'name' of the 'variable' for "
                                                 + "each covariate and try again." )
                                         .build();
            events.add( error );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that all variables are declared when required.
     * @param dataset the dataset
     * @param orientation the dataset orientation
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> variablesDeclaredIfRequired( Dataset dataset,
                                                                            DatasetOrientation orientation )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Check for source interfaces that require a variable
        EvaluationStatusEvent.Builder eventBuilder = EvaluationStatusEvent.newBuilder()
                                                                          .setStatusLevel( StatusLevel.ERROR );

        String article = "the";
        if ( orientation == DatasetOrientation.COVARIATE )
        {
            article = "a";
        }

        String messageStart = "Discovered a data source for "
                              + article
                              + " '"
                              + orientation.toString()
                                           .toLowerCase()
                              + "' dataset with an interface shorthand of ";
        String messageEnd = ", which requires the 'variable' to be declared. Please declare the 'variable' and try "
                            + AGAIN;

        // Check the dataset
        if ( DeclarationValidator.variableIsNotDeclared( dataset ) )
        {
            if ( DeclarationValidator.hasSourceInterface( dataset.sources(), SourceInterface.USGS_NWIS ) )
            {
                String message = messageStart + SourceInterface.USGS_NWIS + messageEnd;
                EvaluationStatusEvent event = eventBuilder.setEventMessage( message )
                                                          .build();
                events.add( event );
            }

            if ( DeclarationValidator.hasSourceInterface( dataset.sources(), SourceInterface.WRDS_NWM ) )
            {
                String message = messageStart + SourceInterface.WRDS_NWM + messageEnd;
                EvaluationStatusEvent event = eventBuilder.setEventMessage( message )
                                                          .build();
                events.add( event );
            }

            if ( DeclarationValidator.hasSourceInterface( dataset.sources(), SourceInterface.WRDS_HEFS ) )
            {
                String message = messageStart + SourceInterface.WRDS_HEFS + messageEnd;
                EvaluationStatusEvent event = eventBuilder.setEventMessage( message )
                                                          .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that all covariates have unique variable names.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> covariateVariableNamesAreUnique( EvaluationDeclaration declaration )
    {
        if ( declaration.covariates()
                        .isEmpty() )
        {
            LOGGER.debug( "Not checking for unique variable names of the covariates because no covariates were "
                          + "declared." );

            return List.of();
        }

        // Determine the duplicate names, i.e., those that occur more than once
        Set<String> duplicates = declaration.covariates()
                                            .stream()
                                            .filter( c -> Objects.nonNull( c.dataset()
                                                                            .variable() ) )
                                            .map( c -> c.dataset()
                                                        .variable()
                                                        .name() )
                                            .collect( Collectors.groupingBy( Function.identity(),
                                                                             Collectors.counting() ) )
                                            .entrySet()
                                            .stream()
                                            .filter( e -> e.getValue() > 1 )
                                            .map( Map.Entry::getKey )
                                            .collect( Collectors.toSet() );

        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( !duplicates.isEmpty() )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Discovered duplicate variable names among the "
                                                             + "covariates, which is not allowed. Each covariate must "
                                                             + "have a unique variable name. The duplicate names "
                                                             + "were: "
                                                             + duplicates
                                                             + "." )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that all covariates filters are valid.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> covariateFiltersAreValid( EvaluationDeclaration declaration )
    {
        if ( declaration.covariates()
                        .isEmpty() )
        {
            LOGGER.debug( "Not checking covariate filters because no covariates were declared." );

            return List.of();
        }

        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Filter covariates whose minimum > maximum
        Set<CovariateDataset> variables = declaration.covariates()
                                                     .stream()
                                                     .filter( c -> Objects.nonNull( c.minimum() )
                                                                   && Objects.nonNull( c.maximum() )
                                                                   && c.minimum() > c.maximum() )
                                                     .collect( Collectors.toSet() );

        Set<String> named = variables.stream()
                                     .filter( c -> Objects.nonNull( c.dataset() )
                                                   && Objects.nonNull( c.dataset()
                                                                        .variable() )
                                                   && Objects.nonNull( c.dataset()
                                                                        .variable()
                                                                        .name() ) )
                                     .map( c -> c.dataset()
                                                 .variable()
                                                 .name() )
                                     .collect( Collectors.toSet() );

        if ( !named.isEmpty() )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Discovered covariates whose 'minimum' value is larger "
                                                             + "than the 'maximum' value, which is not allowed. Please "
                                                             + "fix the 'minimum' and/or 'maximum' value associated "
                                                             + "with the following covariates and try again: "
                                                             + named )
                                           .build();
            events.add( event );
        }

        if ( variables.size() - named.size() > 0 )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Discovered covariates whose 'minimum' value is larger "
                                                             + "than the 'maximum' value, which is not allowed. These "
                                                             + "covariates were not declared with a variable name to "
                                                             + "distinguish them. The number of unnamed covariates to "
                                                             + "fix is: "
                                                             + ( variables.size() - named.size() )
                                                             + ". Please fix the 'minimum' and/or "
                                                             + "'maximum' value associated with these covariates and "
                                                             + TRY_AGAIN )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the declaration of web services is valid.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> declarationValidForWebServices( EvaluationDeclaration declaration )
    {
        // Check that the dates are declared for web services and that each web-like source has a source interface
        List<EvaluationStatusEvent> events =
                new ArrayList<>( DeclarationValidator.datesPresentForWebServices( declaration,
                                                                                  declaration.left(),
                                                                                  DatasetOrientation.LEFT ) );

        List<EvaluationStatusEvent> leftInterfaceEvents =
                new ArrayList<>( DeclarationValidator.interfacePresentForWebSources( declaration.left(),
                                                                                     DatasetOrientation.LEFT ) );
        events.addAll( leftInterfaceEvents );

        List<EvaluationStatusEvent> rightEvents =
                DeclarationValidator.datesPresentForWebServices( declaration,
                                                                 declaration.right(),
                                                                 DatasetOrientation.RIGHT );
        events.addAll( rightEvents );

        List<EvaluationStatusEvent> rightInterfaceEvents =
                DeclarationValidator.interfacePresentForWebSources( declaration.right(),
                                                                    DatasetOrientation.RIGHT );
        events.addAll( rightInterfaceEvents );

        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            List<EvaluationStatusEvent> baselineEvents =
                    DeclarationValidator.datesPresentForWebServices( declaration,
                                                                     declaration.baseline()
                                                                                .dataset(),
                                                                     DatasetOrientation.BASELINE );
            events.addAll( baselineEvents );

            List<EvaluationStatusEvent> baselineInterfaceEvents =
                    DeclarationValidator.interfacePresentForWebSources( declaration.baseline()
                                                                                   .dataset(),
                                                                        DatasetOrientation.BASELINE );
            events.addAll( baselineInterfaceEvents );
        }

        for ( CovariateDataset covariate : declaration.covariates() )
        {
            List<EvaluationStatusEvent> covariateEvents =
                    DeclarationValidator.datesPresentForWebServices( declaration,
                                                                     covariate.dataset(),
                                                                     DatasetOrientation.COVARIATE );
            events.addAll( covariateEvents );

            List<EvaluationStatusEvent> covariateInterfaceEvents =
                    DeclarationValidator.interfacePresentForWebSources( covariate.dataset(),
                                                                        DatasetOrientation.COVARIATE );
            events.addAll( covariateInterfaceEvents );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that dates are available to constrain requests to web services.
     * @param declaration the declaration
     * @param dataset the dataset
     * @param orientation the orientation
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> datesPresentForWebServices( EvaluationDeclaration declaration,
                                                                           Dataset dataset,
                                                                           DatasetOrientation orientation )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Some web services declared?
        if ( DeclarationValidator.hasWebSources( dataset ) )
        {
            List<EvaluationStatusEvent> sourceEvents =
                    DeclarationValidator.datesPresentForWebServices( declaration,
                                                                     dataset.type(),
                                                                     orientation );
            events.addAll( sourceEvents );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Determines whether the source interface is missing for any web-like sources and warns if so.
     *
     * @param dataset the dataset
     * @param orientation the dataset orientation
     * @return any validation events encountered
     */

    private static List<EvaluationStatusEvent> interfacePresentForWebSources( Dataset dataset,
                                                                              DatasetOrientation orientation )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Check that a source interface is defined for each web-like source, otherwise warn
        if ( Objects.nonNull( dataset )
             && Objects.nonNull( dataset.sources() )
             && dataset.sources()
                       .stream()
                       .anyMatch( s -> DeclarationValidator.isWebSource( s )
                                       && Objects.isNull( s.sourceInterface() ) ) )
        {
            String add = "";
            if ( Objects.nonNull( dataset.variable() )
                 && Objects.nonNull( dataset.variable()
                                            .name() ) )
            {
                add = " for the '"
                      + dataset.variable()
                               .name()
                      + "' variable";
            }

            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "One or more '"
                                                             + orientation
                                                             + "' data sources"
                                                             + add
                                                             + " refers to an HTTP address, but does not declare a "
                                                             + "source 'interface'. This is allowed, but the source "
                                                             + "'interface' will need to be interpolated from the URL "
                                                             + "itself, which may not be reliable. The 'interface' is "
                                                             + "used to establish the rules for communicating with a "
                                                             + "web service, such as how to formulate requests and "
                                                             + "read responses. " )
                                           .build();
            events.add( event );
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
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( declaration.left() ) )
        {
            List<EvaluationStatusEvent> leftEvents = DeclarationValidator.sourcesAreValid( declaration.left()
                                                                                                      .sources(),
                                                                                           declaration.left()
                                                                                                      .type(),
                                                                                           OBSERVED );
            events.addAll( leftEvents );
        }

        if ( Objects.nonNull( declaration.right() ) )
        {
            List<EvaluationStatusEvent> rightEvents =
                    DeclarationValidator.sourcesAreValid( declaration.right()
                                                                     .sources(),
                                                          declaration.right()
                                                                     .type(),
                                                          PREDICTED );
            events.addAll( rightEvents );
        }

        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            List<EvaluationStatusEvent> baselineEvents =
                    DeclarationValidator.sourcesAreValid( declaration.baseline()
                                                                     .dataset()
                                                                     .sources(),
                                                          declaration.baseline()
                                                                     .dataset()
                                                                     .type(),
                                                          BASELINE );
            events.addAll( baselineEvents );
        }

        for ( CovariateDataset nextCovariate : declaration.covariates() )
        {
            List<EvaluationStatusEvent> covariateEvents =
                    DeclarationValidator.sourcesAreValid( nextCovariate.dataset()
                                                                       .sources(),
                                                          nextCovariate.dataset()
                                                                       .type(),
                                                          COVARIATE );
            events.addAll( covariateEvents );
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
        // Left dataset
        List<EvaluationStatusEvent> events =
                new ArrayList<>( DeclarationValidator.datasetTimeScaleConsistentWithDesiredTimeScale( declaration.left(),
                                                                                                      DatasetOrientation.LEFT,
                                                                                                      declaration.timeScale() ) );

        // Right dataset
        List<EvaluationStatusEvent> rightEvents =
                DeclarationValidator.datasetTimeScaleConsistentWithDesiredTimeScale( declaration.right(),
                                                                                     DatasetOrientation.RIGHT,
                                                                                     declaration.timeScale() );

        events.addAll( rightEvents );

        // Baseline sources, if needed
        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            // Source timescale must be consistent with desired/evaluation timescale
            List<EvaluationStatusEvent> baselineEvents =
                    DeclarationValidator.datasetTimeScaleConsistentWithDesiredTimeScale( declaration.baseline()
                                                                                                    .dataset(),
                                                                                         DatasetOrientation.BASELINE,
                                                                                         declaration.timeScale() );

            events.addAll( baselineEvents );
        }

        // Covariates
        for ( CovariateDataset covariate : declaration.covariates() )
        {
            TimeScale desiredScale = declaration.timeScale();

            // Validate with respect to the target timescale function
            if ( Objects.nonNull( covariate.rescaleFunction() )
                 && Objects.nonNull( desiredScale ) )
            {
                wres.statistics.generated.TimeScale innerScale = desiredScale.timeScale()
                                                                             .toBuilder()
                                                                             .setFunction( covariate.rescaleFunction() )
                                                                             .build();
                desiredScale = new TimeScale( innerScale );
            }

            List<EvaluationStatusEvent> covariateEvents =
                    DeclarationValidator.datasetTimeScaleConsistentWithDesiredTimeScale( covariate.dataset(),
                                                                                         DatasetOrientation.COVARIATE,
                                                                                         desiredScale );

            events.addAll( covariateEvents );

            // Can only define a rescale function when there is an evaluation timescale
            List<EvaluationStatusEvent> covariateRescale =
                    DeclarationValidator.covariateRescaleFunctionIsConsistent( covariate, declaration );

            events.addAll( covariateRescale );
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
     * Checks that an evaluation timescale is declared when a covariate dataset includes a rescale function.
     * @param dataset the covariate dataset
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> covariateRescaleFunctionIsConsistent( CovariateDataset dataset,
                                                                                     EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( dataset.rescaleFunction() )
             && Objects.isNull( declaration.timeScale() ) )
        {
            String extra = "";

            if ( Objects.nonNull( dataset.dataset() )
                 && Objects.nonNull( dataset.dataset()
                                            .variable() )
                 && Objects.nonNull( dataset.dataset()
                                            .variable()
                                            .name() ) )
            {
                extra = "for variable '" + dataset.dataset()
                                                  .variable()
                                                  .name() + "' ";
            }

            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Discovered a covariate dataset "
                                                             + extra
                                                             + "with a 'rescale_function' of '"
                                                             + dataset.rescaleFunction()
                                                             + "', but the evaluation 'time_scale' was missing, "
                                                             + "which is not allowed. Please declare an evaluation "
                                                             + "'time_scale' or remove the 'rescale_function' from "
                                                             + "the covariate dataset and try again." )
                                           .build();
            events.add( event );
        }
        else if ( Objects.isNull( dataset.rescaleFunction() )
                  && Objects.nonNull( declaration.timeScale() ) )
        {
            String extra = "";

            if ( Objects.nonNull( dataset.dataset() )
                 && Objects.nonNull( dataset.dataset()
                                            .variable() )
                 && Objects.nonNull( dataset.dataset()
                                            .variable()
                                            .name() ) )
            {
                extra = "for variable '" + dataset.dataset()
                                                  .variable()
                                                  .name() + "' ";
            }

            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "Discovered a covariate dataset "
                                                             + extra
                                                             + "that does not have a 'rescale_function' declared. If "
                                                             + "this dataset needs to be rescaled, it will be rescaled "
                                                             + "to the evaluation 'time_scale', which has a declared "
                                                             + "'function' of '"
                                                             + declaration.timeScale()
                                                                          .timeScale()
                                                                          .getFunction()
                                                             + "'. If this is incorrect, please add the correct "
                                                             + "'rescale_function' for the covariate dataset and "
                                                             + TRY_AGAIN )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that any declared timescale associated with the dataset is consistent with the evaluation timescale.
     * @param dataset the dataset
     * @param orientation the dataset orientation
     * @param desiredTimeScale the evaluation timescale
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> datasetTimeScaleConsistentWithDesiredTimeScale( Dataset dataset,
                                                                                               DatasetOrientation orientation,
                                                                                               TimeScale desiredTimeScale )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( dataset ) )
        {
            if ( Objects.nonNull( dataset.timeScale() ) )
            {
                TimeScale timeScale = dataset.timeScale();
                List<EvaluationStatusEvent> next = DeclarationValidator.timeScaleIsValid( timeScale,
                                                                                          orientation,
                                                                                          false );
                events.addAll( next );

                // Source timescale must be consistent with desired/evaluation timescale
                List<EvaluationStatusEvent> scaleEvents =
                        DeclarationValidator.datasetTimeScaleConsistentWithDesiredTimeScale( timeScale,
                                                                                             desiredTimeScale,
                                                                                             orientation );
                events.addAll( scaleEvents );
            }
            // Evaluation timescale exists, dataset timescale does not
            else if ( Objects.nonNull( desiredTimeScale )
                      // Dataset time scale cannot be gleaned from context
                      && DeclarationValidator.hasDataSourceWithUnknownTimeScale( dataset ) )
            {
                String orientationString = DeclarationValidator.getTimeScaleOrientationString( orientation, false );

                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( "The evaluation declares a 'time_scale', but the "
                                                                 + "'time_scale' associated with "
                                                                 + orientationString
                                                                 + " dataset is undefined. Unless the data source for "
                                                                 + orientationString
                                                                 + " dataset clarifies its own time scale, it is "
                                                                 + "assumed that the dataset has the same time scale "
                                                                 + "as the evaluation 'time_scale' and no rescaling "
                                                                 + "will be performed. If this is incorrect or you are "
                                                                 + "unsure, it is best to declare the 'time_scale' of "
                                                                 + orientationString
                                                                 + " dataset." )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * @param dataset the dataset
     * @return whether any dataset has a data source whose timescale cannot be guaranteed present
     */
    private static boolean hasDataSourceWithUnknownTimeScale( Dataset dataset )
    {
        // The USGS NWIS IV service is guaranteed to supply instantaneous data
        return !dataset.sources()
                       .stream()
                       .allMatch( s -> s.sourceInterface() == SourceInterface.USGS_NWIS );
    }

    /**
     * Checks that the time zone offsets are mutually consistent.
     * @param declaration the declaration
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> timeZoneOffsetsAreValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> leftEvents =
                DeclarationValidator.timeZoneOffsetsAreValid( declaration.left(),
                                                              DatasetOrientation.LEFT );
        List<EvaluationStatusEvent> events = new ArrayList<>( leftEvents );
        List<EvaluationStatusEvent> rightEvents
                = DeclarationValidator.timeZoneOffsetsAreValid( declaration.right(),
                                                                DatasetOrientation.RIGHT );
        events.addAll( rightEvents );

        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            List<EvaluationStatusEvent> baselineEvents
                    = DeclarationValidator.timeZoneOffsetsAreValid( declaration.baseline()
                                                                               .dataset(),
                                                                    DatasetOrientation.BASELINE );
            events.addAll( baselineEvents );
        }

        for ( CovariateDataset covariate : declaration.covariates() )
        {
            List<EvaluationStatusEvent> covariateEvents
                    = DeclarationValidator.timeZoneOffsetsAreValid( covariate.dataset(),
                                                                    DatasetOrientation.COVARIATE );
            events.addAll( covariateEvents );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the measurement units are mutually consistent.
     * @param declaration the declaration
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> unitsAreValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> leftEvents =
                DeclarationValidator.unitsAreValid( declaration.left(),
                                                    DatasetOrientation.LEFT );
        List<EvaluationStatusEvent> events = new ArrayList<>( leftEvents );
        List<EvaluationStatusEvent> rightEvents
                = DeclarationValidator.unitsAreValid( declaration.right(),
                                                      DatasetOrientation.RIGHT );
        events.addAll( rightEvents );

        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            List<EvaluationStatusEvent> baselineEvents
                    = DeclarationValidator.unitsAreValid( declaration.baseline()
                                                                     .dataset(),
                                                          DatasetOrientation.BASELINE );
            events.addAll( baselineEvents );
        }

        for ( CovariateDataset covariate : declaration.covariates() )
        {
            List<EvaluationStatusEvent> covariateEvents
                    = DeclarationValidator.unitsAreValid( covariate.dataset(),
                                                          DatasetOrientation.COVARIATE );
            events.addAll( covariateEvents );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the time zone offsets are mutually consistent.
     * @param dataset the dataset
     * @param orientation the dataset orientation to help with logging
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> timeZoneOffsetsAreValid( Dataset dataset,
                                                                        DatasetOrientation orientation )
    {
        if ( Objects.isNull( dataset ) )
        {
            LOGGER.debug( "When validating the time zone offset of the {} dataset, discovered that the dataset was "
                          + "missing.",
                          orientation );
            return List.of();
        }

        ZoneOffset universalOffset = dataset.timeZoneOffset();
        if ( Objects.isNull( universalOffset ) )
        {
            LOGGER.debug( "The {} dataset did not contain a universal time zone offset.",
                          orientation );

            return Collections.emptyList();
        }

        // There is a universal time zone offset, so all the sources must have a null offset or the same offset as the
        // universal one
        Set<ZoneOffset> offsets = dataset.sources()
                                         .stream()
                                         .map( Source::timeZoneOffset )
                                         .filter( Objects::nonNull )
                                         .collect( Collectors.toSet() );

        if ( offsets.isEmpty() || Set.of( universalOffset )
                                     .equals( offsets ) )
        {
            LOGGER.debug( "The {} dataset contained a universal time zone offset of {}, which is consistent with the "
                          + "offsets for the individual sources it composes.",
                          orientation,
                          universalOffset );

            return Collections.emptyList();
        }

        String article = "the";
        if ( orientation == DatasetOrientation.COVARIATE )
        {
            article = "a";
        }

        EvaluationStatusEvent event
                = EvaluationStatusEvent.newBuilder()
                                       .setStatusLevel( StatusLevel.ERROR )
                                       .setEventMessage( "Discovered a 'time_zone_offset' of "
                                                         + universalOffset
                                                         + " for "
                                                         + article
                                                         + " '"
                                                         + orientation
                                                         + "' dataset, which is inconsistent with some of the "
                                                         + "'time_zone_offset' declared for the individual sources it "
                                                         + "contains: "
                                                         + offsets
                                                         + ". Please address this conflict by removing the "
                                                         + "'time_zone-offset' for "
                                                         + article
                                                         + " '"
                                                         + orientation
                                                         + "' dataset or its individual "
                                                         + "sources or ensuring they match." )
                                       .build();

        return List.of( event );
    }

    /**
     * Checks that the measurement units are mutually consistent.
     * @param dataset the dataset
     * @param orientation the dataset orientation to help with logging
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> unitsAreValid( Dataset dataset,
                                                              DatasetOrientation orientation )
    {
        if ( Objects.isNull( dataset ) )
        {
            LOGGER.debug( "When validating the unit of the {} dataset, discovered that the dataset was "
                          + "missing.",
                          orientation );
            return List.of();
        }

        String universalUnit = dataset.unit();
        if ( Objects.isNull( universalUnit ) )
        {
            LOGGER.debug( "The {} dataset did not contain a universal measurement unit.",
                          orientation );

            return Collections.emptyList();
        }

        // There is a universal measurement unit, so all the sources must have a null unit or the same unit as the
        // universal one
        Set<String> units = dataset.sources()
                                   .stream()
                                   .map( Source::unit )
                                   .filter( Objects::nonNull )
                                   .collect( Collectors.toSet() );

        if ( units.isEmpty() || Set.of( universalUnit )
                                   .equals( units ) )
        {
            LOGGER.debug( "The {} dataset contained a universal measurement unit of {}, which is consistent with the "
                          + "units for the individual sources it composes.",
                          orientation,
                          universalUnit );

            return Collections.emptyList();
        }

        String article = "the";
        if ( orientation == DatasetOrientation.COVARIATE )
        {
            article = "a";
        }

        EvaluationStatusEvent event
                = EvaluationStatusEvent.newBuilder()
                                       .setStatusLevel( StatusLevel.ERROR )
                                       .setEventMessage( "Discovered a 'unit' of "
                                                         + universalUnit
                                                         + " for "
                                                         + article
                                                         + " '"
                                                         + orientation
                                                         + "' dataset, which is inconsistent with some of the "
                                                         + "'unit' declared for the individual sources it "
                                                         + "contains: "
                                                         + units
                                                         + ". Please address this conflict by removing the "
                                                         + "'unit' for "
                                                         + article
                                                         + " '"
                                                         + orientation
                                                         + "' dataset or its individual "
                                                         + "sources or ensuring they match." )
                                       .build();

        return List.of( event );
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
        // Reference dates and valid dates
        List<EvaluationStatusEvent> events
                = new ArrayList<>( DeclarationValidator.timeIntervalIsValid( declaration.referenceDates(),
                                                                             REFERENCE_DATES ) );
        List<EvaluationStatusEvent> validDates = DeclarationValidator.timeIntervalIsValid( declaration.validDates(),
                                                                                           VALID_DATES );
        events.addAll( validDates );

        // Ignored valid dates as they relate to valid dates
        List<EvaluationStatusEvent> ignored =
                DeclarationValidator.ignoredValidDatesAreValid( declaration.validDates(),
                                                                declaration.ignoredValidDates() );
        events.addAll( ignored );

        // Analysis durations
        List<EvaluationStatusEvent> analysisDurations
                = DeclarationValidator.analysisTimesAreValid( declaration.analysisTimes() );
        events.addAll( analysisDurations );

        // Lead times
        List<EvaluationStatusEvent> leadTimes = DeclarationValidator.leadIntervalIsValid( declaration );
        events.addAll( leadTimes );

        // Check that the datetime intervals are mutually consistent/overlapping, after accounting for lead times
        List<EvaluationStatusEvent> consistent = DeclarationValidator.timeIntervalsAreConsistent( declaration );
        events.addAll( consistent );

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the lead time interval is valid.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> leadIntervalIsValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Lead times
        LeadTimeInterval leadTimes = declaration.leadTimes();
        if ( Objects.nonNull( leadTimes )
             && Objects.nonNull( leadTimes.minimum() )
             && Objects.nonNull( leadTimes.maximum() )
             && leadTimes.maximum()
                         .compareTo( declaration.leadTimes()
                                                .minimum() ) < 0 )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The 'lead_times' interval is invalid because the "
                                                             + "'minimum' value is greater than the 'maximum' value. "
                                                             + "Please adjust the 'minimum' to occur before the "
                                                             + "'maximum', or at the same time, and try again. " )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the time intervals are overlapping, after accounting for lead durations, as appropriate.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> timeIntervalsAreConsistent( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Check the reference times and valid times
        TimeInterval referenceDates = declaration.referenceDates();
        TimeInterval validDates = declaration.validDates();
        LeadTimeInterval leadTimes = declaration.leadTimes();

        // Reference dates and valid dates and lead times are all present. Potential error if no overlaps
        if ( Objects.nonNull( referenceDates )
             && Objects.nonNull( validDates )
             && Objects.nonNull( leadTimes ) )
        {
            List<EvaluationStatusEvent> leadTimesPresent =
                    DeclarationValidator.timeIntervalsAreConsistentWithLeadTimes( declaration );
            events.addAll( leadTimesPresent );
        }
        // Reference dates and valid dates are present, but no lead times. Can only warn in this situation
        else if ( Objects.nonNull( referenceDates )
                  && Objects.nonNull( validDates )
                  && ( referenceDates.minimum()
                                     .isAfter( validDates.maximum() )
                       || referenceDates.maximum()
                                        .isBefore( validDates.minimum() ) ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "The 'reference_dates' and 'valid_dates' do not "
                                                             + "overlap. This is unusual, but is allowed. No "
                                                             + "'lead_times' were provided to further validate "
                                                             + "this declaration. However, if no data is "
                                                             + "subsequently found, you should consider adjusting "
                                                             + "the 'reference_dates' and/or 'valid_dates' so that "
                                                             + "they overlap." )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the time intervals are overlapping, after accounting for lead durations, as appropriate.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> timeIntervalsAreConsistentWithLeadTimes( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Check the reference times and valid times
        TimeInterval referenceDates = declaration.referenceDates();
        TimeInterval validDates = declaration.validDates();
        LeadTimeInterval leadTimes = declaration.leadTimes();

        Instant refMinimum = referenceDates.minimum();
        Instant refMaximum = referenceDates.maximum();

        if ( Objects.nonNull( leadTimes.minimum() )
             && !refMinimum.equals( Instant.MIN )
             && !refMinimum.equals( Instant.MAX ) )
        {
            refMinimum = refMinimum.plus( leadTimes.minimum() );
        }

        if ( Objects.nonNull( leadTimes.maximum() )
             && !refMaximum.equals( Instant.MIN )
             && !refMaximum.equals( Instant.MAX ) )
        {
            refMaximum = refMaximum.plus( leadTimes.maximum() );
        }

        if ( refMinimum.isAfter( validDates.maximum() ) )
        {
            EvaluationStatusEvent event;

            if ( Objects.nonNull( leadTimes.minimum() ) )
            {
                event = EvaluationStatusEvent.newBuilder()
                                             .setStatusLevel( StatusLevel.ERROR )
                                             .setEventMessage( "The 'minimum' value of the 'reference_dates' is "
                                                               + referenceDates.minimum()
                                                               + " and the 'minimum' value of the 'lead_times' "
                                                               + "is "
                                                               + leadTimes.minimum()
                                                               + ", but the 'maximum' value of the 'valid_dates' "
                                                               + "is "
                                                               + validDates.maximum()
                                                               + ". This will not select any data for evaluation "
                                                               + "and is not, therefore, allowed. Please adjust "
                                                               + "one or more of these times to ensure that "
                                                               + "data will be selected for evaluation." )
                                             .build();
            }
            else
            {
                event = EvaluationStatusEvent.newBuilder()
                                             .setStatusLevel( StatusLevel.WARN )
                                             .setEventMessage( "The 'minimum' value of the 'reference_dates' is "
                                                               + referenceDates.minimum()
                                                               + ", but the 'maximum' value of the 'valid_dates' "
                                                               + "is "
                                                               + validDates.maximum()
                                                               + ". This may not select any data for evaluation. "
                                                               + "However, a 'minimum' value for the "
                                                               + "'lead_times' was not discovered, which could "
                                                               + "further clarify. If no data is found "
                                                               + "subsequently, you should consider adjusting "
                                                               + "one or more of these time intervals to ensure "
                                                               + "that data is selected." )
                                             .build();
            }

            events.add( event );
        }

        if ( refMaximum.isBefore( validDates.minimum() ) )
        {
            EvaluationStatusEvent event;

            if ( Objects.nonNull( leadTimes.maximum() ) )
            {
                event = EvaluationStatusEvent.newBuilder()
                                             .setStatusLevel( StatusLevel.ERROR )
                                             .setEventMessage( "The 'maximum' value of the 'reference_dates' is "
                                                               + referenceDates.maximum()
                                                               + " and the 'maximum' value of the 'lead_times' "
                                                               + "is "
                                                               + leadTimes.maximum()
                                                               + ", but the 'minimum' value of the 'valid_dates' "
                                                               + "is "
                                                               + validDates.minimum()
                                                               + ". This will not select any data for evaluation "
                                                               + "and is not, therefore, allowed. Please adjust "
                                                               + "one or more of these times to ensure that "
                                                               + "data will be selected for evaluation." )
                                             .build();
            }
            else
            {
                event = EvaluationStatusEvent.newBuilder()
                                             .setStatusLevel( StatusLevel.WARN )
                                             .setEventMessage( "The 'maximum' value of the 'reference_dates' is "
                                                               + referenceDates.maximum()
                                                               + ", but the 'minimum' value of the 'valid_dates' "
                                                               + "is "
                                                               + validDates.minimum()
                                                               + ". This may not select any data for evaluation. "
                                                               + "However, a 'maximum' value for the "
                                                               + "'lead_times' was not discovered, which could "
                                                               + "further clarify. If no data is found "
                                                               + "subsequently, you should consider adjusting "
                                                               + "one or more of these time intervals to ensure "
                                                               + "that data is selected." )
                                             .build();
            }

            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the ignored valid dates are valid.
     * @param validDates the valid dates
     * @param ignoredValidDates the ignored valid dates
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> ignoredValidDatesAreValid( TimeInterval validDates,
                                                                          Set<TimeInterval> ignoredValidDates )
    {
        if ( ignoredValidDates.isEmpty() )
        {
            LOGGER.debug( "No ignored valid dates intervals were detected for validation." );
            return List.of();
        }

        List<EvaluationStatusEvent> events =
                new ArrayList<>( DeclarationValidator.ignoredValidDatesDoNotOverlap( ignoredValidDates ) );

        List<EvaluationStatusEvent> someData =
                DeclarationValidator.ignoredValidDatesDoNotSpanAllValidDates( validDates, ignoredValidDates );
        events.addAll( someData );

        for ( TimeInterval nextInterval : ignoredValidDates )
        {
            List<EvaluationStatusEvent> invalidInterval =
                    DeclarationValidator.timeIntervalIsValid( nextInterval,
                                                              "'ignored_valid_dates' ("
                                                              + nextInterval.minimum()
                                                              + ", "
                                                              + nextInterval.maximum()
                                                              + ")" );
            events.addAll( invalidInterval );
        }
        return Collections.unmodifiableList( events );
    }

    /**
     * Warns if the ignored valid date intervals overlap.
     * @param ignoredValidDates the ignored valid dates
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> ignoredValidDatesDoNotOverlap( Set<TimeInterval> ignoredValidDates )
    {
        if ( ignoredValidDates.size() == 1 )
        {
            LOGGER.debug( "Detected only one interval of valid dates to ignore, which passes the non-overlapping "
                          + "interval test." );
            return List.of();
        }

        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Sort the intervals
        SortedSet<Pair<Instant, Instant>> sorted = ignoredValidDates.stream()
                                                                    .map( d -> Pair.of( d.minimum(), d.maximum() ) )
                                                                    .collect( Collectors.toCollection( TreeSet::new ) );
        Instant lastMaximum = null;
        for ( Pair<Instant, Instant> nextInterval : sorted )
        {
            if ( Objects.isNull( lastMaximum ) )
            {
                lastMaximum = nextInterval.getRight();
            }
            else if ( nextInterval.getLeft()
                                  .compareTo( lastMaximum ) <= 0 )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( "One or more of the intervals contained in "
                                                                 + "'ignored_valid_dates' overlap. As it is sufficient "
                                                                 + "to define the overlapping period only once, this "
                                                                 + "may not be intended. Please check that the "
                                                                 + "'ignored_valid_dates' are declared correctly and, "
                                                                 + "if necessary, adjust them so that they do not "
                                                                 + "overlap. This evaluation will proceed with the "
                                                                 + "overlapping intervals." )
                                               .build();
                events.add( event );
                break;
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Produces an error if the ignored valid date intervals exhaust the valid dates interval.
     * @param validDates the valid dates
     * @param ignoredValidDates the ignored valid dates
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> ignoredValidDatesDoNotSpanAllValidDates( TimeInterval validDates,
                                                                                        Set<TimeInterval> ignoredValidDates )
    {
        if ( Objects.isNull( validDates ) )
        {
            LOGGER.debug( "Detected no valid_dates interval, so the ignored_valid_dates are assumed valid." );
            return List.of();
        }

        // Sort the intervals
        SortedSet<Pair<Instant, Instant>> sorted = ignoredValidDates.stream()
                                                                    .map( d -> Pair.of( d.minimum(), d.maximum() ) )
                                                                    .collect( Collectors.toCollection( TreeSet::new ) );
        if ( validDates.minimum()
                       .isBefore( sorted.first()
                                        .getLeft() )
             || validDates.maximum()
                          .isAfter( sorted.last()
                                          .getRight() ) )
        {
            LOGGER.debug( "Detected some valid dates before or after the earliest or latest date to ignore, "
                          + "respectively." );

            return List.of();
        }

        // Eliminate any overlap between the ignored intervals, which otherwise complicates the comparison
        SortedSet<Pair<Instant, Instant>> sortedNoOverlaps = new TreeSet<>();
        Pair<Instant, Instant> last = null;
        for ( Pair<Instant, Instant> nextPair : sorted )
        {
            if ( Objects.isNull( last ) )
            {
                last = nextPair;
                sortedNoOverlaps.add( last );
            }
            else
            {
                if ( last.getRight()
                         .isAfter( nextPair.getLeft() ) )
                {
                    sortedNoOverlaps.add( Pair.of( last.getRight(), nextPair.getRight() ) );
                }
                else
                {
                    sortedNoOverlaps.add( nextPair );
                }
            }
        }

        // Find the duration by which each ignored interval overlaps the valid dates.
        Duration totalOverlapDuration = Duration.ZERO;
        for ( Pair<Instant, Instant> nextInterval : sortedNoOverlaps )
        {
            Instant maximumLower = Collections.max( List.of( nextInterval.getLeft(), validDates.minimum() ) );
            Instant minimumHigher = Collections.min( List.of( nextInterval.getRight(), validDates.maximum() ) );
            Duration difference = Duration.between( maximumLower, minimumHigher );
            totalOverlapDuration = totalOverlapDuration.plus( difference );
        }

        // The total possible overlap duration is the duration between the earliest date in the first interval and the
        // latest date in the last interval
        Instant maximumLower = Collections.max( List.of( sortedNoOverlaps.first()
                                                                         .getLeft(), validDates.minimum() ) );
        Instant minimumHigher = Collections.min( List.of( sortedNoOverlaps.last()
                                                                          .getRight(), validDates.maximum() ) );
        Duration totalPossibleOverlapDuration = Duration.between( maximumLower, minimumHigher );

        LOGGER.debug( "The total overlapping duration among the ignored intervals and valid dates was: {}, whereas the "
                      + "total possible duration for overlap was: {}.",
                      totalOverlapDuration,
                      totalPossibleOverlapDuration );

        List<EvaluationStatusEvent> events = new ArrayList<>();
        if ( totalOverlapDuration.equals( totalPossibleOverlapDuration ) )
        {
            EvaluationStatusEvent error
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The 'ignored_valid_dates' completely overlap the "
                                                             + "'valid_dates', which is not allowed because the "
                                                             + "evaluation will contain no data, by definition. Please "
                                                             + "adjust the 'ignored_valid_dates' and/or the "
                                                             + "'valid_dates' and try again." )
                                           .build();
            events.add( error );
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
        List<EvaluationStatusEvent> validPools = DeclarationValidator.timePoolsAreValid( declaration.validDatePools(),
                                                                                         declaration.validDates(),
                                                                                         "valid_date_pools",
                                                                                         "valid_dates" );
        List<EvaluationStatusEvent> events = new ArrayList<>( validPools );

        List<EvaluationStatusEvent> referencePools =
                DeclarationValidator.timePoolsAreValid( declaration.referenceDatePools(),
                                                        declaration.referenceDates(),
                                                        "reference_date_pools",
                                                        "reference_dates" );
        events.addAll( referencePools );

        List<EvaluationStatusEvent> leadTimePools =
                DeclarationValidator.leadTimePoolsAreValid( declaration.leadTimePools(),
                                                            declaration.leadTimes() );
        events.addAll( leadTimePools );

        // Add a warning if a pool sequence is combined with explicitly generated pools
        Set<String> generated = new TreeSet<>();
        if ( !declaration.validDatePools()
                         .isEmpty() )
        {
            generated.add( "'valid_date_pools'" );
        }
        if ( !declaration.referenceDatePools()
                         .isEmpty() )
        {
            generated.add( "'reference_date_pools'" );
        }
        if ( !declaration.leadTimePools()
                         .isEmpty() )
        {
            generated.add( "'lead_time_pools'" );
        }

        if ( !declaration.timePools()
                         .isEmpty()
             && !generated.isEmpty() )
        {
            EvaluationStatusEvent warning
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "The declaration contained both an explicit list of "
                                                             + "'time_pools' and an implicitly declared sequence of "
                                                             + "pools: "
                                                             + generated
                                                             + ". This is allowed, and the "
                                                             + "resulting pools from all sources will be added "
                                                             + "together. If this was not intended, please adjust "
                                                             + "your declaration." )
                                           .build();
            events.add( warning );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that any event detection is valid.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> eventDetectionIsValid( EvaluationDeclaration declaration )
    {
        // Validate the covariates, if defined. This includes sad paths even when event detection is undefined
        List<EvaluationStatusEvent> events =
                new ArrayList<>( DeclarationValidator.covariatesAreValidForEventDetection( declaration ) );

        // All remaining sad paths require event detection to be declared
        if ( Objects.isNull( declaration.eventDetection() ) )
        {
            return events;
        }

        // No forecast data for event detection
        List<EvaluationStatusEvent> forecasts =
                DeclarationValidator.eventDetectionDoesNotUseForecastData( declaration );
        events.addAll( forecasts );

        // Error when also declaring valid date pools
        if ( !declaration.validDatePools()
                         .isEmpty() )
        {
            EvaluationStatusEvent error
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Event detection was declared alongside valid date pools, "
                                                             + "which is not allowed because event detection "
                                                             + "also generates valid date pools. Please remove the "
                                                             + "declaration of either 'event_detection' or "
                                                             + "'valid_date_pools' and try again." )
                                           .build();
            events.add( error );
        }

        // Warn when also declaring explicit time pools, but allow because a user may want to guarantee an explicit pool
        if ( !declaration.timePools()
                         .isEmpty() )
        {
            EvaluationStatusEvent error
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "Event detection was declared alongside explicit time "
                                                             + "pools, which is allowed, but may not be intended as "
                                                             + "event detection also generates time pools. If this is "
                                                             + "not intended, please remove the declaration of either "
                                                             + "'event_detection' or 'time_pools' and try again." )
                                           .build();
            events.add( error );
        }

        // Error when declaring feature groups or a feature service with pooling across features within a group
        if ( DeclarationUtilities.hasFeatureGroups( declaration ) )
        {
            EvaluationStatusEvent error
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Event detection was declared alongside feature groups, "
                                                             + "which is not currently supported. Please remove the "
                                                             + "declaration of 'feature_groups' or a 'feature_service' "
                                                             + "with a 'group' whose features will be pooled together "
                                                             + "(i.e., 'pool: true'), as applicable, and try again. "
                                                             + "Alternatively, please remove 'event_detection'. Hint: "
                                                             + "summary statistics are supported alongside event "
                                                             + "detection if your goal is to compute statistics across "
                                                             + "events for multiple geographic features." )
                                           .build();
            events.add( error );
        }

        // Error when a baseline dataset is declared for event detection that does not exist
        if ( declaration.eventDetection()
                        .datasets()
                        .contains( EventDetectionDataset.BASELINE )
             && !DeclarationUtilities.hasBaseline( declaration ) )
        {
            EvaluationStatusEvent error
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Event detection was declared with a baseline data "
                                                             + "source, but no baseline dataset was declared. Please "
                                                             + "declare a 'baseline' dataset or remove the 'baseline' "
                                                             + "from the 'dataset' used for 'event_detection' and try "
                                                             + AGAIN )
                                           .build();
            events.add( error );
        }

        // Warning when declaring other types of explicit pool
        if ( !declaration.leadTimePools()
                         .isEmpty() )
        {
            EvaluationStatusEvent warn
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "Event detection was declared alongside lead time pools, "
                                                             + "which is allowed, but may not be intended. A separate "
                                                             + "pool will be generated for each combination of event "
                                                             + "and lead time pool. If this is not intended, please "
                                                             + "remove either 'event_detection' or 'lead_time_pools' "
                                                             + "and try again." )
                                           .build();
            events.add( warn );
        }
        if ( !declaration.referenceDatePools()
                         .isEmpty() )
        {
            EvaluationStatusEvent warn
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "Event detection was declared alongside reference date "
                                                             + "pools, which is allowed, but may not be intended. A "
                                                             + "separate pool will be generated for each combination "
                                                             + "of event and reference date pool. If this is not "
                                                             + "intended, please remove either 'event_detection' or "
                                                             + "'reference_date_pools' and try again." )
                                           .build();
            events.add( warn );
        }

        // Check parameters
        List<EvaluationStatusEvent> parameters = DeclarationValidator.eventDetectionParametersAreValid( declaration );
        events.addAll( parameters );

        return Collections.unmodifiableList( events );
    }

    /**
     * Validates covariates in the context of event detection.
     *
     * @param declaration the declaration
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> covariatesAreValidForEventDetection( EvaluationDeclaration declaration )
    {

        if ( Objects.isNull( declaration.eventDetection() ) )
        {
            List<EvaluationStatusEvent> events = new ArrayList<>();

            // Error when a covariate dataset is declared with the purpose of event detection, but no event detection is
            // declared
            if ( declaration.covariates()
                            .stream()
                            .anyMatch( n -> n.purposes()
                                             .contains( CovariatePurpose.DETECT ) ) )
            {
                EvaluationStatusEvent error
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The evaluation declared one or more 'covariates' "
                                                                 + "whose 'purpose' is to 'detect' events, but event "
                                                                 + "detection was not declared. Please declare "
                                                                 + "'event_detection' or remove the 'covariates' whose "
                                                                 + "'purpose' is to 'detect' and " + TRY_AGAIN )
                                               .build();
                events.add( error );
            }

            return Collections.unmodifiableList( events );
        }

        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Error when a covariate dataset is declared for event detection that does not exist
        if ( declaration.eventDetection()
                        .datasets()
                        .contains( EventDetectionDataset.COVARIATES )
             && declaration.covariates()
                           .stream()
                           // No explicit or implicit purpose of detect, implicit if no filtering
                           .noneMatch( n -> n.purposes()
                                             .contains( CovariatePurpose.DETECT )
                                            || ( n.purposes()
                                                  .isEmpty()
                                                 && Objects.isNull( n.minimum() )
                                                 && Objects.isNull( n.maximum() ) ) ) )
        {
            EvaluationStatusEvent error
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Event detection was declared with a covariate data "
                                                             + "source, but no covariate dataset was declared for "
                                                             + "event detection. Please declare one or more "
                                                             + "'covariates' with a 'purpose' of 'detect' or "
                                                             + "remove the 'covariates' from the 'dataset' declared "
                                                             + "for 'event_detection' and try "
                                                             + AGAIN )
                                           .build();
            events.add( error );
        }

        // Warn if covariates are used without an explicit purpose of detect
        if ( declaration.eventDetection()
                        .datasets()
                        .contains( EventDetectionDataset.COVARIATES )
             && declaration.covariates()
                           .stream()
                           .anyMatch( n -> n.purposes()
                                            .isEmpty()
                                           && Objects.isNull( n.minimum() )
                                           && Objects.isNull( n.maximum() ) ) )
        {
            EvaluationStatusEvent error
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "Event detection was declared with covariate data "
                                                             + "sources, but the 'purpose' of one or more of these "
                                                             + "covariates was not explicitly declared as 'detect'. "
                                                             + "As these covariates do not provide filtering "
                                                             + "criteria, they are assumed to be declared for event "
                                                             + "detection and will be used for this purpose." )
                                           .build();
            events.add( error );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that any event detection parameters are valid.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> eventDetectionParametersAreValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        EventDetection detection = declaration.eventDetection();
        EventDetectionParameters parameters = detection.parameters();

        // Parameters undefined for which estimates/defaults are speculative: warn
        if ( Objects.isNull( parameters )
             || Objects.isNull( parameters.windowSize() ) )
        {
            EvaluationStatusEvent warn
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "Event detection was declared, but the window size "
                                                             + "parameter was undefined. An attempt will be made to "
                                                             + "choose a reasonable default by inspecting the "
                                                             + "time-series data, but it is strongly recommended that "
                                                             + "you instead declare the 'window_size' explicitly as "
                                                             + "the default value may not be appropriate." )
                                           .build();
            events.add( warn );
        }
        if ( Objects.isNull( parameters )
             || Objects.isNull( parameters.halfLife() ) )
        {
            EvaluationStatusEvent warn
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "Event detection was declared, but the half-life "
                                                             + "parameter was undefined. An attempt will be made to "
                                                             + "choose a reasonable default by inspecting the "
                                                             + "time-series data, but it is strongly recommended that "
                                                             + "you instead declare the 'half_life' explicitly as the "
                                                             + "default value may not be appropriate." )
                                           .build();
            events.add( warn );
        }
        if ( Objects.nonNull( parameters ) )
        {
            if ( parameters.combination() != EventDetectionCombination.INTERSECTION
                 && Objects.nonNull( parameters.aggregation() ) )
            {
                EvaluationStatusEvent warn
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "Event detection was declared with an 'operation' of '"
                                                                 + parameters.combination()
                                                                             .toString()
                                                                             .toLowerCase()
                                                                 + "' and an aggregation method of '"
                                                                 + parameters.aggregation()
                                                                             .toString()
                                                                             .toLowerCase()
                                                                 + "', which is not valid. Please remove the "
                                                                 + "'aggregation' method or change the 'operation' to "
                                                                 + "'intersection'. An explicit 'aggregation' method "
                                                                 + "is only valid when the 'operation' is "
                                                                 + "'intersection'." )
                                               .build();
                events.add( warn );
            }

            // Warn if a non-default combination method is declared for a singleton: it will have no effect
            if ( parameters.combination() != EventDetectionCombination.UNION
                 && detection.datasets()
                             .size() == 1
                 && ( !detection.datasets()
                                .contains( EventDetectionDataset.COVARIATES )
                      || declaration.covariates()
                                    .size() == 1 ) )
            {
                EvaluationStatusEvent warn
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( "Event detection was declared for a single dataset "
                                                                 + "with 'combination' parameters, but these "
                                                                 + "parameters are only applicable when performing "
                                                                 + "event detection on more than one dataset. The "
                                                                 + "'combination' parameters are redundant and will "
                                                                 + "have no effect. For clarity, it would be better to "
                                                                 + "remove them." )
                                               .build();
                events.add( warn );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the time-series datasets used for event detection are not forecasts.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> eventDetectionDoesNotUseForecastData( EvaluationDeclaration declaration )
    {
        if ( Objects.isNull( declaration.eventDetection() ) )
        {
            LOGGER.debug( "No need to validate event detection for non-forecast datasets as event detection was not "
                          + "declared." );

            return List.of();
        }

        List<EvaluationStatusEvent> events = new ArrayList<>();

        Set<EventDetectionDataset> datasets = declaration.eventDetection()
                                                         .datasets();

        Set<EventDetectionDataset> failed =
                datasets.stream()
                        .filter( n -> DeclarationValidator.isEventDetectionDatasetForecastType( declaration, n ) )
                        .collect( Collectors.toCollection( TreeSet::new ) );

        if ( !failed.isEmpty() )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The declaration requested event detection, but the "
                                                             + "following data sources used for event detection "
                                                             + "contained forecast data, which is not allowed: "
                                                             + failed
                                                             + ". Please remove 'event_detection' or declare only "
                                                             + "non-forecast datasets for event detection and try "
                                                             + AGAIN )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Determines whether the nominated dataset is a forecast type.
     * @param declaration the declaration
     * @param dataset the dataset to test
     * @return whether the dataset is a forecast type
     * @throws NullPointerException if the left or right dataset is missing
     */

    private static boolean isEventDetectionDatasetForecastType( EvaluationDeclaration declaration,
                                                                EventDetectionDataset dataset )
    {
        Objects.requireNonNull( declaration.left() );
        Objects.requireNonNull( declaration.right() );

        switch ( dataset )
        {
            case OBSERVED ->
            {
                if ( DeclarationUtilities.isForecast( declaration.left() ) )
                {
                    return true;
                }
            }
            case PREDICTED ->
            {
                if ( DeclarationUtilities.isForecast( declaration.right() ) )
                {
                    return true;
                }
            }
            case BASELINE ->
            {
                if ( DeclarationUtilities.hasBaseline( declaration )
                     && DeclarationUtilities.isForecast( declaration.baseline()
                                                                    .dataset() ) )
                {
                    return true;
                }
            }
            case COVARIATES ->
            {
                if ( declaration.covariates()
                                .stream()
                                .anyMatch( n -> n.purposes()
                                                 .contains( CovariatePurpose.DETECT )
                                                && DeclarationUtilities.isForecast( n.dataset() ) ) )
                {
                    return true;
                }
            }
        }

        return false;
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
        // Check that a features service is present when resolving sparse features with different authorities
        List<EvaluationStatusEvent> featureService =
                DeclarationValidator.checkFeatureServicePresentIfRequired( declaration );
        events.addAll( featureService );
        // Feature authorities for any covariates are consistent with one of the prescribed data orientations
        List<EvaluationStatusEvent> covariateFeatureAuthorities =
                DeclarationValidator.checkCovariateFeatureAuthorities( declaration );
        events.addAll( covariateFeatureAuthorities );
        // Check that any featureful thresholds correlate with declared features
        List<EvaluationStatusEvent> thresholds =
                DeclarationValidator.validateFeaturefulThresholds( declaration );
        events.addAll( thresholds );

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the metrics declaration is valid.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> metricsAreValid( EvaluationDeclaration declaration )
    {
        // Check and warn about duplication of metrics by name
        List<EvaluationStatusEvent> duplication =
                DeclarationValidator.checkForDuplicationOfMetricsByName( declaration );
        List<EvaluationStatusEvent> events = new ArrayList<>( duplication );
        // Baseline defined for metrics that require one
        List<EvaluationStatusEvent> baselinePresent =
                DeclarationValidator.checkBaselinePresentForMetricsThatNeedIt( declaration );
        events.addAll( baselinePresent );
        // When categorical metrics are declared, there must be event thresholds as a minimum
        List<EvaluationStatusEvent> categoricalMetrics =
                DeclarationValidator.checkEventThresholdsForCategoricalMetrics( declaration );
        events.addAll( categoricalMetrics );

        // Check that low-level parameters do not disagree with top-level ones
        List<EvaluationStatusEvent> parameters =
                DeclarationValidator.checkMetricParametersAreConsistent( declaration );
        events.addAll( parameters );

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the summary statistics declaration is valid.
     * @param declaration the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> summaryStatisticsAreValid( EvaluationDeclaration declaration )
    {
        if ( declaration.summaryStatistics()
                        .isEmpty() )
        {
            LOGGER.debug( "No summary statistics declaration to validate." );

            return List.of();
        }

        List<EvaluationStatusEvent> events = new ArrayList<>();
        Set<SummaryStatistic> summaryStatistics = declaration.summaryStatistics();

        // Ensure feature groups are declared when needed. Unlike features, they cannot be declared implicitly, so
        // absence is an error rather than a warning
        if ( !DeclarationUtilities.hasFeatureGroups( declaration )
             && declaration.summaryStatistics()
                           .stream()
                           .anyMatch( s -> s.getDimensionList()
                                            .contains( SummaryStatistic.StatisticDimension.FEATURE_GROUP ) ) )
        {
            EvaluationStatusEvent error
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Summary statistics were declared to summarize the "
                                                             + "evaluation results across geographic feature groups, "
                                                             + "but no feature groups with multiple features were "
                                                             + "declared for evaluation. Please declare some feature "
                                                             + "groups that contain multiple features or remove the "
                                                             + "'summary_statistics' with a 'dimensions' entry of "
                                                             + "'feature groups' and try again." )
                                           .build();
            events.add( error );
        }

        // Ensure that valid time pools are declared when needed
        if ( !DeclarationValidator.hasPoolsWithQualifiedValidDates( declaration )
             && declaration.summaryStatistics()
                           .stream()
                           .anyMatch( s -> s.getDimensionList()
                                            .contains( SummaryStatistic.StatisticDimension.VALID_DATE_POOLS ) ) )
        {
            EvaluationStatusEvent error
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Summary statistics were declared to summarize the "
                                                             + "evaluation results across valid date pools, "
                                                             + "but the declaration does not contain any pools with "
                                                             + "qualified valid dates. Please declare some pools with "
                                                             + "qualified valid dates or remove the option to "
                                                             + "calculate summary statistics across valid date pools." )
                                           .build();
            events.add( error );
        }

        // Warn when no features are declared explicitly. Could still be implicitly declared via ingested data.
        if ( summaryStatistics.stream()
                              .anyMatch( d -> d.getDimensionList()
                                               .contains( SummaryStatistic.StatisticDimension.FEATURES ) )
             && Objects.isNull( declaration.features() )
             && Objects.isNull( declaration.featureService() )
             && Objects.isNull( declaration.spatialMask() ) )
        {
            EvaluationStatusEvent warn
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "Summary statistics were requested to summarize the "
                                                             + "evaluation results across geographic features, but no "
                                                             + "features were declared for evaluation." )
                                           .build();
            events.add( warn );
        }

        // Warn if the declaration contains diagrams and there are quantile statistics without a median
        Predicate<SummaryStatistic> filter = s -> s.getStatistic() == SummaryStatistic.StatisticName.QUANTILE
                                                  && !s.getDimensionList()
                                                       .contains( SummaryStatistic.StatisticDimension.RESAMPLED );

        Set<MetricConstants> diagrams = declaration.metrics()
                                                   .stream()
                                                   .map( Metric::name )
                                                   .filter( m -> m.isInGroup( MetricConstants.StatisticType.DIAGRAM ) )
                                                   .collect( Collectors.toSet() );

        if ( !diagrams.isEmpty()
             && summaryStatistics.stream()
                                 .anyMatch( filter )
             && summaryStatistics.stream()
                                 .noneMatch( s -> s.getProbability() == 0.5 ) )
        {
            EvaluationStatusEvent warn
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "Summary statistics with quantiles were declared "
                                                             + "alongside diagram metrics and graphics formats. "
                                                             + "However, quantile statistics cannot be plotted "
                                                             + "for diagram metrics unless the quantiles include a "
                                                             + "median value. Since no median was declared, graphics "
                                                             + "will not be generated for the quantile summary "
                                                             + "statistics of the following metrics: "
                                                             + diagrams )
                                           .build();
            events.add( warn );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Inspects the declaration for pools with qualified valid dates.
     * @param declaration the declaration
     * @return whether there are any pools with qualified valid dates
     */

    private static boolean hasPoolsWithQualifiedValidDates( EvaluationDeclaration declaration )
    {
        return !declaration.validDatePools()
                           .isEmpty()
               || Objects.nonNull( declaration.eventDetection() )
               || declaration.timePools()
                             .stream()
                             .anyMatch( n -> n.hasEarliestValidTime()
                                             || n.hasLatestValidTime() );
    }

    /**
     * Checks that the declaration of thresholds is valid.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> thresholdsAreValid( EvaluationDeclaration declaration )
    {
        // Check that the threshold sources are valid
        List<EvaluationStatusEvent> events =
                new ArrayList<>( DeclarationValidator.thresholdSourcesAreValid( declaration ) );

        // Check that value thresholds include units
        List<EvaluationStatusEvent> valueThresholds = DeclarationValidator.valueThresholdsIncludeUnits( declaration );
        events.addAll( valueThresholds );

        // Check that the feature orientation is consistent with other declaration
        List<EvaluationStatusEvent> featureThresholds =
                DeclarationValidator.thresholdFeatureNameFromIsValid( declaration );
        events.addAll( featureThresholds );

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that real-valued thresholds include units.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> valueThresholdsIncludeUnits( EvaluationDeclaration declaration )
    {
        // Check that the threshold sources are valid
        List<EvaluationStatusEvent> events = new ArrayList<>();

        Set<Threshold> thresholds = DeclarationUtilities.getInbandThresholds( declaration );
        if ( thresholds.stream()
                       .filter( t -> t.type() == ThresholdType.VALUE )
                       .anyMatch( t -> t.threshold()
                                        .getThresholdValueUnits()
                                        .isEmpty() ) )
        {
            String extra;

            if ( Objects.nonNull( declaration.unit() ) )
            {
                extra = "The threshold values are assumed to adopt the evaluation unit of '"
                        + declaration.unit()
                        + "'.";
            }
            else
            {
                extra = "The unit will be inferred from the time-series data because no evaluation unit was declared. "
                        + "You may clarify the threshold unit by declaring the 'unit' for each threshold or the "
                        + "'unit' associated with the evaluation.";
            }

            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "Discovered one or more real-valued thresholds without a "
                                                             + "declared threshold unit. "
                                                             + extra )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the {@code feature_name_from} declaration associated with thresholds is valid, specifically that a
     * baseline dataset exists when a baseline orientation is declared.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> thresholdFeatureNameFromIsValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        Set<Threshold> thresholds = DeclarationUtilities.getInbandThresholds( declaration );
        if ( thresholds.stream()
                       .anyMatch( t -> t.featureNameFrom() == DatasetOrientation.BASELINE )
             && !DeclarationUtilities.hasBaseline( declaration ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Discovered one or more thresholds with a "
                                                             + "'feature_name_from' of 'baseline', but the evaluation "
                                                             + "does not declare a 'baseline' dataset. Please fix the "
                                                             + "'feature_name_from' or declare a 'baseline' dataset and "
                                                             + TRY_AGAIN )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the sampling uncertainty declaration is valid.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> checkForDuplicationOfMetricsByName( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Find duplicate metrics by name
        Set<MetricConstants> duplicates = declaration.metrics()
                                                     .stream()
                                                     .collect( Collectors.groupingBy( Metric::name ) )
                                                     .entrySet()
                                                     .stream()
                                                     // Filter entries for duplicate names
                                                     .filter( e -> e.getValue()
                                                                    .size() > 1 )
                                                     .map( Map.Entry::getKey )
                                                     .collect( Collectors.toSet() );

        if ( !duplicates.isEmpty() )
        {
            // Always warn about cost of this option
            EvaluationStatusEvent error
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "Encountered duplicate metrics with different parameter "
                                                             + "values for each instance. While this is allowed, it is "
                                                             + "unusual. Please check that you intended to calculate "
                                                             + "the same metric(s) more than once with different "
                                                             + "parameter values for each instance. The following "
                                                             + "metrics were duplicated: "
                                                             + duplicates
                                                             + "." )
                                           .build();
            events.add( error );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the threshold source declaration is valid.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> thresholdSourcesAreValid( EvaluationDeclaration declaration )
    {
        if ( !declaration.thresholdSources()
                         .isEmpty() )
        {
            Set<GeometryTuple> features = DeclarationUtilities.getFeatures( declaration );
            boolean hasBaseline = DeclarationUtilities.hasBaseline( declaration );
            return declaration.thresholdSources()
                              .stream()
                              .flatMap( n -> DeclarationValidator.thresholdSourceIsValid( n,
                                                                                          declaration.unit(),
                                                                                          features,
                                                                                          hasBaseline )
                                                                 .stream() )
                              .toList();
        }

        return Collections.emptyList();
    }

    /**
     * Checks that the sampling uncertainty declaration is valid.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> samplingUncertaintyIsValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();
        SamplingUncertainty samplingUncertainty = declaration.sampleUncertainty();
        if ( Objects.nonNull( declaration.sampleUncertainty() ) )
        {
            // Always warn about cost of this option
            EvaluationStatusEvent warn
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( THE_EVALUATION_REQUESTED_THE_SAMPLING_UNCERTAINTY
                                                             + "option, which will be delivered using a resampling "
                                                             + "scheme that is computationally expensive. This option "
                                                             + "should be used with care and the evaluation may take "
                                                             + "significantly longer to complete than an equivalent "
                                                             + "evaluation without the 'sampling_uncertainty' "
                                                             + "declaration." )
                                           .build();
            events.add( warn );

            // Quantiles are missing
            if ( Objects.isNull( samplingUncertainty.quantiles() )
                 || samplingUncertainty.quantiles()
                                       .isEmpty()
                 // Identity equals to default
                 || samplingUncertainty.quantiles() == DeclarationFactory.DEFAULT_QUANTILES_RESAMPLING )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( "The 'sampling_uncertainty' declaration does not "
                                                                 + "contain any 'quantiles', which are required. "
                                                                 + "Default values will be used, which are "
                                                                 + DeclarationFactory.DEFAULT_QUANTILES_RESAMPLING
                                                                 + ". If you prefer different quantiles, please "
                                                                 + "declare the quantiles explicitly (e.g.: quantiles: "
                                                                 + "[0.1,0.9])." )
                                               .build();

                events.add( event );
            }
            else if ( samplingUncertainty.quantiles()
                                         .stream()
                                         .anyMatch( n -> n <= 0 || n >= 1.0 ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The 'quantiles' associated with the "
                                                                 + "'sampling_uncertainty' must be greater than 0.0 "
                                                                 + "and less than 1.0, but some of the declared "
                                                                 + "'quantiles' are outside of this range: "
                                                                 + samplingUncertainty.quantiles()
                                                                                      .stream()
                                                                                      .filter( n -> n < -0.0
                                                                                                    || n >= 1.0 )
                                                                                      .toList()
                                                                 + ". Please fix these 'quantiles' and try again." )
                                               .build();
                events.add( event );
            }

            // Warning for small sample size
            if ( samplingUncertainty.sampleSize() < 1000 )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( "The 'sample_size' associated with the "
                                                                 + "'sampling_uncertainty' is "
                                                                 + samplingUncertainty.sampleSize()
                                                                 + ", which is smaller than the recommended minimum of "
                                                                 + "1,000 samples. This may lead to inaccurate "
                                                                 + "estimates of the sampling uncertainty. Please "
                                                                 + "consider using a larger 'sample_size', which will "
                                                                 + "increase the evaluation runtime, but should lead "
                                                                 + "to a more accurate estimate of the sampling "
                                                                 + "uncertainty." )
                                               .build();
                events.add( event );
            }

            // Error for a sample size that is too large
            if ( samplingUncertainty.sampleSize() > 100000 )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The 'sample_size' associated with the "
                                                                 + "'sampling_uncertainty' is larger than the "
                                                                 + "reasonable maximum of 100,000 samples: "
                                                                 + samplingUncertainty.sampleSize()
                                                                 + ". This restriction is imposed because sampling "
                                                                 + "uncertainty estimation is extremely expensive, "
                                                                 + "computationally. Please reduce the 'sample_size' "
                                                                 + "to a reasonable value below 100,000. A suggested "
                                                                 + "maximum is 10,000 samples, which should be more "
                                                                 + "than sufficient to estimate the sampling "
                                                                 + "uncertainty with reasonable accuracy and without "
                                                                 + "using excessive resources." )
                                               .build();
                events.add( event );
            }

            List<EvaluationStatusEvent> crossPair =
                    DeclarationValidator.samplingUncertaintyCrossPairingIsValid( declaration );
            events.addAll( crossPair );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the cross-pairing options are valid when sampling uncertainty is declared.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> samplingUncertaintyCrossPairingIsValid( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();
        if ( Objects.isNull( declaration.crossPair() )
             && DeclarationUtilities.hasBaseline( declaration ) )
        {
            EvaluationStatusEvent crossPair
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( THE_EVALUATION_REQUESTED_THE_SAMPLING_UNCERTAINTY
                                                             + "option and contains a 'baseline' dataset, but does "
                                                             + "not request cross-pairing of the 'predicted' and "
                                                             + "'baseline' datasets ('cross_pair: fuzzy'). This is "
                                                             + "allowed, but can lead to the nominal value of a "
                                                             + "statistic falling outside of its prescribed confidence "
                                                             + "interval because cross-pairing is always performed for "
                                                             + "the resampled data. More generally, comparisons "
                                                             + "between the 'predicted' and 'baseline' datasets (e.g., "
                                                             + "in terms of skill) can be misleading unless both "
                                                             + "datasets contain common event times, which is "
                                                             + "enforced by cross-pairing." )
                                           .build();
            events.add( crossPair );
        }

        // Warn if cross-pairing without options that are consistent with sampling uncertainty
        if ( Objects.nonNull( declaration.crossPair() )
             && ( declaration.crossPair()
                             .method() != CrossPairMethod.FUZZY
                  || declaration.crossPair()
                                .scope() != CrossPairScope.ACROSS_FEATURES )
             && DeclarationUtilities.hasFeatureGroups( declaration )
             && DeclarationUtilities.hasBaseline( declaration ) )
        {
            EvaluationStatusEvent crossPair
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( THE_EVALUATION_REQUESTED_THE_SAMPLING_UNCERTAINTY
                                                             + "option and contains a 'baseline' dataset to "
                                                             + "'cross-pair' with the main pairs across multi-"
                                                             + "feature groups. This combination of options is "
                                                             + "allowed, but can lead to the nominal value of any "
                                                             + "statistics falling outside of the prescribed "
                                                             + "confidence intervals because cross-pairing is "
                                                             + "always performed for the resampled data using "
                                                             + "the 'method: fuzzy' and 'scope: across features', "
                                                             + "which were not declared here. To ensure the confidence "
                                                             + "intervals capture the nominal statistics, please "
                                                             + "consider declaring these two cross-pairing options." )
                                           .build();
            events.add( crossPair );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the threshold service declaration is valid.
     * @param source the threshold source
     * @param evaluationUnit the measurement unit of the evaluation, possibly null
     * @param tuples the features
     * @param hasBaseline whether the evaluation has a baseline dataset
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> thresholdSourceIsValid( ThresholdSource source,
                                                                       String evaluationUnit,
                                                                       Set<GeometryTuple> tuples,
                                                                       boolean hasBaseline )
    {
        if ( Objects.isNull( source ) )
        {
            LOGGER.debug( "There is no threshold source declaration to validate." );
            return Collections.emptyList();
        }

        DatasetOrientation orientation = source.featureNameFrom();

        List<EvaluationStatusEvent> events = new ArrayList<>();

        // If the orientation for service thresholds is 'BASELINE', then a baseline must be present
        if ( orientation == DatasetOrientation.BASELINE && !hasBaseline )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The 'threshold_sources' declaration requested that "
                                                             + "feature names with an orientation of '"
                                                             + DatasetOrientation.BASELINE
                                                             + "' are used to correlate features with thresholds, but "
                                                             + "no 'baseline' dataset was discovered. Please add a "
                                                             + "'baseline' dataset or fix the 'feature_name_from' "
                                                             + "in the 'threshold_sources' declaration." )
                                           .build();

            events.add( event );
        }

        // Baseline orientation and some feature tuples present that are missing a baseline feature?
        if ( orientation == DatasetOrientation.BASELINE
             && tuples.stream()
                      .anyMatch( next -> !next.hasBaseline() ) )
        {
            Set<String> missing = tuples.stream()
                                        .filter( next -> !next.hasBaseline() )
                                        .map( DeclarationFactory.PROTBUF_STRINGIFIER )
                                        .collect( Collectors.toSet() );
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Discovered declaration of 'threshold_sources', which "
                                                             + "requests thresholds whose feature names have an "
                                                             + "orientation of '"
                                                             + DatasetOrientation.BASELINE
                                                             + "'. However, "
                                                             + missing.size()
                                                             + " feature(s) were discovered with a missing '"
                                                             + DatasetOrientation.BASELINE
                                                             + "' feature name. Please fix the 'feature_name_from' in "
                                                             + "the 'threshold_sources' declaration or supply fully "
                                                             + "composed feature tuples with an appropriate feature "
                                                             + "for the '"
                                                             + DatasetOrientation.BASELINE
                                                             + "' dataset. The first ten (or fewer) feature tuples "
                                                             + "with a missing baseline feature were: "
                                                             + missing.stream()
                                                                      .limit( 10 )
                                                                      .collect( Collectors.toSet() ) )
                                           .build();
            events.add( event );
        }

        if ( Objects.isNull( source.unit() )
             && source.type() == ThresholdType.VALUE )
        {

            String extra;

            if ( Objects.nonNull( evaluationUnit ) )
            {
                extra = "the threshold values are assumed to adopt the evaluation unit of '"
                        + evaluationUnit
                        + "'.";
            }
            else
            {
                extra = "the unit will be inferred from the time-series data because no evaluation unit was declared. "
                        + "You may clarify the threshold unit by declaring the 'unit' of the threshold source or the "
                        + "'unit' associated with the evaluation.";
            }

            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "Discovered a source of real-valued thresholds without a "
                                                             + "declared threshold unit. If the threshold unit is "
                                                             + "contained inside the source, this unit will be used. "
                                                             + "If the source does not contain a threshold unit, "
                                                             + extra
                                                             + " The URI of the threshold source is: "
                                                             + source.uri() )
                                           .build();
            events.add( event );
        }

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

        // Check that graphics options are consistent with other declaration
        List<EvaluationStatusEvent> shapes = DeclarationValidator.validateGraphicsFormats( declaration );
        events.addAll( shapes );

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
            Outputs outputs = formats.outputs();

            String start = "The evaluation requested the '";
            String middle =
                    "' format, which has been marked deprecated and may be removed from a future version of the "
                    + "software without warning. It is recommended that you substitute this format with the '";
            String end = "' format.";

            if ( outputs.hasNetcdf() )
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
            Outputs outputs = formats.outputs();

            // Check for some score metrics when netcdf is declared, unless the declaration contains no/default metrics,
            // meaning that metrics will be interpolated, of which some will always be scores
            if ( outputs.hasNetcdf2()
                 && !declaration.metrics()  // Scores will always be interpolated in this case
                                .isEmpty()
                 && declaration.metrics()
                               .stream()
                               .noneMatch( DeclarationValidator::isScore ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "When declaring the 'netcdf2' format "
                                                                 + "option, the evaluation must include at least one "
                                                                 + "score metric because these formats only support "
                                                                 + "the writing of verification scores. Please add a "
                                                                 + "score metric to the declaration or remove the "
                                                                 + "unused format from the list of 'output_formats'. A "
                                                                 + "score is a metric that measures quality with a "
                                                                 + "single number, such as the mean square error." )
                                               .build();
                events.add( event );
            }

            // Warn about netcdf2 when feature groups are declared
            if ( outputs.hasNetcdf2()
                 && Objects.nonNull( declaration.featureGroups() ) )
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
     * Determines whether the metric is a score.
     * @param metric the metric
     * @return whether the metric is a score
     */

    private static boolean isScore( Metric metric )
    {
        // Double scores or timing error metrics with summary statistics
        return metric.name()
                     .isInGroup( MetricConstants.StatisticType.DOUBLE_SCORE )
               || ( metric.name()
                          .isInGroup( MetricConstants.StatisticType.DURATION_DIAGRAM )
                    && Objects.nonNull( metric.parameters() )
                    && !metric.parameters()
                              .summaryStatistics()
                              .isEmpty() );
    }

    /**
     * Validates the graphics declaration.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> validateGraphicsFormats( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Do not allow both legacy netcdf and netcdf2 together
        Formats formats = declaration.formats();
        if ( Objects.nonNull( formats ) )
        {
            Outputs outputs = formats.outputs();
            if ( DeclarationValidator.hasNonPoolingWindowGraphicsShape( outputs )
                 && ( !declaration.validDatePools()
                                  .isEmpty()
                      || !declaration.referenceDatePools()
                                     .isEmpty() ) )
            {
                String scope = DeclarationValidator.getScopeOfPoolingDeclaration( declaration );

                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The 'output_formats' includes one or more graphics "
                                                                 + "formats with an 'orientation' that is inconsistent "
                                                                 + "with the pooling declaration, which contains "
                                                                 + scope
                                                                 + ". Please correct the 'orientation' of each "
                                                                 + "graphics format or correct the pooling declaration "
                                                                 + "and try again. " )
                                               .build();
                events.add( event );
            }
        }

        List<EvaluationStatusEvent> combinedGraphics = DeclarationValidator.validateCombinedGraphics( declaration );

        events.addAll( combinedGraphics );

        return Collections.unmodifiableList( events );
    }

    /**
     * Validates the combined graphics option.
     * @param declaration the declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> validateCombinedGraphics( EvaluationDeclaration declaration )
    {
        if ( Boolean.FALSE.equals( declaration.combinedGraphics() ) )
        {
            LOGGER.debug( "When validating the declaration for graphics options, the 'combined_graphics' option was "
                          + "not detected." );
            return List.of();
        }

        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Graphics formats declared?
        if ( !DeclarationUtilities.hasGraphicsFormats( declaration ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "The declaration includes 'combined_graphics', but the "
                                                             + "'output_formats' do not contain any graphics formats "
                                                             + "to write. The 'combined_graphics' option is, "
                                                             + "therefore, redundant and will be ignored. If this is "
                                                             + "not intended, please add one or more graphics formats "
                                                             + "to the 'output_formats' and try again." )
                                           .build();
            events.add( event );
        }

        // Baseline declared?
        if ( !DeclarationUtilities.hasBaseline( declaration ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "The declaration includes 'combined_graphics', but does "
                                                             + "not include a 'baseline'. The 'combined_graphics' "
                                                             + "option is, therefore, redundant and will be ignored. "
                                                             + "If this is not intended, please add a 'baseline' with "
                                                             + "'separate_metrics: true' and try again." )
                                           .build();
            events.add( event );
        }
        // Separate metrics declared?
        else if ( Boolean.FALSE.equals( declaration.baseline()
                                                   .separateMetrics() ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "The declaration includes 'combined_graphics', but does "
                                                             + "not include a 'baseline' with "
                                                             + "'separate_metrics: true'. The 'combined_graphics' "
                                                             + "option is, therefore, redundant and will be ignored. "
                                                             + "If this is not intended, please add "
                                                             + "'separate_metrics: true' to the 'baseline' and try "
                                                             + AGAIN )
                                           .build();
            events.add( event );
        }

        // Metrics that do not support combined graphics?
        Set<MetricConstants> metrics =
                declaration.metrics()
                           .stream()
                           .map( Metric::name )
                           .filter( m -> m.isInGroup( MetricConstants.StatisticType.PAIRS )
                                         || m.isInGroup( MetricConstants.StatisticType.BOXPLOT_PER_POOL )
                                         || m.isInGroup( MetricConstants.StatisticType.BOXPLOT_PER_PAIR ) )
                           .collect( Collectors.toSet() );

        if ( !metrics.isEmpty() )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( "The declaration includes 'combined_graphics', but "
                                                             + "one or more of the declared 'metrics' do not support "
                                                             + "combined graphics. When generating plots for these "
                                                             + "metrics, the graphics will not be combined. The "
                                                             + "following metrics do not support combined graphics: "
                                                             + metrics
                                                             + "." )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * @param outputs the outputs to test
     * @return whether there is a non-pooling window graphics shape declared
     */
    private static boolean hasNonPoolingWindowGraphicsShape( Outputs outputs )
    {
        Outputs.GraphicFormat.GraphicShape shape = null;

        if ( outputs.hasPng()
             && outputs.getPng()
                       .hasOptions() )
        {
            shape = outputs.getPng()
                           .getOptions()
                           .getShape();
        }

        if ( outputs.hasSvg()
             && outputs.getSvg()
                       .hasOptions() )
        {
            shape = outputs.getSvg()
                           .getOptions()
                           .getShape();
        }

        return Objects.nonNull( shape )
               && shape != Outputs.GraphicFormat.GraphicShape.DEFAULT
               && shape != Outputs.GraphicFormat.GraphicShape.ISSUED_DATE_POOLS
               && shape != Outputs.GraphicFormat.GraphicShape.VALID_DATE_POOLS;
    }

    /**
     * @param declaration the declaration
     * @return a message indicating the scope of the pooling declaration
     */
    private static String getScopeOfPoolingDeclaration( EvaluationDeclaration declaration )
    {
        if ( !declaration.validDatePools()
                         .isEmpty()
             && !declaration.referenceDatePools()
                            .isEmpty() )
        {
            return "'valid_date_pools and reference_date_pools'";
        }
        else if ( !declaration.validDatePools()
                              .isEmpty() )
        {
            return "'valid_date_pools'";
        }
        else
        {
            return "'reference_date_pools'";
        }
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
                                                  .filter( MetricConstants::isExplicitBaselineRequired )
                                                  .collect( Collectors.toSet() );

        if ( !DeclarationUtilities.hasBaseline( declaration )
             && !metrics.isEmpty() )

        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The declaration includes metrics that require an "
                                                             + "explicit 'baseline' dataset, but no 'baseline' dataset "
                                                             + "was found. Please remove the following metrics from "
                                                             + "the declaration or add a 'baseline' dataset and try "
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
                                                                 + "'thresholds' or 'probability_thresholds' "
                                                                 + "but none were found. Please remove the following "
                                                                 + "metrics or add the required thresholds and try "
                                                                 + "again: "
                                                                 + metrics
                                                                 + "." )
                                               .build();
                events.add( event );
            }

            // Ensembles, but no decision/classifier thresholds: warn
            if ( declaration.right()
                            .type() == DataType.ENSEMBLE_FORECASTS
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
     * Checks that all metric parameters are consistent with corresponding top-level declaration.
     * @param declaration the declaration
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> checkMetricParametersAreConsistent( EvaluationDeclaration declaration )
    {
        // Ensemble average type
        List<EvaluationStatusEvent> ensembleAverageType =
                DeclarationValidator.checkEnsembleAverageTypeIsConsistent( declaration );
        List<EvaluationStatusEvent> events = new ArrayList<>( ensembleAverageType );

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the ensemble average type is declared consistently.
     * @param declaration the declaration
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> checkEnsembleAverageTypeIsConsistent( EvaluationDeclaration declaration )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();
        wres.statistics.generated.Pool.EnsembleAverageType topType = declaration.ensembleAverageType();
        if ( Objects.nonNull( topType ) )
        {
            Set<MetricConstants> warnAboutMe = new HashSet<>();
            for ( Metric next : declaration.metrics() )
            {
                MetricParameters pars = next.parameters();
                if ( Objects.nonNull( pars )
                     && Objects.nonNull( pars.ensembleAverageType() )
                     && pars.ensembleAverageType() != topType )
                {
                    warnAboutMe.add( next.name() );
                }
            }

            // Warn because some low-level metric parameters have an ensemble average type that conflicts with the top-
            // level type. This is allowed (e.g., the top-level type could be used to fill in the gaps), but is
            // potentially unintended.
            if ( !warnAboutMe.isEmpty() )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( "The declaration included an ensemble average type of "
                                                                 + "'"
                                                                 + topType
                                                                 + "' for all metrics, but also included some metrics "
                                                                 + "with their own ensemble average type that differs "
                                                                 + "from the overall type. This is allowed, but all of "
                                                                 + "the metrics that declare their own type will "
                                                                 + "retain that type. The following metrics will not "
                                                                 + "be adjusted: "
                                                                 + warnAboutMe
                                                                 + "." )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
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
            String end = " Please add some geospatial features to the declaration (e.g., 'features', 'feature_groups' "
                         + "or 'feature_service') and try again.";

            // Web services require features
            if ( DeclarationValidator.hasWebSources( declaration.left() ) )
            {
                eventBuilder.setEventMessage( start + OBSERVED + middle + end );
                EvaluationStatusEvent event = eventBuilder.build();
                events.add( event );
            }
            if ( DeclarationValidator.hasWebSources( declaration.right() ) )
            {
                eventBuilder.setEventMessage( start + PREDICTED + middle + end );
                EvaluationStatusEvent event = eventBuilder.build();
                events.add( event );
            }
            if ( DeclarationUtilities.hasBaseline( declaration )
                 && DeclarationValidator.hasWebSources( declaration.baseline()
                                                                   .dataset() ) )
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

        if ( Objects.nonNull( declaration.left() ) )
        {
            Set<SourceInterface> leftRequireFeatures =
                    DeclarationValidator.getNwmSourceInterfaces( declaration.left()
                                                                            .sources() );
            if ( !leftRequireFeatures.isEmpty() )
            {
                eventBuilder.setEventMessage( start + OBSERVED + middle + leftRequireFeatures + end );
                EvaluationStatusEvent event = eventBuilder.build();
                events.add( event );
            }
        }

        if ( Objects.nonNull( declaration.right() ) )
        {
            Set<SourceInterface> rightRequireFeatures =
                    DeclarationValidator.getNwmSourceInterfaces( declaration.right()
                                                                            .sources() );
            if ( !rightRequireFeatures.isEmpty() )
            {
                eventBuilder.setEventMessage( start + PREDICTED + middle + rightRequireFeatures + end );
                EvaluationStatusEvent event = eventBuilder.build();
                events.add( event );
            }
        }

        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            Set<SourceInterface> baselineRequireFeatures =
                    DeclarationValidator.getNwmSourceInterfaces( declaration.baseline()
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
        if ( !DeclarationUtilities.hasBaseline( declaration ) )
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
                                                                 + "features for a 'baseline' dataset but no "
                                                                 + "'baseline' dataset is defined. Please add a "
                                                                 + "'baseline' dataset or remove the baseline "
                                                                 + "'features' and try again." )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that a feature service is defined when there are sparse features declared and each side of data contains
     * different feature authorities.
     *
     * @param evaluation the evaluation
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> checkFeatureServicePresentIfRequired( EvaluationDeclaration evaluation )
    {
        // Feature service already defined? If so, no problem
        if ( Objects.nonNull( evaluation.featureService() ) )
        {
            LOGGER.debug( "Discovered a feature service, which will resolve feature correlations when required." );
            return Collections.emptyList();
        }

        // Are there sparse features?
        Set<GeometryTuple> sparse = DeclarationInterpolator.getSparseFeaturesToInterpolate( evaluation );
        if ( sparse.isEmpty() )
        {
            LOGGER.debug( "Discovered no sparse features to interpolate." );
            return Collections.emptyList();
        }

        // Are there different feature authorities on each side of data?
        Set<FeatureAuthority> leftAuthorities = DeclarationUtilities.getFeatureAuthorities( evaluation.left() );
        Set<FeatureAuthority> rightAuthorities = DeclarationUtilities.getFeatureAuthorities( evaluation.right() );
        Set<FeatureAuthority> baselineAuthorities = null;

        String baselineStatement = "";
        if ( DeclarationUtilities.hasBaseline( evaluation ) )
        {
            baselineAuthorities = DeclarationUtilities.getFeatureAuthorities( evaluation.baseline()
                                                                                        .dataset() );
            baselineStatement = " The feature authorities detected for the 'baseline' data were '"
                                + baselineAuthorities
                                + "'.";
        }

        if ( Objects.equals( leftAuthorities, rightAuthorities )
             && ( !DeclarationUtilities.hasBaseline( evaluation ) || Objects.equals( leftAuthorities,
                                                                                     baselineAuthorities ) ) )
        {
            LOGGER.debug( "Discovered the same feature authorities for all datasets." );
            return Collections.emptyList();
        }

        // Feature authorities differ across each side of data and there is no feature service to correlate the sparse
        // features, which is not allowed
        EvaluationStatusEvent event
                = EvaluationStatusEvent.newBuilder()
                                       .setStatusLevel( StatusLevel.ERROR )
                                       .setEventMessage( "The declaration contains "
                                                         + sparse.size()
                                                         + " geospatial "
                                                         + "feature tuples that are declared sparsely (i.e., where at "
                                                         + "least one side of the tuple is missing), but different "
                                                         + "feature authorities were detected for each side of data "
                                                         + "and no feature service was declared to resolve the "
                                                         + "feature correlations. Please add a feature service to "
                                                         + "allow the feature correlations to be determined or fully "
                                                         + "declare every feature tuple in the evaluation. The "
                                                         + "feature authorities detected for the 'observed' data "
                                                         + "were: '"
                                                         + leftAuthorities
                                                         + "'. The feature authorities detected for the 'predicted' "
                                                         + "data were '"
                                                         + rightAuthorities
                                                         + "'."
                                                         + baselineStatement )
                                       .build();

        return List.of( event );
    }

    /**
     * Checks for the consistency of the covariate feature authorities.
     * @param evaluation the evaluation declaration
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> checkCovariateFeatureAuthorities( EvaluationDeclaration evaluation )
    {
        // When defined, covariate authorities must all be consistent with one of the prescribed data orientations
        if ( evaluation.covariates()
                       .isEmpty() )
        {
            LOGGER.debug( "Not checking the feature authorities of the covariate datasets because no covariate "
                          + "datasets were declared." );
            return List.of();
        }

        Set<FeatureAuthority> featureAuthorities = new HashSet<>();

        if ( Objects.nonNull( evaluation.left() ) )
        {
            Set<FeatureAuthority> left = DeclarationUtilities.getFeatureAuthorities( evaluation.left() );
            featureAuthorities.addAll( left );
        }

        if ( Objects.nonNull( evaluation.right() ) )
        {
            Set<FeatureAuthority> right = DeclarationUtilities.getFeatureAuthorities( evaluation.right() );
            featureAuthorities.addAll( right );
        }

        if ( DeclarationUtilities.hasBaseline( evaluation ) )
        {
            Set<FeatureAuthority> baseline = DeclarationUtilities.getFeatureAuthorities( evaluation.baseline()
                                                                                                   .dataset() );
            featureAuthorities.addAll( baseline );
        }

        // Check the covariates
        Set<FeatureAuthority> missing = evaluation.covariates()
                                                  .stream()
                                                  .flatMap( d -> DeclarationUtilities.getFeatureAuthorities( d.dataset() )
                                                                                     .stream() )
                                                  .collect( Collectors.toCollection( TreeSet::new ) );
        missing.removeAll( featureAuthorities );

        List<EvaluationStatusEvent> events = new ArrayList<>();
        if ( !missing.isEmpty() )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "Discovered one or more covariates whose feature "
                                                             + "authorities were declared explicitly, but did not "
                                                             + "match a feature authority associated with any other "
                                                             + "dataset ('observed', 'predicted' or, where applicable, "
                                                             + "'baseline'). Each 'covariate' dataset must have the "
                                                             + "same feature authority as one of these other datasets. "
                                                             + "Please fix the declaration or use a 'covariate' "
                                                             + "dataset that has the same feature authority as an "
                                                             + "existing dataset. The unrecognized feature authorities "
                                                             + "associated with 'covariate' datasets were: "
                                                             + missing
                                                             + "." )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that featureful thresholds are correlated with declared features.
     * @param declaration the declaration
     * @return any validation events encountered
     */

    private static List<EvaluationStatusEvent> validateFeaturefulThresholds( EvaluationDeclaration declaration )
    {
        Set<GeometryTuple> features = DeclarationUtilities.getFeatures( declaration );
        Set<Threshold> thresholds = DeclarationUtilities.getInbandThresholds( declaration );

        // Are there featureful thresholds?
        Set<Threshold> featureful = thresholds.stream()
                                              .filter( n -> Objects.nonNull( n.feature() ) )
                                              .collect( Collectors.toSet() );
        if ( featureful.isEmpty() )
        {
            LOGGER.debug( "No featureful thresholds were available to validate." );
            return List.of();
        }

        // Must be lenient if there are no features, because they can be obtained on ingest
        if ( features.isEmpty() )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( THE_EVALUATION_DECLARED
                                                             + featureful.size()
                                                             + "thresholds with explicit features, but no "
                                                             + "features to evaluate. An attempt will be made to "
                                                             + "identify features from relevant data sources or "
                                                             + "services, but the featureful thresholds cannot be "
                                                             + "validated until then." )
                                           .build();
            return List.of( event );
        }

        List<EvaluationStatusEvent> leftEvents =
                DeclarationValidator.validateFeaturefulThresholds( featureful,
                                                                   features,
                                                                   DatasetOrientation.LEFT );
        List<EvaluationStatusEvent> events = new ArrayList<>( leftEvents );
        List<EvaluationStatusEvent> rightEvents =
                DeclarationValidator.validateFeaturefulThresholds( featureful,
                                                                   features,
                                                                   DatasetOrientation.RIGHT );
        events.addAll( rightEvents );

        if ( DeclarationUtilities.hasBaseline( declaration ) )
        {
            List<EvaluationStatusEvent> baselineEvents =
                    DeclarationValidator.validateFeaturefulThresholds( featureful,
                                                                       features,
                                                                       DatasetOrientation.BASELINE );
            events.addAll( baselineEvents );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the featureful thresholds for the supplied orientation can be correlated with features.
     * @param thresholds the thresholds to correlate
     * @param features the features to correlate
     * @param orientation the orientation of the feature names
     * @return any validation events encountered
     */

    private static List<EvaluationStatusEvent> validateFeaturefulThresholds( Set<Threshold> thresholds,
                                                                             Set<GeometryTuple> features,
                                                                             DatasetOrientation orientation )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( thresholds.isEmpty() )
        {
            LOGGER.debug( "No featureful thresholds to validate" );
            return List.of();
        }

        // Explicit features and featureful thresholds. Some featureful thresholds must be correlated with features
        Set<String> thresholdFeatureNames =
                thresholds.stream()
                          .filter( n -> n.featureNameFrom() == orientation )
                          // Ignore all data, which was added automagically
                          .filter( n -> !DeclarationUtilities.ALL_DATA_THRESHOLD.threshold()
                                                                                .equals( n.threshold() ) )
                          .map( Threshold::feature )
                          .map( wres.statistics.generated.Geometry::getName )
                          .collect( Collectors.toSet() );

        if ( thresholdFeatureNames.isEmpty() )
        {
            LOGGER.debug( "No featureful thresholds to validate with an {} orientation.", orientation );
            return List.of();
        }

        Set<String> names = DeclarationUtilities.getFeatureNamesFor( features, orientation );

        int before = thresholdFeatureNames.size();
        // Any threshold feature names without corresponding feature names?
        thresholdFeatureNames.removeAll( names );
        int after = thresholdFeatureNames.size();

        // Some must match
        if ( before == after )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( THE_EVALUATION_DECLARED
                                                             + before
                                                             + " thresholds with features whose "
                                                             + "'feature_name_from' is '"
                                                             + orientation
                                                             + "', but "
                                                             + after
                                                             + " of the features associated with these thresholds "
                                                             + "had no corresponding feature to evaluate anywhere in "
                                                             + "the declaration (e.g., 'features', 'feature_groups'). "
                                                             + "Please remove the thresholds for these features, add "
                                                             + "the corresponding features or fix the threshold "
                                                             + "declaration. The missing features are: "
                                                             + thresholdFeatureNames
                                                             + "." )
                                           .build();
            events.add( event );
        }
        // Some matched. some did not: warn
        else if ( !thresholdFeatureNames.isEmpty() )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.WARN )
                                           .setEventMessage( THE_EVALUATION_DECLARED
                                                             + before
                                                             + " thresholds with features whose "
                                                             + "'feature_name_from' is '"
                                                             + orientation
                                                             + "', but "
                                                             + after
                                                             + " of the features associated with these thresholds "
                                                             + "had no corresponding feature to evaluate anywhere in "
                                                             + "the declaration (e.g., 'features', 'feature_groups'). "
                                                             + "Please remove the thresholds for these features, add "
                                                             + "the corresponding features or fix the threshold "
                                                             + "declaration. The missing features are: "
                                                             + thresholdFeatureNames
                                                             + "." )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * @param sources the sources to search
     * @return a set of source interfaces that begin with the specified characters
     */

    private static Set<SourceInterface> getNwmSourceInterfaces( List<Source> sources )
    {
        return sources.stream()
                      .map( Source::sourceInterface )
                      .filter( next -> Objects.nonNull( next )
                                       && next.isNwmInterface() )
                      .collect( Collectors.toSet() );
    }

    /**
     * Checks that the time pools are valid.
     * @param timePools the time pools
     * @param interval interval the interval
     * @param poolName the pool name to help with messaging
     * @param intervalName the interval name to help with messaging
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> timePoolsAreValid( Set<TimePools> timePools,
                                                                  TimeInterval interval,
                                                                  String poolName,
                                                                  String intervalName )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        for ( TimePools pools : timePools )
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
                Duration period = pools.period();
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
     * @param timePools the time pools
     * @param interval interval the interval
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> leadTimePoolsAreValid( Set<TimePools> timePools,
                                                                      LeadTimeInterval interval )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        for ( TimePools pool : timePools )
        {
            // Time interval must be fully declared
            if ( Objects.isNull( interval )
                 || Objects.isNull( interval.minimum() )
                 || Objects.isNull( interval.maximum() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The declaration included 'lead_time_pools', which "
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
                Duration period = pool.period();
                Duration start = interval.minimum();
                Duration end = interval.maximum();

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
     *
     * @param timeInterval the time interval
     * @param context the context for the interval to help with messaging
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> timeIntervalIsValid( TimeInterval timeInterval, String context )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( timeInterval )
             && Objects.nonNull( timeInterval.minimum() )
             && Objects.nonNull( timeInterval.maximum() ) )
        {
            if ( timeInterval.maximum()
                             .isBefore( timeInterval.minimum() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( "The "
                                                                 + context
                                                                 + " interval is invalid because the 'minimum' value is "
                                                                 + "greater than or equal to the 'maximum' value. Please "
                                                                 + "adjust the 'minimum' to occur before the 'maximum' and "
                                                                 + "try " + AGAIN )
                                               .build();
                events.add( event );
            }
            else if ( Objects.equals( timeInterval.minimum(), timeInterval.maximum() ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( "The "
                                                                 + context
                                                                 + " interval is suspicious because the 'minimum' "
                                                                 + "value is equal to the 'maximum' value. If a "
                                                                 + "zero-wide interval was intended, no action is "
                                                                 + "required." )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the analysis durations are valid.
     * @param analysisTimes the analysis durations
     * @return the validation events encountered
     */

    private static List<EvaluationStatusEvent> analysisTimesAreValid( AnalysisTimes analysisTimes )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.nonNull( analysisTimes )
             && Objects.nonNull( analysisTimes.minimum() )
             && Objects.nonNull( analysisTimes.maximum() )
             && ( analysisTimes.maximum()
                               .compareTo( analysisTimes.minimum() ) < 0 ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The 'analysis_times' interval is invalid because the "
                                                             + "'maximum' value of '"
                                                             + analysisTimes.maximum()
                                                             + "' is less than the 'minimum' "
                                                             + "value of '"
                                                             + analysisTimes.minimum()
                                                             + "'. Please adjust the analysis times to form a "
                                                             + "valid time interval and try again." )
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
                                                                                                  null,
                                                                                                  true );
            events.addAll( evaluationEvents );

            wres.statistics.generated.TimeScale timeScaleInner = timeScale.timeScale();

            // Cannot be instantaneous
            if ( DeclarationValidator.isInstantaneous( timeScaleInner ) )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.WARN )
                                               .setEventMessage( "The evaluation 'time_scale' is instantaneous. If the "
                                                                 + "datasets all contain instantaneous values, this "
                                                                 + "declaration is redundant and should be removed. "
                                                                 + "Otherwise, the declaration is invalid because "
                                                                 + "the smallest possible 'time_scale' is "
                                                                 + "instantaneous and downscaling is not supported." )
                                               .build();
                events.add( event );
            }
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that the dataset timescale is consistent with the evaluation timescale.
     * @param sourceScale the source timescale
     * @param desiredScale the desired timescale
     * @param orientation the dataset context to help with messaging
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> datasetTimeScaleConsistentWithDesiredTimeScale( TimeScale sourceScale,
                                                                                               TimeScale desiredScale,
                                                                                               DatasetOrientation orientation )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        if ( Objects.isNull( sourceScale )
             || Objects.isNull( desiredScale ) )
        {
            LOGGER.debug( "Not checking the consistency of the dataset time scale and the evaluation time scale for "
                          + "the {} dataset because one or both of the time scales were missing.", orientation );

            return Collections.emptyList();
        }

        wres.statistics.generated.TimeScale sourceScaleInner = sourceScale.timeScale();
        wres.statistics.generated.TimeScale evaluationScaleInner = desiredScale.timeScale();

        String orientationString = DeclarationValidator.getTimeScaleOrientationString( orientation, false );

        // If the desired scale is a sum, the existing scale must be instantaneous or the function must be a sum or mean
        if ( desiredScale.timeScale()
                         .getFunction() == TimeScaleFunction.TOTAL
             && !DeclarationValidator.isInstantaneous( sourceScaleInner )
             && sourceScaleInner.getFunction() != TimeScaleFunction.MEAN
             && sourceScaleInner.getFunction() != TimeScaleFunction.TOTAL )
        {
            String extra = "";

            if ( orientation == DatasetOrientation.COVARIATE )
            {
                extra = "and/or the 'rescale_function' associated with each covariate dataset";
            }

            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( "The evaluation 'time_scale' requires a total, but the "
                                                             + "time scale associated with "
                                                             + orientationString
                                                             + " dataset does not have a supported time scale "
                                                             + "function from which to compute this total. Please "
                                                             + "check the evaluation 'time_scale' "
                                                             + extra
                                                             + " and try again." )
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
            if ( evaluationDuration.compareTo( sourceDuration ) < 0 )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH
                                                                 + orientationString
                                                                 + " dataset is smaller than the evaluation "
                                                                 + "'time_scale', which is not allowed. Please "
                                                                 + "increase the evaluation 'time_scale' and try "
                                                                 + AGAIN )
                                               .build();
                events.add( event );
            }

            // No upscaling unless the period is an integer multiple
            if ( evaluationDuration.toMillis() % sourceDuration.toMillis() != 0 )
            {
                EvaluationStatusEvent event
                        = EvaluationStatusEvent.newBuilder()
                                               .setStatusLevel( StatusLevel.ERROR )
                                               .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH
                                                                 + orientationString
                                                                 + " dataset is not exactly divisible by the "
                                                                 + "evaluation 'time_scale', which is not allowed. "
                                                                 + "Please change the evaluation 'time_scale' to an "
                                                                 + "integer multiple of every source time scale." )
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

            return duration.compareTo( INSTANTANEOUS_DURATION ) <= 0;
        }

        return timeScale.getStartDay() == timeScale.getEndDay()
               && timeScale.getStartMonth() == timeScale.getEndMonth();
    }

    /**
     * Checks that the timescale is valid.
     * @param timeScale the timescale
     * @param orientation the orientation of the timescale to provide context in any error message
     * @param isEvaluationTimeScale whether the timescale is the evaluation timescale and not a dataset timescale
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> timeScaleIsValid( TimeScale timeScale,
                                                                 DatasetOrientation orientation,
                                                                 boolean isEvaluationTimeScale )
    {
        List<EvaluationStatusEvent> events = new ArrayList<>();

        // Timescale is present?
        if ( Objects.isNull( timeScale ) )
        {
            LOGGER.debug( "Encountered a missing time scale with orientation {}.", orientation );
            return Collections.emptyList();
        }

        LOGGER.debug( "Discovered a timescale to validate for {}: {}.", orientation, timeScale );
        wres.statistics.generated.TimeScale timeScaleInner = timeScale.timeScale();

        String orientationString = DeclarationValidator.getTimeScaleOrientationString( orientation,
                                                                                       isEvaluationTimeScale );

        // Function must be present
        if ( timeScaleInner.getFunction() == wres.statistics.generated.TimeScale.TimeScaleFunction.UNKNOWN )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH
                                                             + orientationString
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
                                           .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH
                                                             + orientationString
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
                                           .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH
                                                             + orientationString
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
                                           .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH
                                                             + orientationString
                                                             + " is not properly declared. When including either "
                                                             + "a 'maximum_day' or a 'maximum_month', both must be "
                                                             + "present." )
                                           .build();
            events.add( event );
        }

        // If the season declaration is incomplete, then a period must be present
        if ( !timeScaleInner.hasPeriod() && ( timeScaleInner.getStartDay() == 0
                                              || timeScaleInner.getEndDay() == 0
                                              || timeScaleInner.getStartMonth() == 0
                                              || timeScaleInner.getEndMonth() == 0 ) )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH
                                                             + orientationString
                                                             + " is not properly declared. The time scale 'period' "
                                                             + "must be declared explicitly or a time scale season "
                                                             + "fully defined, else a valid combination of the two." )
                                           .build();
            events.add( event );
        }

        // If the season is fully defined, the period cannot be defined
        if ( timeScaleInner.hasPeriod()
             && Math.min( timeScaleInner.getStartDay(), timeScaleInner.getStartMonth() ) > 0
             && Math.max( timeScaleInner.getEndDay(), timeScaleInner.getEndMonth() ) > 0 )
        {
            EvaluationStatusEvent event
                    = EvaluationStatusEvent.newBuilder()
                                           .setStatusLevel( StatusLevel.ERROR )
                                           .setEventMessage( THE_TIME_SCALE_ASSOCIATED_WITH
                                                             + orientationString
                                                             + " is not properly declared. The 'period' cannot be "
                                                             + "declared alongside a fully defined season. Please "
                                                             + "remove the 'period' and 'unit' or remove the time "
                                                             + "scale season and try again." )
                                           .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Returns a qualifying string for the timescale orientation.
     * @param orientation the dataset orientation
     * @param isEvaluationTimeScale whether the timescale is an evaluation timescale
     * @return the timescale orientation string
     */
    private static String getTimeScaleOrientationString( DatasetOrientation orientation,
                                                         boolean isEvaluationTimeScale )
    {
        String orientationString;
        if ( isEvaluationTimeScale )
        {
            orientationString = "'evaluation'";
        }
        else
        {
            orientationString = "'" + orientation + "'";
        }

        String article = "the ";
        if ( orientation == DatasetOrientation.COVARIATE )
        {
            article = "a ";
        }

        return article + orientationString;
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
                                                                           DatasetOrientation orientation )
    {
        EvaluationStatusEvent.Builder eventBuilder =
                EvaluationStatusEvent.newBuilder()
                                     .setStatusLevel( StatusLevel.ERROR );

        List<EvaluationStatusEvent> events = new ArrayList<>();

        String messageStart = "Discovered '";
        String messageStartOuter = "' data sources that have a data 'type' of '";
        String messageMiddleInner = "' and use web services, but the ";
        String messageMiddleOuter =
                " were incomplete or undefined, which is not allowed. Please declare a complete " + "pair of ";
        String messageMiddleFinal = " when acquiring ";
        String messageEnd = " data from web services.";

        // The web service is for a generated baseline and a valid interval is defined
        if ( orientation == DatasetOrientation.BASELINE
             && DeclarationUtilities.hasGeneratedBaseline( declaration.baseline() )
             && declaration.baseline()
                           .generatedBaseline()
                           .method() == GeneratedBaselines.CLIMATOLOGY
             && Objects.nonNull( declaration.baseline()
                                            .generatedBaseline()
                                            .minimumDate() )
             && Objects.nonNull( declaration.baseline()
                                            .generatedBaseline()
                                            .maximumDate() ) )
        {
            LOGGER.debug( "Discovered a generated baseline with a web service and both the 'minimum_date' and the "
                          + "'maximum_date' were defined." );
        }
        // Unknown type with both reference times and valid times incomplete
        else if ( Objects.isNull( type )
                  && DeclarationValidator.isTimeIntervalIncomplete( declaration.referenceDates() )
                  && DeclarationValidator.isTimeIntervalIncomplete( declaration.validDates() ) )
        {
            EvaluationStatusEvent event = eventBuilder.setEventMessage( messageStart
                                                                        + orientation
                                                                        + "' data sources that have no declared data "
                                                                        + "'type"
                                                                        + messageMiddleInner
                                                                        + REFERENCE_DATES
                                                                        + " and/or "
                                                                        + VALID_DATES
                                                                        + messageMiddleOuter
                                                                        + REFERENCE_DATES
                                                                        + " or "
                                                                        + VALID_DATES
                                                                        + messageMiddleFinal
                                                                        + "time-series"
                                                                        + messageEnd )
                                                      .build();
            events.add( event );
        }
        // Forecasts with incomplete reference times
        else if ( Objects.nonNull( type )
                  && type.isForecastType()
                  && DeclarationValidator.isTimeIntervalIncomplete( declaration.referenceDates() ) )
        {
            EvaluationStatusEvent event = eventBuilder.setEventMessage( messageStart
                                                                        + orientation
                                                                        + messageStartOuter
                                                                        + type
                                                                        + messageMiddleInner
                                                                        + REFERENCE_DATES
                                                                        + messageMiddleOuter
                                                                        + REFERENCE_DATES
                                                                        + messageMiddleFinal
                                                                        + type
                                                                        + messageEnd )
                                                      .build();
            events.add( event );
        }
        // Non-forecasts with incomplete valid times
        else if ( Objects.nonNull( type )
                  && !type.isForecastType()
                  && DeclarationValidator.isTimeIntervalIncomplete( declaration.validDates() ) )
        {
            EvaluationStatusEvent event = eventBuilder.setEventMessage( messageStart
                                                                        + orientation
                                                                        + messageStartOuter
                                                                        + type
                                                                        + messageMiddleInner
                                                                        + VALID_DATES
                                                                        + messageMiddleOuter
                                                                        + VALID_DATES
                                                                        + messageMiddleFinal
                                                                        + type + messageEnd )
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

        String article = "the";
        if ( orientation.equals( COVARIATE ) )
        {
            article = "a";
        }

        // No sources? Not allowed.
        if ( Objects.isNull( sources )
             || sources.isEmpty() )
        {
            EvaluationStatusEvent event =
                    EvaluationStatusEvent.newBuilder()
                                         .setStatusLevel( StatusLevel.ERROR )
                                         .setEventMessage( "No data sources were declared for "
                                                           + article
                                                           + " '"
                                                           + orientation
                                                           + "' dataset, which is not allowed. Please declare at least "
                                                           + "one data source for the '"
                                                           + orientation
                                                           + "' dataset and try again." )
                                         .build();
            events.add( event );
        }
        else
        {
            List<EvaluationStatusEvent> sourceEvents = DeclarationValidator.sourcesAreValid( sources,
                                                                                             type,
                                                                                             orientation,
                                                                                             article );
            events.addAll( sourceEvents );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * Checks that all declared sources are valid.
     * @param sources the source to validate
     * @param type the data type
     * @param orientation the orientation of the sources
     * @param article the article qualifier for the dataset orientation
     * @return the validation events encountered
     */
    private static List<EvaluationStatusEvent> sourcesAreValid( List<Source> sources,
                                                                DataType type,
                                                                String orientation,
                                                                String article )
    {
        int index = 1;
        List<Integer> invalid = new ArrayList<>();
        Set<EvaluationStatusEvent> oneOf = new HashSet<>();
        for ( Source source : sources )
        {
            // Identify any source whose URI failed to deserialize
            // Cannot provide full context here, but this is a compromise between validating at schema validation
            // time or post deserialization, given that a URI is deserialized to a type that performs instant
            // validation and exits exceptionally for invalid URIs. The compromise is to return a null URI in that
            // case and report the error here. See #126974
            if ( Objects.isNull( source.uri() ) )
            {
                invalid.add( index );
            }

            // Warn about time zone offset
            if ( Objects.nonNull( source.timeZoneOffset() ) )
            {
                EvaluationStatusEvent event =
                        EvaluationStatusEvent.newBuilder()
                                             .setStatusLevel( StatusLevel.WARN )
                                             .setEventMessage( DISCOVERED_ONE_OR_MORE
                                                               + orientation
                                                               + "' data sources for which a 'time_zone_offset' "
                                                               + "was declared. This information is generally not "
                                                               + "required and will be ignored if the data source "
                                                               + "itself contains a time zone offset." )
                                             .build();
                oneOf.add( event );
            }

            // Warn about the use of the http/s scheme for sources that declare an nwm interface. The cdms3 scheme
            // may be much quicker as this leverages the S3 or GCS APIs to make byte-range requests: see GitHub #75
            if ( Objects.nonNull( source.uri() )
                 && DeclarationValidator.isWebSource( source )
                 && Objects.nonNull( source.sourceInterface() )
                 && source.sourceInterface()
                          .isNwmInterface() )
            {
                EvaluationStatusEvent event =
                        EvaluationStatusEvent.newBuilder()
                                             .setStatusLevel( StatusLevel.WARN )
                                             .setEventMessage( DISCOVERED_ONE_OR_MORE
                                                               + orientation
                                                               + "' data sources from a web source with an interface "
                                                               + "of '"
                                                               + source.sourceInterface()
                                                               + "', which uses the 'http' scheme. If this source "
                                                               + "originates from a web service that implements the S3 "
                                                               + "or Google Cloud Services (GCS) interfaces, then "
                                                               + "you should declare this source with the 'cdms3' "
                                                               + "scheme, rather than the 'http' scheme, as this "
                                                               + "should be much more performant." )
                                             .build();
                oneOf.add( event );
            }

            // Warn about unit
            if ( Objects.nonNull( source.unit() ) )
            {
                EvaluationStatusEvent event =
                        EvaluationStatusEvent.newBuilder()
                                             .setStatusLevel( StatusLevel.WARN )
                                             .setEventMessage( DISCOVERED_ONE_OR_MORE
                                                               + orientation
                                                               + "' data sources for which a 'unit' was declared. "
                                                               + "This information is generally not required and "
                                                               + "will be ignored if the data source itself "
                                                               + "contains a measurement unit." )
                                             .build();
                oneOf.add( event );
            }

            index++;
        }

        // Add the singleton instances
        List<EvaluationStatusEvent> events = new ArrayList<>( oneOf );

        if ( !invalid.isEmpty() )
        {
            EvaluationStatusEvent event =
                    EvaluationStatusEvent.newBuilder()
                                         .setStatusLevel( StatusLevel.ERROR )
                                         .setEventMessage( "The URIs ('uri') at the following positions in "
                                                           + article
                                                           + " '"
                                                           + orientation
                                                           + "' data source were invalid: "
                                                           + invalid
                                                           + ". Each URI is validated against RFC 2396: "
                                                           + "Uniform Resource Identifiers (URI): Generic "
                                                           + "Syntax, amended by RFC 2732: Format for Literal "
                                                           + "IPv6 Addresses in URLs. Please check the URI "
                                                           + AND_TRY_AGAIN )
                                         .build();
            events.add( event );
        }

        // Check that each source interface is consistent with the data type
        List<EvaluationStatusEvent> interfaceEvents
                = DeclarationValidator.interfacesAreConsistentWithTheDataType( sources, type, orientation );
        events.addAll( interfaceEvents );

        return Collections.unmodifiableList( events );
    }

    /**
     * Determines whether the source refers to a web service.
     * @param source the source
     * @return whether the source refers to an HTTP address
     */

    private static boolean isWebSource( Source source )
    {
        return Objects.nonNull( source.uri() )
               && source.uri()
                        .toString()
                        .startsWith( "http" );
    }

    /**
     * Checks that all declared sources are valid.
     * @param sources the source to validate
     * @param type the data type
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
                                                      .map( Source::sourceInterface )
                                                      .filter( Objects::nonNull )
                                                      .toList();

            // Iterate through the interfaces and compare the implied data types to the declared or inferred type for
            // this side of data
            for ( SourceInterface nextInterface : interfaces )
            {
                // The type is a WRDS interface
                Set<DataType> types = nextInterface.getDataTypes();
                List<EvaluationStatusEvent> next = DeclarationValidator.validateWrdsSourceInterfaces( nextInterface,
                                                                                                      type,
                                                                                                      orientation );
                events.addAll( next );

                // The type is not one of the expected ones
                if ( Objects.nonNull( type ) && !types.contains( type ) )
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
                                                             WHEN_INSPECTING_THE_INTERFACES_ASSOCIATED_WITH_THE
                                                             + orientation
                                                             + DATA_DISCOVERED_AN_INTERFACE_OF
                                                             + nextInterface
                                                             + WHICH_ADMITS_THE_DATA_TYPES
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
                                                             WHEN_INSPECTING_THE_INTERFACES_ASSOCIATED_WITH_THE
                                                             + orientation + DATA_DISCOVERED_AN_INTERFACE_OF
                                                             + nextInterface
                                                             + WHICH_ADMITS_THE_DATA_TYPES
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
     * Validates source interfaces that correspond to the NOAA Water Resources Data Service (WRDS).
     * @param sourceInterface the interface
     * @param type the data type
     * @param orientation the dataset orientation
     * @return any validation events encountered
     */

    private static List<EvaluationStatusEvent> validateWrdsSourceInterfaces( SourceInterface sourceInterface,
                                                                             DataType type,
                                                                             String orientation )
    {
        // The WRDS HEFS service always serves ensemble forecasts, so no need to validate that

        List<EvaluationStatusEvent> events = new ArrayList<>();
        Set<DataType> types = sourceInterface.getDataTypes();
        if ( Objects.isNull( type )
             && ( sourceInterface == SourceInterface.WRDS_AHPS
                  || sourceInterface == SourceInterface.WRDS_NWM ) )
        {
            EvaluationStatusEvent event =
                    EvaluationStatusEvent.newBuilder()
                                         .setStatusLevel( StatusLevel.WARN )
                                         .setEventMessage( WHEN_INSPECTING_THE_INTERFACES_ASSOCIATED_WITH_THE
                                                           + orientation
                                                           + DATA_DISCOVERED_AN_INTERFACE_OF
                                                           + sourceInterface
                                                           + WHICH_ADMITS_THE_DATA_TYPES
                                                           + types
                                                           + ", but the data 'type' for the '"
                                                           + orientation
                                                           + "' data was not declared. This is allowed, but the data "
                                                           + "'type' will need to be inferred from the available "
                                                           + "context and there may be subsequent warnings or errors "
                                                           + "related to this." )
                                         .build();
            events.add( event );
        }

        return Collections.unmodifiableList( events );
    }

    /**
     * @param dataset the dataset
     * @return whether web sources are declared for the dataset
     */
    private static boolean hasWebSources( Dataset dataset )
    {
        return Objects.nonNull( dataset )
               && DeclarationValidator.hasSourceInterface( dataset.sources(),
                                                           SourceInterface.USGS_NWIS,
                                                           SourceInterface.WRDS_AHPS,
                                                           SourceInterface.WRDS_NWM,
                                                           SourceInterface.WRDS_HEFS )
               && dataset.sources()
                         .stream()
                         .anyMatch( s -> Objects.nonNull( s )
                                         && Objects.nonNull( s.uri() )
                                         && Objects.nonNull( s.uri()
                                                              .getScheme() )
                                         && s.uri()
                                             .getScheme()
                                             .startsWith( "http" ) );
    }

    /**
     * @param dataset the dataset
     * @return whether the variable is defined for the dataset with the prescribed orientation
     */
    private static boolean variableIsNotDeclared( Dataset dataset )
    {
        return Objects.nonNull( dataset )
               && Objects.isNull( dataset.variable() );
    }

    /**
     * @param timeInterval the time interval
     * @return whether the time interval is fully defined
     */
    private static boolean isTimeIntervalIncomplete( TimeInterval timeInterval )
    {
        return Objects.isNull( timeInterval )
               || Objects.isNull( timeInterval.minimum() )
               || Objects.isNull( timeInterval.maximum() );
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
     * Determines whether the data type is present on any side. If the data type is undefined for a given side, then
     * that is neutral, i.e., the data type cannot be shown to be absent on that side.
     * @param type the data type
     * @param declaration the declaration
     * @return whether the declaration has the data type on any side
     */

    private static boolean doesNotHaveThisDataType( DataType type, EvaluationDeclaration declaration )
    {
        return Objects.nonNull( declaration.left() )
               && Objects.nonNull( declaration.left()
                                              .type() )
               && declaration.left()
                             .type() != type
               && Objects.nonNull( declaration.right() )
               && Objects.nonNull( declaration.right()
                                              .type() )
               && declaration.right()
                             .type() != type
               && ( !DeclarationUtilities.hasBaseline( declaration )
                    || ( Objects.nonNull( declaration.baseline()
                                                     .dataset()
                                                     .type() ) && declaration.baseline()
                                                                             .dataset()
                                                                             .type() != type ) );
    }

    /**
     * Do not construct.
     */
    private DeclarationValidator()
    {
    }
}
