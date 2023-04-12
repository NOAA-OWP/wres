package wres.config.yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.protobuf.DoubleValue;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.lang3.tuple.Pair;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.util.GeometricShapeFactory;
import org.locationtech.jts.io.WKTWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.resolver.Resolver;

import wres.config.MetricConstants;
import wres.config.generated.ThresholdFormat;
import wres.config.xml.CsvThresholdReader;
import wres.config.xml.MetricConstantsFactory;
import wres.config.xml.ProjectConfigs;
import wres.config.generated.Circle;
import wres.config.generated.DataSourceBaselineConfig;
import wres.config.generated.DataSourceConfig;
import wres.config.generated.DatasourceType;
import wres.config.generated.DateCondition;
import wres.config.generated.DesiredTimeScaleConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DoubleBoundsType;
import wres.config.generated.DurationBoundsType;
import wres.config.generated.DurationUnit;
import wres.config.generated.EnsembleCondition;
import wres.config.generated.FeatureDimension;
import wres.config.generated.FeatureGroup;
import wres.config.generated.FeaturePool;
import wres.config.generated.GraphicalType;
import wres.config.generated.IntBoundsType;
import wres.config.generated.LenienceType;
import wres.config.generated.MetricConfigName;
import wres.config.generated.MetricsConfig;
import wres.config.generated.NamedFeature;
import wres.config.generated.OutputTypeSelection;
import wres.config.generated.PairConfig;
import wres.config.generated.Polygon;
import wres.config.generated.PoolingWindowConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.ThresholdDataType;
import wres.config.generated.ThresholdsConfig;
import wres.config.generated.TimeScaleConfig;
import wres.config.generated.TimeSeriesMetricConfigName;
import wres.config.generated.UnnamedFeature;
import wres.config.generated.UrlParameter;
import wres.config.yaml.components.AnalysisDurations;
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.CrossPair;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.DecimalFormatPretty;
import wres.config.yaml.components.EnsembleFilter;
import wres.config.yaml.components.EnsembleFilterBuilder;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.FeatureAuthority;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.FeatureService;
import wres.config.yaml.components.FeatureServiceBuilder;
import wres.config.yaml.components.FeatureServiceGroup;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.Format;
import wres.config.yaml.components.Formats;
import wres.config.yaml.components.LeadTimeInterval;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.MetricBuilder;
import wres.config.yaml.components.MetricParameters;
import wres.config.yaml.components.MetricParametersBuilder;
import wres.config.yaml.components.Season;
import wres.config.yaml.components.Source;
import wres.config.yaml.components.SourceBuilder;
import wres.config.yaml.components.SourceInterface;
import wres.config.yaml.components.SpatialMask;
import wres.config.yaml.components.SpatialMaskBuilder;
import wres.config.yaml.components.Threshold;
import wres.config.yaml.components.ThresholdBuilder;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.config.yaml.components.ThresholdService;
import wres.config.yaml.components.ThresholdServiceBuilder;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.components.TimeInterval;
import wres.config.yaml.components.TimePools;
import wres.config.yaml.components.TimeScale;
import wres.config.yaml.components.TimeScaleBuilder;
import wres.config.yaml.components.TimeScaleLenience;
import wres.config.yaml.components.UnitAlias;
import wres.config.yaml.components.Values;
import wres.config.yaml.components.Variable;
import wres.config.yaml.components.VariableBuilder;
import wres.config.yaml.deserializers.ZoneOffsetDeserializer;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.Outputs;
import wres.statistics.generated.Pool;

/**
 * <p>Factory class for generating POJOs from a YAML-formatted declaration string that is consistent with the WRES 
 * declaration schema. This consistency is asserted during deserialization.
 *
 * <p>Implementation notes:
 *
 * <p>There are many possible approaches to deserializing formatted declaration strings into POJOs. These include 
 * numerous deserialization frameworks, such as Jackson and Gson, which use binding annotation to help deserialize a
 * formatted string into POJOs, and source code generation tools, such as jsonschema2pojo, which generate Java sources
 * directly from a schema and include the binding annotation for a given deserialization framework.
 *
 * <p>The project declaration language is YAML, which is a superset of JSON. Currently, Jackson is used to deserialize
 * a YAML-formatted string that conforms to the WRES schema into Java records, which are slim, immutable classes. To
 * begin with, the string is deserialized into a Jackson abstraction. It is then validated against the schema, which is
 * also read into a Jackson abstraction. Finally, if the validation succeeds, the objects are mapped to POJOs using
 * default (to Jackson) deserializers wherever possible, else customer deserializers. This achieves a reasonable
 * balance between objects with desirable properties, such as immutability, and ease of maintaining the deserialization
 * code as the schema evolves. However, other approaches may be considered as the tooling improves. For example, were
 * jsonschema2pojo to support immutable types (e.g., org.immutables), allow for closer control over the sources created
 * (e.g., with custom Jackson deserializers) and to properly handle YAML constructs such as allOf, oneOf and anyOf,
 * then the advantages of deserialization directly from the schema without any brittle boilerplate would strongly favor
 * that approach.
 *
 * <p>For discussion of these topics, see:
 * <a href="https://github.com/joelittlejohn/jsonschema2pojo/issues/392">jsonschema2pojo-issue392</a>
 * <a href="https://github.com/joelittlejohn/jsonschema2pojo/issues/1405">jsonschema2pojo-issue1405</a>
 *
 * <p>Declaration is processed in at least two stages. First, the raw declaration is read from a YAML string into POJOs
 * using {@link #from(String)}. Second, options that are declared "implicitly" are resolved via interpolation using
 * the {@link DeclarationInterpolator}.
 *
 * @author James Brown
 */

public class DeclarationFactory
{
    /** Default threshold operator. */
    public static final ThresholdOperator DEFAULT_THRESHOLD_OPERATOR = ThresholdOperator.GREATER;

    /** Default threshold data type. */
    public static final ThresholdOrientation DEFAULT_THRESHOLD_ORIENTATION = ThresholdOrientation.LEFT;

    /** Default threshold type (e.g., for a threshold data service). */
    public static final ThresholdType DEFAULT_THRESHOLD_TYPE = ThresholdType.VALUE;

    /** Default canonical threshold with no values. */
    public static final wres.statistics.generated.Threshold DEFAULT_CANONICAL_THRESHOLD
            = wres.statistics.generated.Threshold.newBuilder()
                                                 .setDataType( DEFAULT_THRESHOLD_ORIENTATION.canonical() )
                                                 .setOperator( DEFAULT_THRESHOLD_OPERATOR.canonical()  )
                                                 .build();
    /** Default wrapped threshold. */
    public static final Threshold DEFAULT_THRESHOLD = new Threshold( DEFAULT_CANONICAL_THRESHOLD, null, null, null );

    /** To stringify protobufs into presentable JSON. */
    public static final Function<MessageOrBuilder, String> PROTBUF_STRINGIFIER = message -> {
        if ( Objects.nonNull( message ) )
        {
            try
            {
                return JsonFormat.printer().omittingInsignificantWhitespace().print( message );
            }
            catch ( InvalidProtocolBufferException e )
            {
                throw new IllegalStateException( "Failed to stringify a protobuf message." );
            }
        }

        return "null";
    };

    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DeclarationFactory.class );

    /** Mapper for deserialization. */
    private static final ObjectMapper DESERIALIZER = YAMLMapper.builder()
                                                               .enable( MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS )
                                                               .build()
                                                               .registerModule( new ProtobufModule() )
                                                               .registerModule( new JavaTimeModule() )
                                                               .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
                                                                           true );

    /** Mapper for serialization. */
    private static final ObjectMapper SERIALIZER =
            new ObjectMapper( new YAMLFactoryWithCustomGenerator().disable( YAMLGenerator.Feature.WRITE_DOC_START_MARKER )
                                                                  .disable( YAMLGenerator.Feature.SPLIT_LINES )
                                                                  .enable( YAMLGenerator.Feature.MINIMIZE_QUOTES )
                                                                  .enable( YAMLGenerator.Feature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS )
                                                                  .configure( YAMLGenerator.Feature.INDENT_ARRAYS_WITH_INDICATOR,
                                                                              true ) )
                    .registerModule( new JavaTimeModule() )
                    .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES, true )
                    .configure( SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true )
                    .configure( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false )
                    .setSerializationInclusion( JsonInclude.Include.NON_NULL )
                    .setSerializationInclusion( JsonInclude.Include.NON_EMPTY );

    /** Schema file name on the classpath. */
    private static final String SCHEMA = "schema.yml";

    /** String representation of the default missing value in the old declaration language. */
    private static final String DEFAULT_MISSING_STRING_OLD = "-999.0";

    /** Default metric parameters. */
    private static final MetricParameters DEFAULT_METRIC_PARAMETERS = MetricParametersBuilder.builder()
                                                                                             .build();

    /**
     * Deserializes a YAML string into a POJO and performs validation against the schema. Optionally performs
     * interpolation of missing declaration, followed by validation of the interpolated declaration. Interpolation is
     * performed with {@link DeclarationInterpolator#interpolate(EvaluationDeclaration)} and validation is performed
     * with {@link DeclarationValidator#validateAndNotify(EvaluationDeclaration)}.
     *
     * @see #from(String)
     * @param yaml the yaml string
     * @param interpolateAndValidate is true to interpolate any missing declaration and then validate the declaration
     * @return an evaluation declaration
     * @throws IllegalStateException if the project declaration schema could not be found on the classpath
     * @throws IOException if the schema could not be read
     * @throws DeclarationSchemaException if the project declaration could not be validated against the schema
     * @throws DeclarationException if the declaration is invalid
     * @throws NullPointerException if the yaml string is null
     */

    public static EvaluationDeclaration from( String yaml, boolean interpolateAndValidate ) throws IOException
    {
        EvaluationDeclaration declaration = DeclarationFactory.from( yaml );

        if ( interpolateAndValidate )
        {
            declaration = DeclarationInterpolator.interpolate( declaration );
            DeclarationValidator.validateAndNotify( declaration );
        }

        return declaration;
    }

    /**
     * Deserializes a YAML string into a POJO and performs validation against the schema only. Does not "interpolate"
     * any missing declaration options that may be gleaned from other declaration. To perform "interpolation", use
     * {@link DeclarationInterpolator#interpolate(EvaluationDeclaration)}. Also, does not perform any high-level
     * validation of the declaration for mutual consistency and coherence (aka "business logic"). To perform high-level
     * validation, see the {@link DeclarationValidator}.
     *
     * @see DeclarationValidator#validateAndNotify(EvaluationDeclaration)
     * @see DeclarationValidator#validate(EvaluationDeclaration)
     * @param yaml the yaml string
     * @return an evaluation declaration
     * @throws IllegalStateException if the project declaration schema could not be found on the classpath
     * @throws IOException if the schema could not be read
     * @throws DeclarationSchemaException if the project declaration could not be validated against the schema
     * @throws NullPointerException if the yaml string is null
     */

    public static EvaluationDeclaration from( String yaml ) throws IOException
    {
        Objects.requireNonNull( yaml );

        // Get the schema from the classpath
        URL schema = DeclarationFactory.class.getClassLoader().getResource( SCHEMA );

        LOGGER.debug( "Read the declaration schema from classpath resource '{}'.", SCHEMA );

        if ( Objects.isNull( schema ) )
        {
            throw new IllegalStateException(
                    "Could not find the project declaration schema " + SCHEMA + "on the classpath." );
        }

        try ( InputStream stream = schema.openStream() )
        {
            String schemaString = new String( stream.readAllBytes(), StandardCharsets.UTF_8 );

            // Map the schema to a json node
            JsonNode schemaNode = DESERIALIZER.readTree( schemaString );

            // Unfortunately, Jackson does not currently resolve anchors/references, despite using SnakeYAML under the
            // hood. Instead, use SnakeYAML to create a resolved string first. This is inefficient and undesirable
            // because it means  that the declaration string is being read twice
            // TODO: use Jackson to read the raw YAML string once it can handle anchors/references properly
            Yaml snakeYaml = new Yaml( new Constructor( new LoaderOptions() ),
                                       new Representer( new DumperOptions() ),
                                       new DumperOptions(),
                                       new CustomResolver() );
            Object resolvedYaml = snakeYaml.load( yaml );
            String resolvedYamlString =
                    DESERIALIZER.writerWithDefaultPrettyPrinter().writeValueAsString( resolvedYaml );

            LOGGER.info( "Resolved a YAML declaration string: {}.", resolvedYaml );

            // Use Jackson to (re-)read the declaration string once any anchors/references are resolved
            JsonNode declaration = DESERIALIZER.readTree( resolvedYamlString );

            JsonSchemaFactory factory =
                    JsonSchemaFactory.builder( JsonSchemaFactory.getInstance( SpecVersion.VersionFlag.V201909 ) )
                                     .objectMapper( DESERIALIZER )
                                     .build();

            // Validate the declaration against the schema
            JsonSchema validator = factory.getSchema( schemaNode );

            Set<ValidationMessage> errors = validator.validate( declaration );

            LOGGER.debug( "Validated a declaration string against the schema, which produced {} errors.",
                          errors.size() );

            if ( !errors.isEmpty() )
            {
                throw new DeclarationSchemaException( "Encountered an error while attempting to validate a project "
                                                      + "declaration string against the schema. Please check your "
                                                      + "declaration and fix any errors. The errors encountered were: "
                                                      + errors );
            }

            LOGGER.debug( "Deserializing a declaration string into POJOs." );

            // Deserialize
            return DESERIALIZER.reader()
                               .readValue( declaration, EvaluationDeclaration.class );
        }
    }

    /**
     * Serializes an evaluation to a YAML declaration string.
     *
     * @param evaluation the evaluation declaration
     * @return the YAML string
     * @throws IOException if the string could not be written
     * @throws NullPointerException if the evaluation is null
     */

    public static String from( EvaluationDeclaration evaluation ) throws IOException
    {
        Objects.requireNonNull( evaluation );

        try
        {
            return SERIALIZER.writerWithDefaultPrettyPrinter()
                             .writeValueAsString( evaluation );
        }
        catch ( JsonProcessingException e )
        {
            throw new IOException( "While serializing an evaluation to a YAML string.", e );
        }
    }

    /**
     * Migrates from an old-style declaration POJO (unmarshalled from am XML string) to a new-style declaration POJO
     * (unmarshalled from a YAML string).
     *
     * @param projectConfig the old style declaration POJO
     * @return the new style declaration POJO
     * @throws NullPointerException if expected declaration is missing
     */

    public static EvaluationDeclaration from( ProjectConfig projectConfig )
    {
        Objects.requireNonNull( projectConfig );
        Objects.requireNonNull( projectConfig.getInputs() );
        Objects.requireNonNull( projectConfig.getPair() );
        Objects.requireNonNull( projectConfig.getMetrics() );
        Objects.requireNonNull( projectConfig.getOutputs() );

        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder();

        // Migrate the datasets
        DeclarationFactory.migrateDatasets( projectConfig.getInputs(), builder );
        // Migrate the features
        DeclarationFactory.migrateFeatures( projectConfig.getPair()
                                                         .getFeature(), builder );
        // Migrate the feature groups
        DeclarationFactory.migrateFeatureGroups( projectConfig.getPair()
                                                              .getFeatureGroup(), builder );
        // Migrate the feature service
        DeclarationFactory.migrateFeatureService( projectConfig.getPair()
                                                               .getFeatureService(), builder );
        // Migrate the spatial mask
        DeclarationFactory.migrateSpatialMask( projectConfig.getPair()
                                                            .getGridSelection(), builder );
        // Migrate all the time filters
        DeclarationFactory.migrateTimeFilters( projectConfig.getPair(), builder );
        // Migrate the evaluation timescale
        DeclarationFactory.migrateEvaluationTimeScale( projectConfig.getPair()
                                                                    .getDesiredTimeScale(), builder );
        // Migrate the units
        builder.unit( projectConfig.getPair()
                                   .getUnit() );
        // Migrate the unit aliases
        DeclarationFactory.migrateUnitAliases( projectConfig.getPair()
                                                            .getUnitAlias(), builder );
        // Migrate cross-pairing
        DeclarationFactory.migrateCrossPairing( projectConfig.getPair()
                                                             .getCrossPair(), builder );
        // Migrate the value filter
        DeclarationFactory.migrateValueFilter( projectConfig.getPair()
                                                            .getValues(), builder );
        // Migrate the metrics and any associated thresholds
        DeclarationFactory.migrateMetrics( projectConfig, builder );
        // Migrate the output formats
        DeclarationFactory.migrateOutputFormats( projectConfig.getOutputs(), builder );

        return builder.build();
    }


    /**
     * Gets a human-friendly enum name from an enum string. The reverse of {@link #toEnumName(String)}.
     * @param enumString the enum string
     * @return a human-friendly name with spaces instead of underscores and all lower case
     * @throws NullPointerException if the input is null
     */

    public static String fromEnumName( String enumString )
    {
        Objects.requireNonNull( enumString );

        return enumString.replace( "_", " " )
                         .toLowerCase();
    }

    /**
     * Returns an enumeration name from an informal string. The reverse of {@link #fromEnumName(String)}.
     * @param name the name
     * @return the enum-friendly name
     * @throws NullPointerException if the input is null
     */

    public static String toEnumName( String name )
    {
        Objects.requireNonNull( name );

        return name.toUpperCase()
                   .replace( " ", "_" );
    }

    /**
     * Returns a string representation of the duration in hours if possible, otherwise seconds. Warns if this will
     * result in a loss of precision.
     *
     * @param duration the duration to serialize
     * @return the duration string and associated units
     */

    public static Pair<Long, String> getDurationInPreferredUnits( Duration duration )
    {
        // Loss of precision?
        if ( duration.toNanosPart() > 0 )
        {
            LOGGER.warn( "Received a duration of {}, which  is not exactly divisible by hours or seconds. The "
                         + "nanosecond part of this duration is {} and will not be serialized.", duration,
                         duration.toNanosPart() );
        }

        // Integer number of hours?
        if ( duration.toSeconds() % 3600L == 0 )
        {
            return Pair.of( duration.toHours(), "hours" );
        }

        return Pair.of( duration.toSeconds(), "seconds" );
    }

    /**
     * Returns a string representation of each ensemble declaration item discovered.
     * @param declaration the declaration
     * @return the ensemble declaration was found
     */

    static Set<String> getEnsembleDeclaration( EvaluationDeclaration declaration )
    {
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder( declaration );
        return DeclarationFactory.getEnsembleDeclaration( builder );
    }

    /**
     * @param declaration the declaration
     * @return whether analysis durations have been declared
     */
    static boolean hasAnalysisDurations( EvaluationDeclaration declaration )
    {
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder( declaration );
        return DeclarationFactory.hasAnalysisDurations( builder );
    }

    /**
     * @param builder the builder
     * @return whether analysis durations have been declared
     */
    static boolean hasAnalysisDurations( EvaluationDeclarationBuilder builder )
    {
        return Objects.nonNull( builder.analysisDurations() ) && (
                Objects.nonNull( builder.analysisDurations().minimumExclusive() )
                || Objects.nonNull( builder.analysisDurations().maximum() ) );
    }

    /**
     * Returns a string representation of each forecast declaration item discovered.
     * @param declaration the declaration
     * @return the forecast declaration strings, if any
     */
    static Set<String> getForecastDeclaration( EvaluationDeclaration declaration )
    {
        EvaluationDeclarationBuilder builder = EvaluationDeclarationBuilder.builder( declaration );
        return DeclarationFactory.getForecastDeclaration( builder );
    }

    /**
     * Returns a string representation of each ensemble declaration item discovered.
     * @param builder the builder
     * @return the ensemble declaration was found
     */

    static Set<String> getEnsembleDeclaration( EvaluationDeclarationBuilder builder )
    {
        Set<String> ensembleDeclaration = new TreeSet<>();

        // Ensemble filter on predicted dataset?
        if ( Objects.nonNull( builder.right().ensembleFilter() ) )
        {
            ensembleDeclaration.add( "An 'ensemble_filter' was declared on the predicted dataset." );
        }

        // Ensemble filter on baseline dataset?
        if ( DeclarationFactory.hasBaseline( builder ) && Objects.nonNull( builder.baseline()
                                                                                  .dataset()
                                                                                  .ensembleFilter() ) )
        {
            ensembleDeclaration.add( "An 'ensemble_filter' was declared on the baseline dataset." );
        }

        // Any source that contains an interface with the word ensemble?
        Set<String> ensembleInterfaces = DeclarationFactory.getSourcesWithEnsembleInterface( builder );
        if ( !ensembleInterfaces.isEmpty() )
        {
            ensembleDeclaration.add(
                    "Discovered one or more data sources whose interfaces are ensemble-like: " + ensembleInterfaces
                    + "." );
        }

        // Ensemble average declared?
        if ( Objects.nonNull( builder.ensembleAverageType() ) )
        {
            ensembleDeclaration.add( "An 'ensemble_average' was declared." );
        }

        // Ensemble metrics?  Ignore metrics that also belong to the single-valued group
        Set<String> ensembleMetrics = DeclarationFactory.getMetricType( builder,
                                                                        MetricConstants.SampleDataGroup.ENSEMBLE,
                                                                        MetricConstants.SampleDataGroup.SINGLE_VALUED );
        if ( !ensembleMetrics.isEmpty() )
        {
            ensembleDeclaration.add( "Discovered metrics that require ensemble forecasts: " + ensembleMetrics + "." );
        }

        // Discrete probability metrics?
        Set<String> discreteProbabilityMetrics =
                DeclarationFactory.getMetricType( builder,
                                                  MetricConstants.SampleDataGroup.DISCRETE_PROBABILITY,
                                                  null );
        if ( !discreteProbabilityMetrics.isEmpty() )
        {
            ensembleDeclaration.add( "Discovered metrics that focus on discrete probability forecasts and these can "
                                     + "only be obtained from ensemble forecasts, currently: "
                                     + discreteProbabilityMetrics + "." );
        }

        return Collections.unmodifiableSet( ensembleDeclaration );
    }

    /**
     * Returns a string representation of each forecast declaration item discovered.
     * @param builder the builder
     * @return the forecast declaration strings, if any
     */
    static Set<String> getForecastDeclaration( EvaluationDeclarationBuilder builder )
    {
        Set<String> forecastDeclaration = new TreeSet<>();

        // Reference times?
        if ( Objects.nonNull( builder.referenceDates() ) )
        {
            forecastDeclaration.add( "Discovered a 'reference_dates' filter." );
        }

        // Reference time pools?
        if ( Objects.nonNull( builder.referenceDatePools() ) )
        {
            forecastDeclaration.add( "Discovered 'reference_date_pools'." );
        }

        // Lead times?
        if ( Objects.nonNull( builder.leadTimes() ) )
        {
            forecastDeclaration.add( "Discovered 'lead_times' filter." );
        }

        // Lead time pools?
        if ( Objects.nonNull( builder.leadTimePools() ) )
        {
            forecastDeclaration.add( "Discovered 'lead_time_pool'." );
        }

        // One or more sources with a forecast-like interface
        Set<String> forecastInterfaces = DeclarationFactory.getSourcesWithForecastInterface( builder );
        if ( !forecastInterfaces.isEmpty() )
        {
            forecastDeclaration.add( "Discovered one or more data sources whose interfaces are forecast-like: "
                                     + forecastInterfaces
                                     + "." );
        }

        return Collections.unmodifiableSet( forecastDeclaration );
    }

    /**
     * Groups the thresholds by threshold type.
     * @param thresholds the thresholds
     * @return the thresholds grouped by type
     */

    static Map<wres.config.yaml.components.ThresholdType, Set<Threshold>> groupThresholdsByType( Set<Threshold> thresholds )
    {
        return thresholds.stream()
                         .collect( Collectors.groupingBy( Threshold::type,
                                                          Collectors.mapping( Function.identity(),
                                                                              Collectors.toCollection(
                                                                                      LinkedHashSet::new ) ) ) );
    }

    /**
     * Inspects the dataset for an explicitly declared feature authority, else attempts to interpolate the authority
     * from the other information present.
     *
     * @param dataset the dataset
     * @return the set of explicitly declared or implicitly declared feature authorities associated with the dataset
     * @throws NullPointerException if the input is null
     */

    static Set<FeatureAuthority> getFeatureAuthorities( Dataset dataset )
    {
        Objects.requireNonNull( dataset );

        // Explicit authority?
        if ( Objects.nonNull( dataset.featureAuthority() ) )
        {
            if ( LOGGER.isDebugEnabled() )
            {
                LOGGER.debug( "Discovered an explicitly declared feature authority of '{}' for the dataset labelled "
                              + "'{}'.",
                              dataset.featureAuthority(), dataset.label() );
            }

            return Set.of( dataset.featureAuthority() );
        }

        // Try to work out the authority from the sources
        return dataset.sources()
                      .stream()
                      .map( DeclarationFactory::getFeatureAuthorityFromSource )
                      .filter( Objects::nonNull )
                      .collect( Collectors.toUnmodifiableSet() );

    }

    /**
     * Inspects the source and tries to determine the feature authority.
     * @param source the source
     * @return the feature authority or null if none could be determined
     */
    private static FeatureAuthority getFeatureAuthorityFromSource( Source source )
    {
        // Inspect the source interface
        FeatureAuthority sourceInterfaceAuthority = null;
        SourceInterface sourceInterface = source.sourceInterface();

        if ( Objects.nonNull( sourceInterface ) )
        {
            sourceInterfaceAuthority = sourceInterface.getFeatureAuthority();
        }

        // Inspect the source URI
        FeatureAuthority sourceUriAuthority = null;
        URI uri = source.uri();
        if ( Objects.nonNull( uri ) && uri.toString()
                                          .contains( "usgs.gov/nwis" ) )
        {
            sourceUriAuthority = FeatureAuthority.USGS_SITE_CODE;
        }

        // Feature authority from source URI is null or both are equal, return the authority from the interface
        if ( Objects.isNull( sourceUriAuthority ) || sourceInterfaceAuthority == sourceUriAuthority )
        {
            return sourceInterfaceAuthority;
        }

        // Authority from the interface is null, return the authority from the source URI
        if ( Objects.isNull( sourceInterfaceAuthority ) )
        {
            return sourceUriAuthority;
        }

        LOGGER.warn( "Discovered a source whose feature authority implied by the source URI contradicts the "
                     + "feature authority implied by the source interface. The source interface says {} and the URI "
                     + "says {}. The feature authority cannot be determined. The source URI is: {}. Please adjust the "
                     + "source as needed.", sourceInterfaceAuthority, sourceUriAuthority, uri );

        return null;
    }

    /**
     * @param builder the builder
     * @param groupType the group the metric should be part of
     * @param notInGroupType the optional group the metric should not be part of, in case it belongs to several groups
     * @return whether there are any metrics with the designated type
     */
    private static Set<String> getMetricType( EvaluationDeclarationBuilder builder,
                                              MetricConstants.SampleDataGroup groupType,
                                              MetricConstants.SampleDataGroup notInGroupType )
    {
        return builder.metrics()
                      .stream()
                      .filter( next -> next.name()
                                           .isInGroup( groupType )
                                       && ( Objects.isNull( notInGroupType ) || !next.name()
                                                                                     .isInGroup( notInGroupType ) ) )
                      .map( next -> next.name()
                                        .toString() )
                      .collect( Collectors.toUnmodifiableSet() );
    }

    /**
     * @param builder the builder
     * @return the sources with an interface/api that are exclusively ensemble forecasts
     */
    private static Set<String> getSourcesWithEnsembleInterface( EvaluationDeclarationBuilder builder )
    {
        List<String> right = builder.right()
                                    .sources()
                                    .stream()
                                    .filter( next -> Objects.nonNull( next.sourceInterface() )
                                                     && next.sourceInterface()
                                                            .getDataTypes()
                                                            .equals( Set.of(
                                                                    DataType.ENSEMBLE_FORECASTS ) ) )
                                    .map( next -> next.sourceInterface().toString() )
                                    .toList();

        Set<String> ensembleInterfaces = new TreeSet<>( right );

        if ( DeclarationFactory.hasBaseline( builder ) )
        {
            List<String> baseline = builder.right()
                                           .sources()
                                           .stream()
                                           .filter( next -> Objects.nonNull( next.sourceInterface() )
                                                            && next.sourceInterface()
                                                                   .getDataTypes()
                                                                   .equals( Set.of( DataType.ENSEMBLE_FORECASTS ) ) )
                                           .map( next -> next.sourceInterface().toString() )
                                           .toList();
            ensembleInterfaces.addAll( baseline );
        }

        return Collections.unmodifiableSet( ensembleInterfaces );
    }

    /**
     * @param builder the builder
     * @return return any sources whose interfaces are exclusively forecast-like
     */
    private static Set<String> getSourcesWithForecastInterface( EvaluationDeclarationBuilder builder )
    {
        Set<String> interfaces = new TreeSet<>( DeclarationFactory.getSourcesWithEnsembleInterface( builder ) );

        List<String> right = builder.right()
                                    .sources()
                                    .stream()
                                    .filter( next -> Objects.nonNull( next.sourceInterface() )
                                                     && next.sourceInterface()
                                                            .getDataTypes()
                                                            .stream()
                                                            .allMatch(
                                                                    DataType::isForecastType ) )
                                    .map( next -> next.sourceInterface().toString() )
                                    .toList();

        interfaces.addAll( right );

        if ( DeclarationFactory.hasBaseline( builder ) )
        {
            List<String> baseline = builder.baseline()
                                           .dataset()
                                           .sources()
                                           .stream()
                                           .filter( next -> Objects.nonNull( next.sourceInterface() )
                                                            && next.sourceInterface()
                                                                   .getDataTypes()
                                                                   .stream()
                                                                   .allMatch( DataType::isForecastType ) )
                                           .map( next -> next.sourceInterface().toString() )
                                           .toList();
            interfaces.addAll( baseline );
        }

        return Collections.unmodifiableSet( interfaces );
    }

    /**
     * @param builder the builder
     * @return whether a baseline dataset has been declared
     */
    static boolean hasBaseline( EvaluationDeclarationBuilder builder )
    {
        return Objects.nonNull( builder.baseline() );
    }

    /**
     * Migrates the datasets from the old declaration to the new declaration builder.
     * @param datasets the datasets to migrate
     * @param builder the new declaration builder
     */

    private static void migrateDatasets( ProjectConfig.Inputs datasets, EvaluationDeclarationBuilder builder )
    {
        Objects.requireNonNull( datasets );

        LOGGER.debug( "Migrating datasets from an old-style project declaration to a new-style declaration." );

        Dataset left = DeclarationFactory.migrateDataset( datasets.getLeft() );
        Dataset right = DeclarationFactory.migrateDataset( datasets.getRight() );
        BaselineDataset baseline = null;
        DataSourceBaselineConfig baselineConfig = datasets.getBaseline();

        // Baseline dataset? If so, this has extras
        if ( Objects.nonNull( baselineConfig ) )
        {
            Dataset baselineDataset = DeclarationFactory.migrateDataset( baselineConfig );
            BaselineDatasetBuilder baselineBuilder = BaselineDatasetBuilder.builder()
                                                                           .dataset( baselineDataset );

            if ( Objects.nonNull( baselineConfig.getPersistence() ) )
            {
                LOGGER.debug( "Adding persistence of order {} to the baseline dataset.",
                              baselineConfig.getPersistence() );
                baselineBuilder.persistence( baselineConfig.getPersistence() );
            }

            // Old style declaration
            if ( Objects.nonNull( baselineConfig.getTransformation() )
                 && "persistence".equals( baselineConfig.getTransformation()
                                                        .value() ) )

            {
                LOGGER.debug( "Discovered an old-style persistence declaration. Adding persistence of order 1 to the "
                              + "baseline dataset." );
                baselineBuilder.persistence( 1 );
            }

            baseline = baselineBuilder.build();
        }

        // Add the datasets
        builder.left( left ).right( right ).baseline( baseline );
    }

    /**
     * Migrates the features from the old declaration to the new declaration builder.
     * @param features the features to migrate
     * @param builder the new declaration builder
     */

    private static void migrateFeatures( List<NamedFeature> features, EvaluationDeclarationBuilder builder )
    {
        if ( !features.isEmpty() )
        {
            Set<GeometryTuple> geometries = DeclarationFactory.migrateFeatures( features );
            Features wrappedFeatures = new Features( geometries );
            builder.features( wrappedFeatures );
        }
    }

    /**
     * Migrates the feature groups from the old declaration to the new declaration builder.
     * @param featureGroups the feature groups to migrate
     * @param builder the new declaration builder
     */

    private static void migrateFeatureGroups( List<FeaturePool> featureGroups, EvaluationDeclarationBuilder builder )
    {
        if ( !featureGroups.isEmpty() )
        {
            Set<GeometryGroup> geometryGroups =
                    featureGroups.stream()
                                 .map( DeclarationFactory::migrateFeatureGroup )
                                 .collect( Collectors.toSet() );
            FeatureGroups wrappedGroups = new FeatureGroups( geometryGroups );
            builder.featureGroups( wrappedGroups );
        }
    }

    /**
     * Migrates the feature service from the old declaration to the new declaration builder.
     * @param featureService the feature service to migrate
     * @param builder the new declaration builder
     */

    private static void migrateFeatureService( wres.config.generated.FeatureService featureService,
                                               EvaluationDeclarationBuilder builder )
    {
        if ( Objects.nonNull( featureService ) )
        {
            Set<FeatureServiceGroup> featureGroups =
                    featureService.getGroup()
                                  .stream()
                                  .map( DeclarationFactory::migrateFeatureServiceGroup )
                                  .collect( Collectors.toSet() );
            FeatureService service = FeatureServiceBuilder.builder()
                                                          .featureGroups( featureGroups )
                                                          .uri( featureService.getBaseUrl() )
                                                          .build();
            builder.featureService( service );
        }
    }

    /**
     * Migrates a spatial mask from the old declaration to the new declaration builder.
     * @param mask the spatial mask to migrate
     * @param builder the new declaration builder
     */
    private static void migrateSpatialMask( List<UnnamedFeature> mask, EvaluationDeclarationBuilder builder )
    {
        if ( Objects.nonNull( mask ) && !mask.isEmpty() )
        {
            Pair<String, Long> maskComponents = DeclarationFactory.migrateSpatialMask( mask );
            SpatialMask spatialMask = null;
            if ( Objects.nonNull( maskComponents ) && Objects.nonNull( maskComponents.getLeft() ) )
            {
                spatialMask = SpatialMaskBuilder.builder()
                                                .wkt( maskComponents.getLeft() )
                                                .srid( maskComponents.getRight() )
                                                .build();
            }

            builder.spatialMask( spatialMask );
        }
    }

    /**
     * Migrates the time filters from the old declaration to the new declaration builder.
     * @param pair the pairs declaration that contains the time filters
     * @param builder the new declaration builder
     */
    private static void migrateTimeFilters( PairConfig pair, EvaluationDeclarationBuilder builder )
    {
        // Reference time filter
        if ( Objects.nonNull( pair.getIssuedDates() ) )
        {
            TimeInterval referenceDates = DeclarationFactory.migrateTimeInterval( pair.getIssuedDates() );
            builder.referenceDates( referenceDates );
            LOGGER.debug( "Migrated a reference time filter: {}.", referenceDates );
        }

        // Valid time filter
        if ( Objects.nonNull( pair.getDates() ) )
        {
            TimeInterval validDates = DeclarationFactory.migrateTimeInterval( pair.getDates() );
            builder.validDates( validDates );
            LOGGER.debug( "Migrated a valid time filter: {}.", validDates );
        }

        // Reference time pools
        if ( Objects.nonNull( pair.getIssuedDatesPoolingWindow() ) )
        {
            TimePools referenceTimePools = DeclarationFactory.migrateTimePools( pair.getIssuedDatesPoolingWindow() );
            builder.referenceDatePools( referenceTimePools );
            LOGGER.debug( "Migrated reference time pools: {}.", referenceTimePools );
        }

        // Valid time pools
        if ( Objects.nonNull( pair.getValidDatesPoolingWindow() ) )
        {
            TimePools validTimePools = DeclarationFactory.migrateTimePools( pair.getValidDatesPoolingWindow() );
            builder.validDatePools( validTimePools );
            LOGGER.debug( "Migrated valid time pools: {}.", validTimePools );
        }

        // Lead duration filter
        if ( Objects.nonNull( pair.getLeadHours() ) )
        {
            IntBoundsType leadBounds = pair.getLeadHours();
            LeadTimeInterval leadTimes =
                    new LeadTimeInterval( leadBounds.getMinimum(), leadBounds.getMaximum(), ChronoUnit.HOURS );
            builder.leadTimes( leadTimes );
            LOGGER.debug( "Migrated a lead time filter: {}.", leadTimes );
        }

        // Lead time pools
        if ( Objects.nonNull( pair.getLeadTimesPoolingWindow() ) )
        {
            TimePools leadTimePools = DeclarationFactory.migrateTimePools( pair.getLeadTimesPoolingWindow() );
            builder.leadTimePools( leadTimePools );
            LOGGER.debug( "Migrated lead time pools: {}.", leadTimePools );
        }

        // Analysis durations
        if ( Objects.nonNull( pair.getAnalysisDurations() ) )
        {
            DurationBoundsType durations = pair.getAnalysisDurations();
            ChronoUnit unit = ChronoUnit.valueOf( durations.getUnit()
                                                           .name() );
            AnalysisDurations analysisDurations =
                    new AnalysisDurations( durations.getGreaterThan(), durations.getLessThanOrEqualTo(), unit );
            builder.analysisDurations( analysisDurations );
            LOGGER.debug( "Migrated an analysis duration filter: {}.", analysisDurations );
        }

        // Season filter
        if ( Objects.nonNull( pair.getSeason() ) )
        {
            Season season = DeclarationFactory.migrateSeason( pair.getSeason() );
            builder.season( season );
            LOGGER.debug( "Migrated a season filter: {}.", season );
        }
    }

    /**
     * Migrates the evaluation timescale from the old declaration to the new declaration builder.
     * @param timeScale the timescale to migrate
     * @param builder the new declaration builder
     */
    private static void migrateEvaluationTimeScale( DesiredTimeScaleConfig timeScale,
                                                    EvaluationDeclarationBuilder builder )
    {
        TimeScale timeScaleMigrated = DeclarationFactory.migrateTimeScale( timeScale );

        LOGGER.debug( "Encountered a desired time scale to migrate: {}.", timeScale );

        if ( Objects.nonNull( timeScaleMigrated ) )
        {
            wres.statistics.generated.TimeScale.Builder scaleBuilder = timeScaleMigrated.timeScale()
                                                                                        .toBuilder();

            if ( Objects.nonNull( timeScale.getEarliestDay() ) )
            {
                scaleBuilder.setStartDay( timeScale.getEarliestDay() );
            }

            if ( Objects.nonNull( timeScale.getEarliestMonth() ) )
            {
                scaleBuilder.setStartMonth( timeScale.getEarliestMonth() );
            }

            if ( Objects.nonNull( timeScale.getLatestDay() ) )
            {
                scaleBuilder.setEndDay( timeScale.getLatestDay() );
            }

            if ( Objects.nonNull( timeScale.getLatestMonth() ) )
            {
                scaleBuilder.setEndMonth( timeScale.getLatestMonth() );
            }

            if ( Objects.nonNull( timeScale.getFrequency() ) && Objects.nonNull( timeScale.getUnit() ) )
            {
                ChronoUnit unit = ChronoUnit.valueOf( timeScale.getUnit()
                                                               .name() );
                Duration frequency = Duration.of( timeScale.getFrequency(), unit );
                builder.pairFrequency( frequency );
                LOGGER.debug( "Discovered the frequency associated with the desired time scale and migrated it to a "
                              + "paired frequency of: {}.", frequency );
            }

            TimeScale adjusted = new TimeScale( scaleBuilder.build() );
            builder.timeScale( adjusted );

            LOGGER.debug( "Migrated an evaluation timescale: {}.", adjusted );

            LenienceType lenience = timeScale.getLenient();
            TimeScaleLenience timeScaleLenience = TimeScaleLenience.valueOf( lenience.name() );
            builder.rescaleLenience( timeScaleLenience );
        }
    }

    /**
     * Migrates the unit aliases from the old declaration to the new declaration builder.
     * @param unitAliases the unit aliases
     * @param builder the new declaration builder
     */
    private static void migrateUnitAliases( List<wres.config.generated.UnitAlias> unitAliases,
                                            EvaluationDeclarationBuilder builder )
    {
        if ( Objects.nonNull( unitAliases ) && !unitAliases.isEmpty() )
        {
            LOGGER.debug( "Encountered these unit aliases to migrate: {}.", unitAliases );

            Set<UnitAlias> aliases = unitAliases.stream()
                                                .map( DeclarationFactory::migrateUnitAlias )
                                                .collect( Collectors.toSet() );

            LOGGER.debug( "Migrated these unit aliases: {}.", aliases );

            builder.unitAliases( aliases );
        }
    }

    /**
     * Migrates the cross pairing option to the new declaration builder.
     * @param crossPair the cross pairing
     * @param builder the new declaration builder
     */
    private static void migrateCrossPairing( wres.config.generated.CrossPair crossPair,
                                             EvaluationDeclarationBuilder builder )
    {
        if ( Objects.nonNull( crossPair ) )
        {
            LOGGER.debug( "Encountered cross pairing declaration to migrate: {}.", crossPair );
            CrossPair crossPairMigrated = CrossPair.FUZZY;
            if ( crossPair.isExact() )
            {
                crossPairMigrated = CrossPair.EXACT;
            }
            LOGGER.debug( "Migrated this cross-pairing option: {}.", crossPairMigrated );
            builder.crossPair( crossPairMigrated );
        }
    }

    /**
     * Migrates the value filter to the new declaration builder.
     * @param valueFilter the value filter
     * @param builder the new declaration builder
     */
    private static void migrateValueFilter( DoubleBoundsType valueFilter,
                                            EvaluationDeclarationBuilder builder )
    {
        if ( Objects.nonNull( valueFilter ) )
        {
            LOGGER.debug( "Encountered a value filter to migrate: {}.", valueFilter );
            Values values = new Values( valueFilter.getMinimum(),
                                        valueFilter.getMaximum(),
                                        valueFilter.getDefaultMinimum(),
                                        valueFilter.getDefaultMaximum() );
            LOGGER.debug( "Migrated this value filter: {}.", valueFilter );
            builder.values( values );
        }
    }

    /**
     * Migrates the metrics to the new declaration builder.
     *
     * @param projectConfig the project configuration, including the metrics configuration
     * @param builder the new declaration builder
     */
    private static void migrateMetrics( ProjectConfig projectConfig,
                                        EvaluationDeclarationBuilder builder )
    {
        List<MetricsConfig> metrics = projectConfig.getMetrics();

        if ( Objects.nonNull( metrics ) && !metrics.isEmpty() )
        {
            LOGGER.debug( "Encountered {} groups of metrics to migrate.", metrics.size() );

            // Is there a single set of thresholds? If so, migrate to top-level thresholds
            Map<wres.config.generated.ThresholdType, ThresholdsConfig> globalThresholds =
                    DeclarationFactory.getGlobalThresholds( metrics );
            boolean addThresholdsPerMetric = true;
            if ( !globalThresholds.isEmpty() )
            {
                LOGGER.debug( "Discovered these global thresholds to migrate: {}.", globalThresholds );

                addThresholdsPerMetric = false;
                DeclarationFactory.migrateGlobalThresholds( globalThresholds, builder );
            }

            // Now migrate the metrics
            DeclarationFactory.migrateMetrics( metrics, projectConfig, builder, addThresholdsPerMetric );
        }
    }

    /**
     * Migrates the output formats to the new declaration builder.
     *
     * @param outputs the output formats to migrate
     * @param builder the new declaration builder to mutate
     */
    private static void migrateOutputFormats( ProjectConfig.Outputs outputs,
                                              EvaluationDeclarationBuilder builder )
    {
        // Iterate through the destinations and migrate each one
        for ( DestinationConfig next : outputs.getDestination() )
        {
            DeclarationFactory.migrateOutputFormat( next, builder );
        }

        DurationUnit durationUnit = outputs.getDurationFormat();
        ChronoUnit timeFormat = ChronoUnit.valueOf( durationUnit.name() );
        LOGGER.debug( "Discovered a duration format to migrate: {}.", timeFormat );
        builder.durationFormat( timeFormat );
    }

    /**
     * Migrates the output format to the new declaration builder.
     *
     * @param output the output format to migrate
     * @param builder the new declaration builder to mutate
     */
    private static void migrateOutputFormat( DestinationConfig output,
                                             EvaluationDeclarationBuilder builder )
    {
        LOGGER.debug( "Migrating output format: {}.", output.getType() );

        // Migrate the decimal format
        String decimalFormat = output.getDecimalFormat();
        if ( Objects.nonNull( decimalFormat ) )
        {
            if ( Objects.isNull( builder.decimalFormat() ) )
            {
                DecimalFormatPretty format = new DecimalFormatPretty( decimalFormat );
                builder.decimalFormat( format );
                LOGGER.debug( "Discovered a decimal format to migrate: {}.", decimalFormat );
            }
            else if ( decimalFormat.replace( "#", "" )
                                   .length() > builder.decimalFormat()
                                                      .toString()
                                                      .replace( "#", "" )
                                                      .length() )
            {
                DecimalFormatPretty format = new DecimalFormatPretty( decimalFormat );
                LOGGER.warn( "Discovered more than one decimal format to migrate. The new declaration language "
                             + "supports only one format per evaluation. The existing format is {} and the new format "
                             + "is {}. Choosing {}.", builder.decimalFormat(), format, format );
                builder.decimalFormat( format );
            }
        }

        // Set the basic formats
        Outputs.Builder formatsBuilder = Outputs.newBuilder();
        if ( Objects.nonNull( builder.formats() ) )
        {
            formatsBuilder.mergeFrom( builder.formats()
                                             .outputs() );
        }

        // Set the decimal formatter for the individual output options
        Outputs.NumericFormat numericFormat = Formats.DEFAULT_NUMERIC_FORMAT;
        if ( Objects.nonNull( builder.decimalFormat() ) )
        {
            numericFormat = numericFormat.toBuilder()
                                         .setDecimalFormat( builder.decimalFormat()
                                                                   .toPattern() )
                                         .build();
        }

        switch ( output.getType() )
        {
            case CSV, NUMERIC -> formatsBuilder.setCsv( Formats.CSV_FORMAT.toBuilder()
                                                                          .setOptions( numericFormat )
                                                                          .build() );
            case CSV2 -> formatsBuilder.setCsv2( Formats.CSV2_FORMAT.toBuilder()
                                                                    .setOptions( numericFormat )
                                                                    .build() );
            case PNG, GRAPHIC -> DeclarationFactory.migratePngFormat( output, formatsBuilder, builder );
            case SVG -> DeclarationFactory.migrateSvgFormat( output, formatsBuilder, builder );
            case NETCDF -> formatsBuilder.setNetcdf( Formats.NETCDF_FORMAT );
            case NETCDF_2 -> formatsBuilder.setNetcdf2( Formats.NETCDF2_FORMAT );
            case PAIRS -> formatsBuilder.setPairs( Formats.PAIR_FORMAT.toBuilder()
                                                                      .setOptions( numericFormat )
                                                                      .build() );
            case PROTOBUF -> formatsBuilder.setProtobuf( Formats.PROTOBUF_FORMAT );
        }

        // Migrate the formats
        builder.formats( new Formats( formatsBuilder.build() ) );
    }

    /**
     * Migrates a PNG output format.
     * @param output the output
     * @param builder the builder to mutate
     * @param evaluationBuilder the evaluation builder whose metric parameters may need to be updated
     */

    private static void migratePngFormat( DestinationConfig output,
                                          Outputs.Builder builder,
                                          EvaluationDeclarationBuilder evaluationBuilder )
    {
        Outputs.PngFormat.Builder pngBuilder = Formats.PNG_FORMAT.toBuilder();
        Outputs.GraphicFormat.Builder graphicsFormatBuilder = pngBuilder.getOptions()
                                                                        .toBuilder();

        // Set any extra parameters
        GraphicalType graphics = output.getGraphical();
        if ( Objects.nonNull( graphics ) )
        {
            Set<Metric> metrics = DeclarationFactory.migrateGraphicsOptions( graphics,
                                                                             graphicsFormatBuilder,
                                                                             evaluationBuilder.metrics(),
                                                                             output.getOutputType(),
                                                                             Format.PNG );

            // Set the possibly adjusted metrics
            evaluationBuilder.metrics( metrics );
        }

        pngBuilder.setOptions( graphicsFormatBuilder );
        builder.setPng( pngBuilder );
    }

    /**
     * Migrates a SVG output format.
     * @param output the output
     * @param builder the builder to mutate
     * @param evaluationBuilder the evaluation builder whose metric parameters may need to be updated
     */

    private static void migrateSvgFormat( DestinationConfig output,
                                          Outputs.Builder builder,
                                          EvaluationDeclarationBuilder evaluationBuilder )
    {
        Outputs.SvgFormat.Builder svgBuilder = Formats.SVG_FORMAT.toBuilder();
        Outputs.GraphicFormat.Builder graphicsFormatBuilder = svgBuilder.getOptions()
                                                                        .toBuilder();

        // Set any extra parameters
        GraphicalType graphics = output.getGraphical();
        if ( Objects.nonNull( graphics ) )
        {
            Set<Metric> metrics = DeclarationFactory.migrateGraphicsOptions( graphics,
                                                                             graphicsFormatBuilder,
                                                                             evaluationBuilder.metrics(),
                                                                             output.getOutputType(),
                                                                             Format.SVG );

            // Set the possibly adjusted metrics
            evaluationBuilder.metrics( metrics );
        }

        svgBuilder.setOptions( graphicsFormatBuilder );
        builder.setSvg( svgBuilder );
    }

    /**
     * Migrates the graphical format options
     * @param graphics the graphics options.
     * @param builder the builder to mutate
     * @param metrics the metrics whose parameters may need to be updated
     * @param shape the shape of the graphic
     * @param format the format
     * @return the possibly updated metrics
     */

    private static Set<Metric> migrateGraphicsOptions( GraphicalType graphics,
                                                       Outputs.GraphicFormat.Builder builder,
                                                       Set<Metric> metrics,
                                                       OutputTypeSelection shape,
                                                       Format format )
    {
        if ( Objects.nonNull( graphics.getHeight() ) )
        {
            builder.setHeight( graphics.getHeight() );
        }

        if ( Objects.nonNull( graphics.getWidth() ) )
        {
            builder.setWidth( graphics.getWidth() );
        }

        if ( Objects.nonNull( shape ) )
        {
            Outputs.GraphicFormat.GraphicShape shapeNew = Outputs.GraphicFormat.GraphicShape.valueOf( shape.name() );
            builder.setShape( shapeNew );
            LOGGER.debug( "Migrated the graphic shape for the {} format: {}.", format, shapeNew );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            String protoString = PROTBUF_STRINGIFIER.apply( builder.build() );
            LOGGER.debug( "Migrated these graphics options for {}: {}.", format, protoString );
        }

        if ( Objects.nonNull( metrics ) && !graphics.getSuppressMetric()
                                                    .isEmpty() )
        {
            return DeclarationFactory.migrateSuppressMetrics( graphics.getSuppressMetric(), metrics, format );
        }
        else
        {
            return metrics;
        }
    }

    /**
     * Adjusts the metric parameters to suppress writing of graphics for the
     * @param suppress the list of metrics whose format parameter should be suppressed
     * @param metrics the metrics
     * @param format the format to suppress
     * @return the adjusted metrics
     */

    private static Set<Metric> migrateSuppressMetrics( List<MetricConfigName> suppress,
                                                       Set<Metric> metrics,
                                                       Format format )
    {
        // Preserve insertion order
        Set<Metric> migrated = new LinkedHashSet<>();
        Set<MetricConstants> suppressMigrated = suppress.stream()
                                                        .map( MetricConstantsFactory::from )
                                                        .collect( Collectors.toSet() );

        for ( Metric metric : metrics )
        {
            if ( suppressMigrated.contains( metric.name() ) )
            {
                LOGGER.debug( "Migrating a metric parameter for {} to suppress format {}.", metric.name(), format );

                MetricBuilder migratedMetricBuilder = MetricBuilder.builder( metric );
                MetricParametersBuilder parBuilder = MetricParametersBuilder.builder();

                // Set the existing parameters if available
                if ( Objects.nonNull( metric.parameters() ) )
                {
                    parBuilder = MetricParametersBuilder.builder( metric.parameters() );
                }

                // Suppress the format
                if ( format == Format.PNG )
                {
                    parBuilder.png( false );
                }
                else if ( format == Format.SVG )
                {
                    parBuilder.svg( false );
                }

                Metric migratedMetric = migratedMetricBuilder.parameters( parBuilder.build() )
                                                             .build();
                migrated.add( migratedMetric );
            }
            else
            {
                migrated.add( metric );
            }
        }

        return Collections.unmodifiableSet( migrated );
    }

    /**
     * Migrates the unit aliases from the old declaration to the new declaration builder.
     * @param unitAlias the unit alias to migrate
     * @return the migrated unit alias
     */
    private static UnitAlias migrateUnitAlias( wres.config.generated.UnitAlias unitAlias )
    {
        return new UnitAlias( unitAlias.getAlias(), unitAlias.getUnit() );
    }

    /**
     * Migrates a {@link PairConfig.Season} to a {@link Season}.
     * @param season the season to migrate
     * @return the migrated season
     */

    private static Season migrateSeason( PairConfig.Season season )
    {
        short startDay = 1;
        short startMonth = 1;
        short endDay = 31;
        short endMonth = 12;

        // Update defaults
        if ( season.getEarliestMonth() != 0 )
        {
            startMonth = season.getEarliestMonth();
        }
        if ( season.getEarliestDay() != 0 )
        {
            startDay = season.getEarliestDay();
        }
        if ( season.getLatestMonth() != 0 )
        {
            endMonth = season.getLatestMonth();
        }
        if ( season.getLatestDay() != 0 )
        {
            endDay = season.getLatestDay();
        }

        MonthDay earliest = MonthDay.of( startMonth, startDay );
        MonthDay latest = MonthDay.of( endMonth, endDay );

        return new Season( earliest, latest );
    }

    /**
     * Migrates from a {@link DataSourceConfig} to a {@link Dataset}.
     *
     * @param dataSource the data source
     * @return the dataset
     */

    private static Dataset migrateDataset( DataSourceConfig dataSource )
    {
        if ( Objects.isNull( dataSource ) )
        {
            LOGGER.debug( "Encountered a null data source, not migrating." );
            return null;
        }

        ZoneOffset universalZoneOffset = DeclarationFactory.migrateTimeZoneOffset( dataSource.getSource() );
        List<Source> sources = DeclarationFactory.migrateSources( dataSource.getSource(),
                                                                  dataSource.getExistingTimeScale(),
                                                                  dataSource.getUrlParameter(),
                                                                  Objects.nonNull( universalZoneOffset ) );
        EnsembleFilter filter = DeclarationFactory.migrateEnsembleFilter( dataSource.getEnsemble() );
        FeatureAuthority featureAuthority =
                DeclarationFactory.migrateFeatureAuthority( dataSource.getFeatureDimension() );
        Variable variable = DeclarationFactory.migrateVariable( dataSource.getVariable() );
        DataType type = DeclarationFactory.migrateDataType( dataSource.getType() );
        Duration timeShift = DeclarationFactory.migrateTimeShift( dataSource.getTimeShift() );

        return DatasetBuilder.builder()
                             .sources( sources )
                             .ensembleFilter( filter )
                             .featureAuthority( featureAuthority )
                             .label( dataSource.getLabel() )
                             .variable( variable )
                             .type( type )
                             .timeShift( timeShift )
                             .timeZoneOffset( universalZoneOffset )
                             .build();
    }

    /**
     * Attempts to discover and return a universal time zone offset that applies across all sources. If there are no
     * time zone offsets defined, or they vary per-source, returns null.
     * @param sources the sources to inspect
     * @return the universal time zone offset or null if no such offset applies
     */

    private static ZoneOffset migrateTimeZoneOffset( List<DataSourceConfig.Source> sources )
    {
        Set<ZoneOffset> offsets = sources.stream()
                                         .map( DataSourceConfig.Source::getZoneOffset )
                                         .filter( Objects::nonNull )
                                         .map( ZoneOffsetDeserializer::getZoneOffset )
                                         .collect( Collectors.toSet() );

        if ( offsets.size() == 1 )
        {
            ZoneOffset offset = offsets.iterator()
                                       .next();
            LOGGER.debug( "Identified a universal time zone offset of {} for these data sources: {}.",
                          offset,
                          sources );
            return offset;
        }

        LOGGER.debug( "Failed to identify a universal time zone offset for the supplied data sources. Discovered "
                      + "these time zone offsets for the individual sources:  {}. The data sources were: {}.",
                      offsets,
                      sources );

        return null;
    }

    /**
     * Migrates a collection of {@link DataSourceConfig.Source} to a collection of {@link Source}.
     *
     * @param sources the data sources
     * @param timeScale the timescale
     * @param parameters the optional URL parameters
     * @param universalTimeZoneOffset whether there is a universal time zone offset. If so, do not migrate per source
     * @return the migrated sources
     */

    private static List<Source> migrateSources( List<DataSourceConfig.Source> sources,
                                                TimeScaleConfig timeScale,
                                                List<UrlParameter> parameters,
                                                boolean universalTimeZoneOffset )
    {
        return sources.stream()
                      .map( next -> DeclarationFactory.migrateSource( next,
                                                                      timeScale,
                                                                      parameters,
                                                                      universalTimeZoneOffset ) )
                      .toList();
    }

    /**
     * Migrates a {@link DataSourceConfig.Source} to a {@link Source}.
     *
     * @param source the data sources
     * @param timeScale the timescale
     * @param parameters the optional URL parameters
     * @param universalTimeZoneOffset whether there is a universal time zone offset. If so, do not migrate per source
     * @return the migrated source
     */

    private static Source migrateSource( DataSourceConfig.Source source,
                                         TimeScaleConfig timeScale,
                                         List<UrlParameter> parameters,
                                         boolean universalTimeZoneOffset )
    {
        SourceBuilder builder = SourceBuilder.builder()
                                             .uri( source.getValue() )
                                             .pattern( source.getPattern() );

        // Do not propagate the default missing value from the old declaration
        if ( Objects.nonNull( source.getMissingValue() )
             && !DEFAULT_MISSING_STRING_OLD.equals( source.getMissingValue() ) )
        {
            double missingValue = Double.parseDouble( source.getMissingValue() );
            builder.missingValue( missingValue );
        }

        if ( Objects.nonNull( source.getInterface() ) )
        {
            String interfaceName = source.getInterface().name();
            SourceInterface sourceInterface = SourceInterface.valueOf( interfaceName );
            builder.sourceInterface( sourceInterface );
        }

        if ( !universalTimeZoneOffset && Objects.nonNull( source.getZoneOffset() ) )
        {
            ZoneOffset offset = ZoneOffsetDeserializer.getZoneOffset( source.getZoneOffset() );
            builder.timeZoneOffset( offset );
        }

        if ( Objects.nonNull( parameters ) && !parameters.isEmpty() )
        {
            Map<String, String> sourceParameters
                    = parameters.stream()
                                .collect( Collectors.toUnmodifiableMap( UrlParameter::getName,
                                                                        UrlParameter::getValue ) );
            builder.parameters( sourceParameters );
        }

        if ( Objects.nonNull( timeScale ) )
        {
            TimeScale timeScaleMigrated = DeclarationFactory.migrateTimeScale( timeScale );
            builder.timeScale( timeScaleMigrated );
        }

        return builder.build();
    }

    /**
     * Migrates a {@link NamedFeature} to a {@link GeometryTuple}.
     * @param feature the feature to migrate
     * @return the migrated feature
     */

    private static GeometryTuple migrateFeature( NamedFeature feature )
    {
        Geometry left = Geometry.newBuilder()
                                .setName( feature.getLeft() )
                                .build();

        GeometryTuple.Builder builder = GeometryTuple.newBuilder()
                                                     .setLeft( left );

        String rightName = feature.getRight();
        if ( Objects.nonNull( rightName ) )
        {
            Geometry right = Geometry.newBuilder()
                                     .setName( rightName )
                                     .build();
            builder.setRight( right );
        }
        String baselineName = feature.getBaseline();
        if ( Objects.nonNull( baselineName ) )
        {
            Geometry baseline = Geometry.newBuilder()
                                        .setName( baselineName )
                                        .build();
            builder.setBaseline( baseline );
        }

        return builder.build();
    }

    /**
     * Migrates a collection of {@link NamedFeature} to {@link GeometryTuple}.
     * @param features the geospatial features
     * @return the migrated geospatial features
     */

    private static Set<GeometryTuple> migrateFeatures( List<NamedFeature> features )
    {
        return features.stream()
                       .map( DeclarationFactory::migrateFeature )
                       .collect( Collectors.toSet() );
    }

    /**
     * Migrates a {@link FeaturePool} to a {@link GeometryGroup}.
     * @param featureGroup the feature group to migrate
     * @return the migrated feature group
     */

    private static GeometryGroup migrateFeatureGroup( FeaturePool featureGroup )
    {
        Set<GeometryTuple> members = DeclarationFactory.migrateFeatures( featureGroup.getFeature() );

        return GeometryGroup.newBuilder()
                            .setRegionName( featureGroup.getName() )
                            .addAllGeometryTuples( members )
                            .build();
    }

    /**
     * Migrates a {@link FeatureGroup} to a {@link FeatureServiceGroup}.
     * @param featureGroup the feature group to migrate
     * @return the migrated feature group
     */

    private static FeatureServiceGroup migrateFeatureServiceGroup( FeatureGroup featureGroup )
    {
        return new FeatureServiceGroup( featureGroup.getType(), featureGroup.getValue(), featureGroup.isPool() );
    }

    /**
     * Migrates a collection of features to a WKT geometry string.
     * @param features the features
     * @return the WKT string and associated SRID, if any
     */

    private static Pair<String, Long> migrateSpatialMask( List<UnnamedFeature> features )
    {
        org.locationtech.jts.geom.Geometry unionGeometry = null;
        Long srid = null;

        if ( features.isEmpty() )
        {
            LOGGER.debug( "No spatial mask features to migrate." );
            return null;
        }

        if ( features.size() > 1 )
        {
            for ( UnnamedFeature next : features )
            {
                Pair<org.locationtech.jts.geom.Geometry, Long> nextG = DeclarationFactory.migrateGeometry( next );
                org.locationtech.jts.geom.Geometry nextGeometry = nextG.getLeft();
                if ( Objects.nonNull( nextG.getRight() ) )
                {
                    srid = ( long ) nextGeometry.getSRID();
                }
                if ( Objects.isNull( unionGeometry ) && Objects.nonNull( nextGeometry ) )
                {
                    unionGeometry = nextGeometry;
                }
                else if ( Objects.nonNull( nextGeometry ) )
                {
                    unionGeometry = unionGeometry.union( nextGeometry );
                }
            }
        }
        else
        {
            Pair<org.locationtech.jts.geom.Geometry, Long> g = DeclarationFactory.migrateGeometry( features.get( 0 ) );
            unionGeometry = g.getLeft();
            srid = g.getRight();
        }

        String wkt = null;
        if ( Objects.nonNull( unionGeometry ) )
        {
            WKTWriter writer = new WKTWriter();
            wkt = writer.write( unionGeometry );
        }

        LOGGER.debug( "Migrated a spatial mask with a WKT string of {} and a SRID of {}.", wkt, srid );

        return Pair.of( wkt, srid );
    }

    /**
     * Migrates a {@link UnnamedFeature} to a {@link org.locationtech.jts.geom.Geometry}.
     * @param feature the feature
     * @return the migrated geometry and SRID, if any
     */

    private static Pair<org.locationtech.jts.geom.Geometry, Long> migrateGeometry( UnnamedFeature feature )
    {
        org.locationtech.jts.geom.Geometry geometry = null;
        Long srid = null;

        if ( Objects.nonNull( feature.getCircle() ) )
        {
            GeometricShapeFactory shapeMaker = new GeometricShapeFactory();
            Circle circle = feature.getCircle();
            shapeMaker.setWidth( circle.getDiameter() );
            shapeMaker.setBase( new CoordinateXY( circle.getLongitude(), circle.getLatitude() ) );
            geometry = shapeMaker.createCircle();
            srid = circle.getSrid()
                         .longValue();
        }
        else if ( Objects.nonNull( feature.getPolygon() ) && feature.getPolygon()
                                                                    .getPoint()
                                                                    .size() > 1 )
        {
            Polygon polygon = feature.getPolygon();
            srid = polygon.getSrid()
                          .longValue();
            List<Polygon.Point> points = polygon.getPoint();
            GeometryFactory geometryFactory = new GeometryFactory();

            List<Coordinate> coordinates = new ArrayList<>();
            for ( Polygon.Point nextPoint : points )
            {
                double longitude = Double.parseDouble( Float.toString( nextPoint.getLongitude() ) );
                double latitude = Double.parseDouble( Float.toString( nextPoint.getLatitude() ) );
                Coordinate coordinate = new CoordinateXY( longitude, latitude );
                coordinates.add( coordinate );
            }

            // Close the polygon
            coordinates.add( coordinates.get( 0 ) );

            geometry = geometryFactory.createPolygon( coordinates.toArray( new Coordinate[1] ) );
        }
        else if ( Objects.nonNull( feature.getCoordinate() ) )
        {
            LOGGER.warn( "Discovered a spatial coordinate associated with a grid selection, which is not a valid area "
                         + "selection. This spatial coordinate declaration will not be migrated. If you want to "
                         + "include a grid selection, please remove the single coordinate and add a polygon or "
                         + "circle." );
        }

        return Pair.of( geometry, srid );
    }

    /**
     * Migrates a collection of {@link EnsembleCondition} to an {@link EnsembleFilter}. Note that the old-style
     * declaration schema is incoherent because it allows for inclusion/exclusion at the level of individual members,
     * whereas the new style declaration requires that all members have the same included/excluded state. If an old-
     * style declaration contains both included/excluded members in the same declaration it is redundant at best and
     * incoherent at worst. It is incoherent when the excluded members are part of the subset of included members, and
     * it is superfluous otherwise (since all members that are not included are, by definition, excluded). In this case,
     * a warning will be emitted and only the excluded members transferred.
     *
     * @param filters the ensemble filters
     * @return the migrated ensemble filter
     */

    private static EnsembleFilter migrateEnsembleFilter( List<EnsembleCondition> filters )
    {
        if ( Objects.isNull( filters ) || filters.isEmpty() )
        {
            LOGGER.debug( "Encountered a null or empty collection of ensemble filters, not migrating." );
            return null;
        }

        Set<String> included = filters.stream()
                                      .filter( next -> !next.isExclude() )
                                      .map( EnsembleCondition::getName )
                                      .collect( Collectors.toSet() );

        Set<String> excluded = filters.stream()
                                      .filter( EnsembleCondition::isExclude )
                                      .map( EnsembleCondition::getName )
                                      .collect( Collectors.toSet() );

        EnsembleFilterBuilder builder = EnsembleFilterBuilder.builder();

        // Both included and excluded filters, which is not coherent, but does not produce an error here because the
        // old-style declaration allows it
        if ( !included.isEmpty() && !excluded.isEmpty() )
        {
            LOGGER.warn( "The original declaration requests that some ensemble members are included and some are "
                         + "excluded. The members to include are: {}. The members to exclude are: {}. This is "
                         + "probably not coherent because all members outside of the exclude list are included, "
                         + "by definition. Thus, only the excluded members will be migrated.", included, excluded );
            builder.members( excluded ).exclude( true );
        }
        // One or other are present
        else
        {
            Set<String> members = new HashSet<>( excluded );
            members.addAll( included );
            builder.members( members ).exclude( included.isEmpty() );
        }

        return builder.build();
    }

    /**
     * Migrates a {@link TimeScaleConfig} to a {@link TimeScale}.
     *
     * @param timeScaleConfig the timescale
     * @return the migrated timescale
     */

    private static TimeScale migrateTimeScale( TimeScaleConfig timeScaleConfig )
    {
        if ( Objects.isNull( timeScaleConfig ) )
        {
            LOGGER.debug( "Encountered a missing time scale, not migrating." );
            return null;
        }

        wres.statistics.generated.TimeScale.Builder timeScaleInner = wres.statistics.generated.TimeScale.newBuilder();

        if ( Objects.nonNull( timeScaleConfig.getPeriod() ) )
        {
            java.time.Duration period = ProjectConfigs.getDurationFromTimeScale( timeScaleConfig );
            com.google.protobuf.Duration canonicalPeriod = com.google.protobuf.Duration.newBuilder()
                                                                                       .setSeconds( period.getSeconds() )
                                                                                       .setNanos( period.getNano() )
                                                                                       .build();
            timeScaleInner.setPeriod( canonicalPeriod );
        }

        if ( Objects.isNull( timeScaleConfig.getFunction() ) )
        {
            timeScaleInner.setFunction( wres.statistics.generated.TimeScale.TimeScaleFunction.UNKNOWN );
        }
        else
        {
            wres.statistics.generated.TimeScale.TimeScaleFunction innerFunction =
                    wres.statistics.generated.TimeScale.TimeScaleFunction.valueOf( timeScaleConfig.getFunction()
                                                                                                  .name() );
            timeScaleInner.setFunction( innerFunction );
        }

        return TimeScaleBuilder.builder()
                               .timeScale( timeScaleInner.build() )
                               .build();
    }

    /**
     * Migrates a {@link FeatureDimension} to a {@link FeatureAuthority}.
     *
     * @param featureDimension the feature dimension
     * @return the feature authority
     */

    private static FeatureAuthority migrateFeatureAuthority( FeatureDimension featureDimension )
    {
        FeatureAuthority featureAuthority = null;

        if ( Objects.nonNull( featureDimension ) )
        {
            featureAuthority = FeatureAuthority.valueOf( featureDimension.name() );
        }
        return featureAuthority;
    }

    /**
     * Migrates a {@link DataSourceConfig.Variable} to a {@link Variable}.
     *
     * @param variable the variable
     * @return the migrated variable
     */

    private static Variable migrateVariable( DataSourceConfig.Variable variable )
    {
        Variable migrated = null;

        if ( Objects.nonNull( variable ) )
        {
            migrated = VariableBuilder.builder()
                                      .name( variable.getValue() )
                                      .label( variable.getLabel() )
                                      .build();
        }
        return migrated;
    }

    /**
     * Migrates a {@link DatasourceType} to a {@link DataType}.
     *
     * @param type the data source type
     * @return the migrated type
     */

    private static DataType migrateDataType( DatasourceType type )
    {
        DataType dataType = null;

        if ( Objects.nonNull( type ) )
        {
            dataType = DataType.valueOf( type.name() );
        }
        return dataType;
    }

    /**
     * Migrates a {@link DataSourceConfig.TimeShift} to a {@link Duration}.
     *
     * @param timeShift the time shift
     * @return the migrated time shift
     */

    private static Duration migrateTimeShift( DataSourceConfig.TimeShift timeShift )
    {
        Duration duration = null;

        if ( Objects.nonNull( timeShift ) )
        {
            String unitName = timeShift.getUnit().name();
            ChronoUnit chronoUnit = ChronoUnit.valueOf( unitName );
            duration = Duration.of( timeShift.getWidth(), chronoUnit );
        }
        return duration;
    }

    /**
     * Migrates a {@link DateCondition} to a {@link TimeInterval}.
     * @param dateCondition the date condition
     * @return the time interval
     */
    private static TimeInterval migrateTimeInterval( DateCondition dateCondition )
    {
        Instant earliest = null;
        Instant latest = null;

        if ( Objects.nonNull( dateCondition.getEarliest() ) )
        {
            earliest = Instant.parse( dateCondition.getEarliest() );
        }

        if ( Objects.nonNull( dateCondition.getLatest() ) )
        {
            latest = Instant.parse( dateCondition.getLatest() );
        }

        return new TimeInterval( earliest, latest );
    }

    /**
     * Migrates a {@link TimePools} to a {@link PoolingWindowConfig}.
     * @param poolingWindow the pooling window
     * @return the time pool
     */
    private static TimePools migrateTimePools( PoolingWindowConfig poolingWindow )
    {
        ChronoUnit unit = ChronoUnit.valueOf( poolingWindow.getUnit()
                                                           .name() );
        return new TimePools( poolingWindow.getPeriod(), poolingWindow.getFrequency(), unit );
    }

    /**
     * Migrates the thresholds to the declaration builder.
     * @param thresholds the thresholds to migrate
     * @param builder the declaration builder
     */

    private static void migrateGlobalThresholds( Map<wres.config.generated.ThresholdType, ThresholdsConfig> thresholds,
                                                 EvaluationDeclarationBuilder builder )
    {
        // Probability thresholds
        if ( thresholds.containsKey( wres.config.generated.ThresholdType.PROBABILITY ) )
        {
            ThresholdsConfig probabilityThresholdsConfig =
                    thresholds.get( wres.config.generated.ThresholdType.PROBABILITY );
            Set<Threshold> probabilityThresholds = DeclarationFactory.migrateThresholds( probabilityThresholdsConfig,
                                                                                         builder );
            builder.probabilityThresholds( probabilityThresholds );
            LOGGER.debug( "Migrated these probability thresholds for all metrics: {}.", probabilityThresholds );
        }

        // Value thresholds
        if ( thresholds.containsKey( wres.config.generated.ThresholdType.VALUE ) )
        {
            ThresholdsConfig valueThresholdsConfig =
                    thresholds.get( wres.config.generated.ThresholdType.VALUE );
            Set<Threshold> valueThresholds = DeclarationFactory.migrateThresholds( valueThresholdsConfig,
                                                                                   builder );
            builder.valueThresholds( valueThresholds );
            LOGGER.debug( "Migrated these value thresholds for all metrics: {}.", valueThresholds );
        }

        // Probability classifier thresholds
        if ( thresholds.containsKey( wres.config.generated.ThresholdType.PROBABILITY_CLASSIFIER ) )
        {
            ThresholdsConfig classifierThresholdsConfig =
                    thresholds.get( wres.config.generated.ThresholdType.PROBABILITY_CLASSIFIER );
            Set<Threshold> classifierThresholds = DeclarationFactory.migrateThresholds( classifierThresholdsConfig,
                                                                                        builder );
            builder.classifierThresholds( classifierThresholds );
            LOGGER.debug( "Migrated these classifier thresholds for all metrics: {}.", classifierThresholds );
        }
    }

    /**
     * Migrates the metrics. Thresholds are only migrated on request. For example, if global thresholds are present,
     * these can be migrated to the top-level threshold buckets, rather than registered with each metric.
     *
     * @param metrics the metrics to migrate
     * @param projectConfig the overall declaration used as context when migrating metrics
     * @param builder the builder to mutate
     * @param addThresholdsPerMetric whether the thresholds declared in each metric group should be added to each metric
     */
    private static void migrateMetrics( List<MetricsConfig> metrics,
                                        ProjectConfig projectConfig,
                                        EvaluationDeclarationBuilder builder,
                                        boolean addThresholdsPerMetric )
    {
        // Iterate through each metric group
        for ( MetricsConfig next : metrics )
        {
            // In general "all valid" metrics means no explicit migration since no metrics means "all valid" in the new
            // language. However, there is one exception: when the old language suppresses graphics formats on a per-
            // metric basis, then all metrics must be explicitly migrated because the new format records this as a
            // metric parameter
            if ( DeclarationFactory.hasExplicitMetrics( next )
                 || DeclarationFactory.hasSuppressedGraphics( projectConfig ) )
            {
                LOGGER.debug( "Discovered metrics to migrate from the following declaration: {}.", next );

                Set<MetricConstants> metricNames = MetricConstantsFactory.getMetricsFromConfig( next, projectConfig );

                // Acquire the parameters that apply to all metrics in this group
                MetricParameters groupParameters = DeclarationFactory.migrateMetricParameters( next,
                                                                                               builder,
                                                                                               addThresholdsPerMetric );

                // Increment the metrics, preserving insertion order
                Set<Metric> overallMetrics = new LinkedHashSet<>();
                if ( Objects.nonNull( builder.metrics() ) )
                {
                    overallMetrics.addAll( builder.metrics() );
                }

                // Acquire and set the parameters that apply to individual metrics, combining with the group parameters
                Set<Metric> innerMetrics = DeclarationFactory.migrateMetricSpecificParameters( metricNames,
                                                                                               groupParameters );

                overallMetrics.addAll( innerMetrics );

                LOGGER.debug( "Adding these migrated metrics to the metric store, which now contains {} metrics: {}.",
                              overallMetrics.size(), innerMetrics );

                builder.metrics( overallMetrics );
            }
            else
            {
                LOGGER.debug( "The following metrics declaration had no explicit metrics to migrate: {}", next );
            }
        }
    }

    /**
     * Migrates the metric-specific parameters, appending them to the input parameters. The only metric-specific
     * parameters are timing error summary statistics, which can be gleaned from the metric names, since these include
     * both the overall metrics and the summary statistics for timing error metrics.
     *
     * @param metricNames the metric names
     * @param parameters the existing parameters that should be incremented
     * @return the adjusted metrics with parameters
     */

    private static Set<Metric> migrateMetricSpecificParameters( Set<MetricConstants> metricNames,
                                                                MetricParameters parameters )
    {
        Set<Metric> returnMe = new LinkedHashSet<>();
        // Iterate through the metrics, increment the parameters and set them
        for ( MetricConstants next : metricNames )
        {
            // Add the parameters for duration diagrams
            if ( next.isInGroup( MetricConstants.StatisticType.DURATION_DIAGRAM ) )
            {
                MetricParametersBuilder builder = MetricParametersBuilder.builder();
                if ( Objects.nonNull( parameters ) )
                {
                    builder = MetricParametersBuilder.builder( parameters );
                }
                Set<MetricConstants> durationScores
                        = metricNames.stream()
                                     // Find the duration scores whose parent is the diagram in question
                                     .filter( m -> m.getParent() == next )
                                     // Get the child, which is a univariate measure
                                     .map( MetricConstants::getChild )
                                     .collect( Collectors.toSet() );
                builder.summaryStatistics( durationScores );
                returnMe.add( new Metric( next, builder.build() ) );

                LOGGER.debug( "Migrated these summary statistics for the {}: {}.",
                              next,
                              durationScores );
            }
            // Ignore duration scores, which are parameters of duration diagrams and add the other metrics as-is
            else if ( !next.isInGroup( MetricConstants.StatisticType.DURATION_SCORE ) )
            {
                returnMe.add( new Metric( next, parameters ) );
            }
        }

        return Collections.unmodifiableSet( returnMe );
    }

    /**
     * @param metrics the metrics to test
     * @return whether there are some explicitly declared metrics
     */

    private static boolean hasExplicitMetrics( MetricsConfig metrics )
    {
        return !( metrics.getMetric()
                         .stream()
                         .allMatch( next -> next.getName() == MetricConfigName.ALL_VALID )
                  && metrics.getTimeSeriesMetric()
                            .stream()
                            .allMatch( next -> next.getName() == TimeSeriesMetricConfigName.ALL_VALID ) );
    }

    /**
     * @param projectConfig the declaration to inspect
     * @return whether some graphics formats suppress individual metrics
     */
    private static boolean hasSuppressedGraphics( ProjectConfig projectConfig )
    {
        return projectConfig.getOutputs()
                            .getDestination()
                            .stream()
                            .anyMatch( next -> Objects.nonNull( next.getGraphical() )
                                               && !next.getGraphical()
                                                       .getSuppressMetric()
                                                       .isEmpty() );
    }

    /**
     * Migrates the parameters in the input to a {@link MetricParameters}.
     *
     * @param metric the metric whose parameters should be read
     * @param builder the builder, which may be updated with threshold service declaration
     * @param addThresholdsPerMetric whether the thresholds declared in each metric group should be added to each metric
     * @return the migrated metric parameters
     */
    private static MetricParameters migrateMetricParameters( MetricsConfig metric,
                                                             EvaluationDeclarationBuilder builder,
                                                             boolean addThresholdsPerMetric )
    {
        MetricParametersBuilder parametersBuilder = MetricParametersBuilder.builder();

        // Set the thresholds, if needed
        if ( addThresholdsPerMetric && Objects.nonNull( metric.getThresholds() ) )
        {
            List<ThresholdsConfig> thresholds = metric.getThresholds();
            Set<Threshold> migratedThresholds = thresholds.stream()
                                                          .map( next -> DeclarationFactory.migrateThresholds( next,
                                                                                                              builder ) )
                                                          .flatMap( Set::stream )

                                                          .collect( Collectors.toSet() );
            // Group by threshold type
            Map<wres.config.yaml.components.ThresholdType, Set<Threshold>> grouped
                    = DeclarationFactory.groupThresholdsByType( migratedThresholds );

            // Set the thresholds
            if ( grouped.containsKey( ThresholdType.PROBABILITY ) )
            {
                parametersBuilder.probabilityThresholds( grouped.get( ThresholdType.PROBABILITY ) );
            }
            if ( grouped.containsKey( ThresholdType.VALUE ) )
            {
                parametersBuilder.valueThresholds( grouped.get( ThresholdType.VALUE ) );
            }
            if ( grouped.containsKey( ThresholdType.PROBABILITY_CLASSIFIER ) )
            {
                parametersBuilder.classifierThresholds( grouped.get( ThresholdType.PROBABILITY_CLASSIFIER ) );
            }
        }

        // Set the minimum sample size
        parametersBuilder.minimumSampleSize( metric.getMinimumSampleSize() );

        // Set the ensemble average
        if ( Objects.nonNull( metric.getEnsembleAverage() ) )
        {
            Pool.EnsembleAverageType average = Pool.EnsembleAverageType.valueOf( metric.getEnsembleAverage()
                                                                                       .name() );
            parametersBuilder.ensembleAverageType( average );
        }

        MetricParameters migratedParameters = parametersBuilder.build();

        // Do not set default parameters
        if ( DEFAULT_METRIC_PARAMETERS.equals( migratedParameters ) )
        {
            LOGGER.debug( "While migrating parameters, discovered default parameters values, which will not be "
                          + "migrated explicitly. The original declaration to migrate was: {}.", metric );
            migratedParameters = null;
        }

        return migratedParameters;
    }

    /**
     * Migrates old-style declaration of thresholds to canonical thresholds.
     * @param thresholds the thresholds to migrate
     * @param builder the builder, which may be updated with threshold service declaration
     * @return the migrated thresholds
     */

    private static Set<Threshold> migrateThresholds( ThresholdsConfig thresholds,
                                                     EvaluationDeclarationBuilder builder )
    {
        // Either an internal list of thresholds in CSV format or an external point, such as a file or service call,
        // which are not migrated here
        Object internalOrExternal = thresholds.getCommaSeparatedValuesOrSource();

        // Concrete thresholds?
        if ( internalOrExternal instanceof String csv )
        {
            return DeclarationFactory.migrateCsvThresholds( csv, thresholds );
        }
        // External source of thresholds call to obtain concrete thresholds
        else if ( internalOrExternal instanceof ThresholdsConfig.Source source )
        {
            return DeclarationFactory.migrateExternalThresholds( source, thresholds, builder );
        }

        LOGGER.warn( "Discovered an unrecognized threshold source, which will not be migrated: {}.", thresholds );
        return Collections.emptySet();
    }

    /**
     * Migrates a CSV-formatted threshold string.
     *
     * @param thresholds the threshold string
     * @param metadata the threshold metadata
     * @return the thresholds
     */
    private static Set<Threshold> migrateCsvThresholds( String thresholds, ThresholdsConfig metadata )
    {
        LOGGER.debug( "Discovered a source of thresholds in CSV format to migrate: {}.", metadata );
        Set<Threshold> migrated = new LinkedHashSet<>();

        // Default threshold
        wres.statistics.generated.Threshold.Builder builder = DEFAULT_CANONICAL_THRESHOLD.toBuilder();

        // Need to map enums
        wres.config.generated.ThresholdType type = DeclarationFactory.getThresholdType( metadata );
        ThresholdType newType = ThresholdType.valueOf( type.name() );

        if ( Objects.nonNull( metadata.getOperator() ) )
        {
            wres.config.generated.ThresholdOperator operator = metadata.getOperator();
            wres.statistics.generated.Threshold.ThresholdOperator canonicalOperator =
                    ProjectConfigs.getThresholdOperator( operator );
            builder.setOperator( canonicalOperator );
        }

        if ( Objects.nonNull( metadata.getApplyTo() ) )
        {
            ThresholdDataType dataType = metadata.getApplyTo();
            ThresholdOrientation orientation = ThresholdOrientation.valueOf( dataType.name() );
            wres.statistics.generated.Threshold.ThresholdDataType canonicalDataType = orientation.canonical();
            builder.setDataType( canonicalDataType );
        }

        // Read the threshold values
        double[] values = Arrays.stream( thresholds.split( "," ) )
                                .mapToDouble( Double::parseDouble )
                                .toArray();

        // Create the thresholds
        for ( double next : values )
        {
            // Clear existing values
            builder.clearLeftThresholdValue()
                   .clearLeftThresholdProbability();

            DoubleValue value = DoubleValue.of( next );

            if ( newType == ThresholdType.VALUE )
            {
                builder.setLeftThresholdValue( value );
            }
            else
            {
                builder.setLeftThresholdProbability( value );
            }

            wres.statistics.generated.Threshold migratedThreshold = builder.build();
            Threshold nextThreshold = ThresholdBuilder.builder()
                                                      .threshold( migratedThreshold )
                                                      .type( newType )
                                                      .build();
            migrated.add( nextThreshold );
        }

        LOGGER.debug( "Migrated the following thresholds: {}.", migrated );

        return Collections.unmodifiableSet( migrated );
    }

    /**
     * Attempts to migrate an external source of thresholds. If the thresholds are contained in a sidecar file, these
     * are read and returned. If a threshold service is configured, the service declaration is migrated, but no
     * thresholds are read.
     *
     * @param thresholdSource the external source of thresholds
     * @param thresholds the thresholds
     * @param builder the builder, which may be updated with threshold service declaration
     * @return the thresholds, if any
     */

    private static Set<Threshold> migrateExternalThresholds( ThresholdsConfig.Source thresholdSource,
                                                             ThresholdsConfig thresholds,
                                                             EvaluationDeclarationBuilder builder )
    {
        LOGGER.debug( "Discovered an external source of thresholds to migrate: {}.", thresholds );

        wres.config.generated.ThresholdType thresholdType = DeclarationFactory.getThresholdType( thresholds );
        ThresholdType canonicalType = ThresholdType.valueOf( thresholdType.name() );

        // Web service?
        if ( thresholdSource.getFormat() == ThresholdFormat.WRDS )
        {
            LOGGER.debug( "Discovered threshold service declaration to migrate: {}.", thresholdSource );

            DatasetOrientation orientation = DatasetOrientation.LEFT;
            double missingValue = -999.0;  // The default in the legacy schema
            if ( Objects.nonNull( thresholdSource.getFeatureNameFrom() ) )
            {
                orientation = DatasetOrientation.valueOf( thresholdSource.getFeatureNameFrom()
                                                                         .name() );
            }
            if ( Objects.nonNull( thresholdSource.getMissingValue() ) )
            {
                missingValue = Double.parseDouble( thresholdSource.getMissingValue() );
            }

            ThresholdService thresholdService
                    = ThresholdServiceBuilder.builder()
                                             .uri( thresholdSource.getValue() )
                                             .parameter( thresholdSource.getParameterToMeasure() )
                                             .provider( thresholdSource.getProvider() )
                                             .ratingProvider( thresholdSource.getRatingProvider() )
                                             .featureNameFrom( orientation )
                                             .unit( thresholdSource.getUnit() )
                                             .missingValue( missingValue )
                                             .build();

            // Set the declaration
            builder.thresholdService( thresholdService );

            LOGGER.debug( "Migrated this threshold service declaration: {}.", thresholdService );

            return Collections.emptySet();
        }
        // Attempt to read a source of thresholds in CSV format
        else
        {
            try
            {
                String unit = builder.unit();
                if ( Objects.nonNull( thresholdSource.getUnit() ) && !thresholdSource.getUnit()
                                                                                     .isBlank() )
                {
                    unit = thresholdSource.getUnit();
                }

                // Defaults to left in the old-style declaration schema
                DatasetOrientation featureNameFrom = DatasetOrientation.valueOf( thresholdSource.getFeatureNameFrom()
                                                                                                .name() );

                Set<Threshold> migrated = new LinkedHashSet<>();
                Map<String, Set<wres.statistics.generated.Threshold>> external =
                        CsvThresholdReader.readThresholds( thresholds, unit );
                for ( Map.Entry<String, Set<wres.statistics.generated.Threshold>> nextEntry : external.entrySet() )
                {
                    String featureName = nextEntry.getKey();
                    Geometry feature = Geometry.newBuilder()
                                               .setName( featureName )
                                               .build();
                    Set<wres.statistics.generated.Threshold> toMigrate = nextEntry.getValue();
                    Set<Threshold> innerMigrated = toMigrate.stream()
                                                            .map( next -> new Threshold( next,
                                                                                         canonicalType,
                                                                                         feature,
                                                                                         featureNameFrom ) )
                                                            .collect( Collectors.toCollection( LinkedHashSet::new ) );
                    migrated.addAll( innerMigrated );
                }

                if ( LOGGER.isTraceEnabled() )
                {
                    LOGGER.trace( "Read these thresholds from {}: {}.", thresholdSource.getValue(), migrated );
                }

                return Collections.unmodifiableSet( migrated );
            }
            catch ( IOException e )
            {
                throw new DeclarationException( "Encountered an error when attempting to migrate an external source "
                                                + "of thresholds from " + thresholdSource.getValue(), e );
            }
        }
    }

    /**
     * Inspects the metrics and looks for a single/global set of thresholds containing no more than one set of each
     * type from {@link wres.config.yaml.components.ThresholdType}. If there are no global thresholds, returns an empty
     * list. In that case, no thresholds are defined or there are different thresholds for different groups of metrics.
     *
     * @param metrics the metrics to inspect
     * @return the global thresholds, if available
     */

    private static Map<wres.config.generated.ThresholdType, ThresholdsConfig> getGlobalThresholds( List<MetricsConfig> metrics )
    {
        Map<wres.config.generated.ThresholdType, ThresholdsConfig> thresholdsMap =
                new EnumMap<>( wres.config.generated.ThresholdType.class );
        for ( MetricsConfig nextMetrics : metrics )
        {
            List<ThresholdsConfig> thresholds = nextMetrics.getThresholds();
            for ( ThresholdsConfig nextThresholds : thresholds )
            {
                wres.config.generated.ThresholdType nextType = DeclarationFactory.getThresholdType( nextThresholds );
                if ( thresholdsMap.containsKey( nextType )
                     && !thresholdsMap.get( nextType )
                                      .equals( nextThresholds ) )
                {
                    return Collections.emptyMap();
                }
                else
                {
                    thresholdsMap.put( nextType, nextThresholds );
                }
            }
        }

        return Collections.unmodifiableMap( thresholdsMap );
    }

    /**
     * @param thresholds the thresholds
     * @return the threshold type
     */
    private static wres.config.generated.ThresholdType getThresholdType( ThresholdsConfig thresholds )
    {
        // Defaults to probability
        wres.config.generated.ThresholdType thresholdType = wres.config.generated.ThresholdType.PROBABILITY;
        if ( Objects.nonNull( thresholds.getType() ) )
        {
            thresholdType = thresholds.getType();
        }
        return thresholdType;
    }

    /**
     * Custom resolver for YAML strings that ignores times.
     */
    private static class CustomResolver extends Resolver
    {
        /*
         * Do not add a resolver for times.
         */
        @Override
        protected void addImplicitResolvers()
        {
            addImplicitResolver( Tag.BOOL, BOOL, "yYnNtTfFoO" );
            addImplicitResolver( Tag.FLOAT, FLOAT, "-+0123456789." );
            addImplicitResolver( Tag.INT, INT, "-+0123456789" );
            addImplicitResolver( Tag.MERGE, MERGE, "<" );
            addImplicitResolver( Tag.NULL, NULL, "~nN\0" );
            addImplicitResolver( Tag.NULL, EMPTY, null );
        }
    }

    /**
     * Do not construct.
     */

    private DeclarationFactory()
    {
    }
}
