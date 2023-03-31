package wres.config.yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.MonthDay;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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
import wres.config.yaml.components.FormatsBuilder;
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
import wres.statistics.generated.DurationScoreMetric;
import wres.statistics.generated.Geometry;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
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
 * using {@link #from(String)}. Second, options that are declared "implicitly" are resolved via interpolation. The
 * interpolation of missing declaration may be performed in stages, depending on when it is required and how it is
 * informed. For example, the {@link #interpolate(EvaluationDeclaration)} performs minimal interpolation that is
 * informed by the declaration alone, such as the interpolation of each time-series data "type" when not declared
 * explicitly and the interpolation of metrics to evaluate when not declared explicitly (which is, in turn, informed by
 * the data "type"). Currently, {@link #interpolate(EvaluationDeclaration)} does not perform any external service
 * calls. For example, features and thresholds may be declared implicitly using a feature service or a threshold
 * service, respectively. The resulting features and thresholds are not resolved into explicit descriptions of the same
 * options. It is assumed that another module (wres-io) resolves these attributes.
 *
 * @author James Brown
 */

public class DeclarationFactory
{
    /** Default canonical threshold with no values. */
    public static final wres.statistics.generated.Threshold DEFAULT_CANONICAL_THRESHOLD
            = wres.statistics.generated.Threshold.newBuilder()
                                                 .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT )
                                                 .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                 .build();
    /** Default wrapped threshold. */
    public static final Threshold DEFAULT_THRESHOLD = new Threshold( DEFAULT_CANONICAL_THRESHOLD, null, null );

    /** All data threshold. */
    public static final Threshold ALL_DATA_THRESHOLD =
            new Threshold( wres.statistics.generated.Threshold.newBuilder()
                                                              .setLeftThresholdValue( DoubleValue.of( Double.NEGATIVE_INFINITY ) )
                                                              .setOperator( wres.statistics.generated.Threshold.ThresholdOperator.GREATER )
                                                              .setDataType( wres.statistics.generated.Threshold.ThresholdDataType.LEFT_AND_RIGHT )
                                                              .build(),
                           wres.config.yaml.components.ThresholdType.VALUE,
                           null );

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

    /**
     * Deserializes a YAML string into a POJO and performs validation against the schema. Optionally performs
     * interpolation of missing declaration, followed by validation of the interpolated declaration. Interpolation is
     * performed with {@link #interpolate(EvaluationDeclaration)} and validation is performed with
     * {@link DeclarationValidator#validateAndNotify(EvaluationDeclaration)}.
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
            declaration = DeclarationFactory.interpolate( declaration );
            DeclarationValidator.validateAndNotify( declaration );
        }

        return declaration;
    }

    /**
     * Deserializes a YAML string into a POJO and performs validation against the schema only. Does not "interpolate"
     * any missing declaration options that may be gleaned from other declaration. To perform "interpolation", use
     * {@link #interpolate(EvaluationDeclaration)}. Also, does not perform any high-level validation of the
     * declaration for mutual consistency and coherence (aka "business logic"). To perform high-level validation, see
     * the {@link DeclarationValidator}.
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

            // Unfortunately, Jackson does not currently resolve anchors/aliases, despite using SnakeYAML under the
            // hood. Instead, use SnakeYAML to create a resolved string first. This is inefficient and undesirable
            // because it means  that the declaration string is being read twice
            // TODO: use Jackson to read the raw YAML string once it can handle anchors/aliases properly
            Yaml snakeYaml = new Yaml( new Constructor( new LoaderOptions() ),
                                       new Representer( new DumperOptions() ),
                                       new DumperOptions(),
                                       new CustomResolver() );
            Object resolvedYaml = snakeYaml.load( yaml );
            String resolvedYamlString =
                    DESERIALIZER.writerWithDefaultPrettyPrinter().writeValueAsString( resolvedYaml );

            LOGGER.info( "Resolved a YAML declaration string: {}.", resolvedYaml );

            // Use Jackson to (re-)read the declaration string once any aliases/anchors are resolved
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
     * Interpolates "missing" declaration from the available declaration. Currently, this method does not interpolate
     * any declaration that requires service calls, such as features that are resolved by a feature service or
     * thresholds that are resolved by a threshold service. This method can also be viewed as completing or
     * "densifying" the declaration, based on hints provided by a user.
     *
     * @param declaration the raw declaration to interpolate
     * @return the interpolated declaration
     */
    public static EvaluationDeclaration interpolate( EvaluationDeclaration declaration )
    {
        EvaluationDeclarationBuilder adjustedDeclarationBuilder = EvaluationDeclarationBuilder.builder( declaration );

        // Disambiguate the "type" of data when it is not declared
        DeclarationFactory.interpolateDataTypes( adjustedDeclarationBuilder );
        // Interpolate geospatial features when required, but without using a feature service
        DeclarationFactory.interpolateFeaturesWithoutFeatureService( adjustedDeclarationBuilder );
        // Interpolate evaluation metrics when required
        DeclarationFactory.interpolateMetrics( adjustedDeclarationBuilder );
        // interpolate the evaluation-wide decimal format string for each numeric format type
        DeclarationFactory.interpolateDecimalFormatforNumericFormats( adjustedDeclarationBuilder );
        // Interpolate the metrics to ignore for each graphics format
        DeclarationFactory.interpolateMetricsToOmitFromGraphicsFormats( adjustedDeclarationBuilder );
        // Interpolate the graphics formats from the metric parameters that declare them
        DeclarationFactory.interpolateGraphicsFormatsFromMetricParameters( adjustedDeclarationBuilder );
        // Interpolate the measurement units for value thresholds when they have not been declared explicitly
        DeclarationFactory.interpolateMeasurementUnitForValueThresholds( adjustedDeclarationBuilder );
        // Interpolate thresholds for individual metrics without thresholds, adding an "all data" threshold as needed
        DeclarationFactory.interpolateThresholdsForIndividualMetrics( adjustedDeclarationBuilder );
        // Interpolate output formats where none exist
        DeclarationFactory.interpolateOutputFormats( adjustedDeclarationBuilder );

        return adjustedDeclarationBuilder.build();
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
     * Creates a {@link wres.statistics.generated.Threshold.ThresholdDataType} from a string.
     * @param name the type name
     * @return the threshold type
     * @throws NullPointerException if the input is null
     */

    public static wres.statistics.generated.Threshold.ThresholdDataType getThresholdDataType( String name )
    {
        Objects.requireNonNull( name );

        String dataTypeString = name.replace( " ", "_" )
                                    .toUpperCase()
                                    .replace( "OBSERVED", "LEFT" )
                                    .replace( "PREDICTED", "RIGHT" );
        return wres.statistics.generated.Threshold.ThresholdDataType.valueOf( dataTypeString );
    }

    /**
     * Creates a friendly string name from a {@link wres.statistics.generated.Threshold.ThresholdDataType}. The
     * reverse of {@link DeclarationFactory#getThresholdDataType(String)}.
     *
     * @param dataType the data type
     * @return a friendly string for use in declaration
     * @throws NullPointerException if the input is null
     */

    public static String getThresholdDataTypeName( wres.statistics.generated.Threshold.ThresholdDataType dataType )
    {
        Objects.requireNonNull( dataType );

        return dataType.name()
                       .replace( "_", " " )
                       .toLowerCase()
                       .replace( "left", "observed" )
                       .replace( "right", "predicted" );
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

    private static Set<String> getEnsembleDeclaration( EvaluationDeclarationBuilder builder )
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

        // Ensemble metrics?
        Set<String> ensembleMetrics =
                DeclarationFactory.getMetricType( builder, MetricConstants.SampleDataGroup.ENSEMBLE );
        if ( !ensembleMetrics.isEmpty() )
        {
            ensembleDeclaration.add( "Discovered metrics that require ensemble forecasts: " + ensembleMetrics + "." );
        }

        // Discrete probability metrics?
        Set<String> discreteProbabilityMetrics =
                DeclarationFactory.getMetricType( builder, MetricConstants.SampleDataGroup.DISCRETE_PROBABILITY );
        if ( !discreteProbabilityMetrics.isEmpty() )
        {
            ensembleDeclaration.add( "Discovered metrics that focus on discrete probability forecasts and these can "
                                     + "only be obtained from ensemble forecasts, currently: "
                                     + discreteProbabilityMetrics + "." );
        }

        return Collections.unmodifiableSet( ensembleDeclaration );
    }

    /**
     * @param builder the builder
     * @return whether analysis durations have been declared
     */
    private static boolean hasAnalysisDurations( EvaluationDeclarationBuilder builder )
    {
        return Objects.nonNull( builder.analysisDurations() ) && (
                Objects.nonNull( builder.analysisDurations().minimumExclusive() )
                || Objects.nonNull( builder.analysisDurations().maximum() ) );
    }

    /**
     * Returns a string representation of each forecast declaration item discovered.
     * @param builder the builder
     * @return the forecast declaration strings, if any
     */
    private static Set<String> getForecastDeclaration( EvaluationDeclarationBuilder builder )
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
     * Adds autogenerated features to the declaration.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateFeaturesWithoutFeatureService( EvaluationDeclarationBuilder builder )
    {
        // Add autogenerated features
        if ( Objects.isNull( builder.featureService() ) )
        {
            LOGGER.debug( "Interpolating geospatial features from the raw declaration." );

            boolean hasBaseline = DeclarationFactory.hasBaseline( builder );

            // Apply to feature context
            if ( Objects.nonNull( builder.features() ) )
            {
                Set<GeometryTuple> geometries = builder.features()
                                                       .geometries();

                geometries = DeclarationFactory.getAutogeneratedGeometries( geometries, hasBaseline );
                Features adjustedFeatures = new Features( geometries );
                builder.features( adjustedFeatures );
            }

            // Apply to feature group context
            if ( Objects.nonNull( builder.featureGroups() ) )
            {
                Set<GeometryGroup> geoGroups = builder.featureGroups()
                                                      .geometryGroups();

                // Preserve insertion order
                Set<GeometryGroup> adjustedGeoGroups = new LinkedHashSet<>();

                for ( GeometryGroup nextGroup : geoGroups )
                {
                    List<GeometryTuple> nextTuples = nextGroup.getGeometryTuplesList();
                    Set<GeometryTuple> adjustedTuples =
                            DeclarationFactory.getAutogeneratedGeometries( nextTuples, hasBaseline );

                    GeometryGroup nextAdjustedGroup =
                            nextGroup.toBuilder()
                                     .clearGeometryTuples()
                                     .addAllGeometryTuples( adjustedTuples )
                                     .build();
                    adjustedGeoGroups.add( nextAdjustedGroup );
                }
                FeatureGroups adjustedFeatureGroups = new FeatureGroups( adjustedGeoGroups );
                builder.featureGroups( adjustedFeatureGroups );
            }
        }
    }

    /**
     * Fills any geometries that are partially declared, providing there is no feature service declaration, which
     * serves the same purpose.
     * @param geometries the geometries to fill
     * @param hasBaseline whether a baseline has been declared
     * @return the geometries with any partially declared geometries filled, where applicable
     */

    private static Set<GeometryTuple> getAutogeneratedGeometries( Collection<GeometryTuple> geometries,
                                                                  boolean hasBaseline )
    {
        // Preserve insertion order
        Set<GeometryTuple> adjustedGeometries = new LinkedHashSet<>();
        for ( GeometryTuple next : geometries )
        {
            GeometryTuple.Builder adjusted = next.toBuilder();
            if ( !next.hasRight() )
            {
                adjusted.setRight( next.getLeft() );
            }
            if ( !next.hasBaseline() && hasBaseline )
            {
                adjusted.setBaseline( next.getLeft() );
            }
            adjustedGeometries.add( adjusted.build() );
        }

        return Collections.unmodifiableSet( adjustedGeometries );
    }

    /**
     * Adds autogenerated metrics to the declaration. Does not add any metrics from the
     * {@link MetricConstants.SampleDataGroup#SINGLE_VALUED_TIME_SERIES} group because, while strictly valid for all
     * single-valued time-series, they are niche metrics that have a high computational burden and should be explicitly
     * added by a user.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateMetrics( EvaluationDeclarationBuilder builder )
    {
        DataType rightType = builder.right().type();

        // No metrics defined and the type is known, so interpolate all valid metrics
        if ( builder.metrics().isEmpty() && Objects.nonNull( rightType ) )
        {
            LOGGER.debug( "Interpolating metrics from the raw declaration." );

            Set<MetricConstants> metrics = new LinkedHashSet<>();

            // Ensemble forecast time-series
            if ( rightType == DataType.ENSEMBLE_FORECASTS )
            {
                Set<MetricConstants> ensemble = MetricConstants.SampleDataGroup.ENSEMBLE.getMetrics();
                Set<MetricConstants> singleValued = MetricConstants.SampleDataGroup.SINGLE_VALUED.getMetrics();
                metrics.addAll( singleValued );
                metrics.addAll( ensemble );

                // Probability or value thresholds? Then add discrete probability metrics and dichotomous metrics
                if ( !builder.probabilityThresholds().isEmpty() || !builder.valueThresholds().isEmpty() )
                {
                    Set<MetricConstants> discreteProbability =
                            MetricConstants.SampleDataGroup.DISCRETE_PROBABILITY.getMetrics();
                    Set<MetricConstants> dichotomous = MetricConstants.SampleDataGroup.DICHOTOMOUS.getMetrics();
                    metrics.addAll( discreteProbability );
                    metrics.addAll( dichotomous );
                }
            }
            // Single-valued time-series
            else
            {
                Set<MetricConstants> singleValued = MetricConstants.SampleDataGroup.SINGLE_VALUED.getMetrics();
                metrics.addAll( singleValued );

                // Probability or value thresholds? Then add dichotomous metrics
                if ( !builder.probabilityThresholds().isEmpty() || !builder.valueThresholds().isEmpty() )
                {
                    Set<MetricConstants> dichotomous = MetricConstants.SampleDataGroup.DICHOTOMOUS.getMetrics();
                    metrics.addAll( dichotomous );
                }
            }

            // Remove any metrics that are incompatible with other declaration
            DeclarationFactory.removeIncompatibleMetrics( builder, metrics );

            // Wrap the metrics and set them
            Set<Metric> wrapped =
                    metrics.stream()
                           .map( next -> new Metric( next, null ) )
                           .collect( Collectors.toUnmodifiableSet() );
            builder.metrics( wrapped );
        }
    }

    /**
     * Removes any metrics from the mutable set of auto-generated metrics that are inconsistent with other declaration.
     * @param builder the builder
     * @param metrics the metrics
     */
    private static void removeIncompatibleMetrics( EvaluationDeclarationBuilder builder, Set<MetricConstants> metrics )
    {
        // Remove any skill metrics that require an explicit baseline
        if ( Objects.isNull( builder.baseline() ) )
        {
            metrics.remove( MetricConstants.CONTINUOUS_RANKED_PROBABILITY_SKILL_SCORE );
        }
    }

    /**
     * Adds the decimal format string to each numeric format type.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateDecimalFormatforNumericFormats( EvaluationDeclarationBuilder builder )
    {
        // Something to adjust?
        if ( Objects.isNull( builder.formats() ) )
        {
            LOGGER.debug( "No numerical formats were discovered and, therefore, adjusted for decimal format." );
            return;
        }

        FormatsBuilder formatsBuilder = FormatsBuilder.builder( builder.formats() );

        if ( Objects.nonNull( formatsBuilder.csv2Format() ) )
        {
            Outputs.Csv2Format.Builder csv2Builder = formatsBuilder.csv2Format()
                                                                   .toBuilder();
            Outputs.NumericFormat.Builder numericBuilder = csv2Builder.getOptionsBuilder();
            numericBuilder.setDecimalFormat( builder.decimalFormat()
                                                    .toPattern() );
            csv2Builder.setOptions( numericBuilder );
            formatsBuilder.csv2Format( csv2Builder.build() );
        }

        if ( Objects.nonNull( formatsBuilder.csvFormat() ) )
        {
            Outputs.CsvFormat.Builder csvBuilder = formatsBuilder.csvFormat()
                                                                 .toBuilder();
            Outputs.NumericFormat.Builder numericBuilder = csvBuilder.getOptionsBuilder();
            numericBuilder.setDecimalFormat( builder.decimalFormat()
                                                    .toPattern() );
            csvBuilder.setOptions( numericBuilder );
            formatsBuilder.csvFormat( csvBuilder.build() );
        }

        if ( Objects.nonNull( formatsBuilder.pairsFormat() ) )
        {
            Outputs.PairFormat.Builder pairsBuilder = formatsBuilder.pairsFormat()
                                                                    .toBuilder();
            Outputs.NumericFormat.Builder numericBuilder = pairsBuilder.getOptionsBuilder();
            numericBuilder.setDecimalFormat( builder.decimalFormat()
                                                    .toPattern() );
            pairsBuilder.setOptions( numericBuilder );
            formatsBuilder.pairsFormat( pairsBuilder.build() );
        }

        // Set the new format info
        builder.formats( formatsBuilder.build() );
    }

    /**
     * Adds the metrics for which graphics are not required to each geaphics format.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateMetricsToOmitFromGraphicsFormats( EvaluationDeclarationBuilder builder )
    {
        // Something to adjust?
        if ( Objects.isNull( builder.formats() ) )
        {
            LOGGER.debug( "No graphical formats were adjusted for metrics to ignore because no graphics formats were "
                          + "declared." );
            return;
        }

        FormatsBuilder formatsBuilder = FormatsBuilder.builder( builder.formats() );

        if ( Objects.nonNull( formatsBuilder.pngFormat() ) )
        {
            List<MetricName> pngAvoid = builder.metrics()
                                               .stream()
                                               .filter( next -> Objects.nonNull( next.parameters() )
                                                                && !next.parameters().png() )
                                               .map( Metric::name )
                                               .map( next -> MetricName.valueOf( next.name() ) )
                                               .toList();

            LOGGER.debug( "Discovered these metrics to avoid, which will be registered with all graphics formats: {}.",
                          pngAvoid );
            Outputs.PngFormat.Builder pngBuilder = formatsBuilder.pngFormat()
                                                                 .toBuilder();
            Outputs.GraphicFormat.Builder graphicBuilder = pngBuilder.getOptionsBuilder();
            graphicBuilder.clearIgnore()
                          .addAllIgnore( pngAvoid );
            pngBuilder.setOptions( graphicBuilder );
            formatsBuilder.pngFormat( pngBuilder.build() );
        }

        if ( Objects.nonNull( formatsBuilder.svgFormat() ) )
        {
            List<MetricName> svgAvoid = builder.metrics()
                                               .stream()
                                               .filter( next -> Objects.nonNull( next.parameters() )
                                                                && !next.parameters().png() )
                                               .map( Metric::name )
                                               .map( next -> MetricName.valueOf( next.name() ) )
                                               .toList();
            Outputs.SvgFormat.Builder svgBuilder = formatsBuilder.svgFormat()
                                                                 .toBuilder();
            Outputs.GraphicFormat.Builder graphicBuilder = svgBuilder.getOptionsBuilder();
            graphicBuilder.clearIgnore()
                          .addAllIgnore( svgAvoid );
            svgBuilder.setOptions( graphicBuilder );
            formatsBuilder.svgFormat( svgBuilder.build() );
        }

        // Set the new format info
        builder.formats( formatsBuilder.build() );
    }

    /**
     * Adds the metrics for which graphics are not required to each geaphics format.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateGraphicsFormatsFromMetricParameters( EvaluationDeclarationBuilder builder )
    {
        FormatsBuilder formatsBuilder = null;

        if ( Objects.isNull( builder.formats() ) )
        {
            formatsBuilder = FormatsBuilder.builder();
        }
        else
        {
            formatsBuilder = FormatsBuilder.builder( builder.formats() );
        }

        // PNG format required but not declared?
        if ( Objects.isNull( formatsBuilder.pngFormat() ) )
        {
            boolean png = builder.metrics()
                                 .stream()
                                 .anyMatch( next -> Objects.nonNull( next.parameters() ) && next.parameters()
                                                                                                .png() );
            if ( png )
            {
                LOGGER.debug( "Discovered metrics that require PNG graphics, but the PNG format was not declared in "
                              + "the list of 'output_formats'. Adding the PNG format." );
                formatsBuilder.pngFormat( Formats.PNG_FORMAT );
            }
        }

        // SVG format required but not declared?
        if ( Objects.isNull( formatsBuilder.svgFormat() ) )
        {
            boolean svg = builder.metrics()
                                 .stream()
                                 .anyMatch( next -> Objects.nonNull( next.parameters() ) && next.parameters()
                                                                                                .svg() );
            if ( svg )
            {
                LOGGER.debug( "Discovered metrics that require SVG graphics, but the SVG format was not declared in "
                              + "the list of 'output_formats'. Adding the SVG format." );
                formatsBuilder.svgFormat( Formats.SVG_FORMAT );
            }
        }

        // Set the new format info
        builder.formats( formatsBuilder.build() );
    }

    /**
     * Copies the evaluation units to the value threshold units when they are not declared explicitly.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateMeasurementUnitForValueThresholds( EvaluationDeclarationBuilder builder )
    {
        if ( Objects.nonNull( builder.unit() ) )
        {
            LOGGER.debug( "Interpolating measurement units for value thresholds." );

            String unit = builder.unit();

            // Value thresholds
            Set<Threshold> valueThresholds = builder.valueThresholds();
            valueThresholds = DeclarationFactory.addUnitToValueThresholds( valueThresholds, unit );
            builder.valueThresholds( valueThresholds );

            // Threshold sets
            Set<Threshold> thresholdSets = builder.thresholdSets();
            thresholdSets = DeclarationFactory.addUnitToValueThresholds( thresholdSets, unit );
            builder.thresholdSets( thresholdSets );

            // Individual metrics
            Set<Metric> metrics = builder.metrics();
            Set<Metric> adjustedMetrics = new LinkedHashSet<>( metrics.size() );
            for ( Metric next : metrics )
            {
                // Adjust?
                if ( Objects.nonNull( next.parameters() ) && !next.parameters()
                                                                  .valueThresholds()
                                                                  .isEmpty() )
                {
                    Set<Threshold> adjusted = next.parameters()
                                                  .valueThresholds();
                    adjusted = DeclarationFactory.addUnitToValueThresholds( adjusted, unit );
                    MetricParameters adjustedParameters = MetricParametersBuilder.builder( next.parameters() )
                                                                                 .valueThresholds( adjusted )
                                                                                 .build();
                    Metric adjustedMetric = MetricBuilder.builder( next )
                                                         .parameters( adjustedParameters )
                                                         .build();
                    adjustedMetrics.add( adjustedMetric );
                }
                // Retain the existing metric parameters
                else
                {
                    adjustedMetrics.add( next );
                }
            }

            builder.metrics( adjustedMetrics );
        }
    }

    /**
     * Interpolates the metric-specific thresholds for metrics without declared thresholds using the thresholds
     * declared at a higher level.
     *
     * @param builder the builder to mutate
     */
    private static void interpolateThresholdsForIndividualMetrics( EvaluationDeclarationBuilder builder )
    {
        LOGGER.debug( "Interpolating thresholds for metrics." );

        // Assemble the thresholds for each type
        Set<Threshold> allThresholds = new LinkedHashSet<>();
        allThresholds.addAll( builder.probabilityThresholds() );
        allThresholds.addAll( builder.valueThresholds() );
        allThresholds.addAll( builder.classifierThresholds() );
        allThresholds.addAll( builder.thresholdSets() );

        // Group by type
        Map<wres.config.yaml.components.ThresholdType, Set<Threshold>> thresholdsByType
                = DeclarationFactory.groupThresholdsByType( allThresholds );
        LOGGER.debug( "When interpolating thresholds for metrics, discovered the following thresholds to use: {}.",
                      thresholdsByType );

        DeclarationFactory.addThresholdsToMetrics( thresholdsByType, builder );
    }

    /**
     * Groups the thresholds by threshold type.
     * @param thresholds the thresholds
     * @return the thresholds grouped by type
     */

    private static Map<wres.config.yaml.components.ThresholdType, Set<Threshold>> groupThresholdsByType( Set<Threshold> thresholds )
    {
        return thresholds.stream()
                         .collect( Collectors.groupingBy( Threshold::type,
                                                          Collectors.mapping( Function.identity(),
                                                                              Collectors.toCollection(
                                                                                      LinkedHashSet::new ) ) ) );
    }

    /**
     * Adds a default output format of CSV2 where none exists.
     *
     * @param builder the builder to mutate
     */
    private static void interpolateOutputFormats( EvaluationDeclarationBuilder builder )
    {
        if ( Objects.isNull( builder.formats() ) || builder.formats().equals( FormatsBuilder.builder()
                                                                                            .build() ) )
        {
            LOGGER.debug( "Adding a default output format of CSV2 because no output formats were declared." );
            FormatsBuilder formatsBuilder = FormatsBuilder.builder();
            formatsBuilder.csv2Format( Formats.CSV2_FORMAT );
            builder.formats( formatsBuilder.build() );
        }
    }

    /**
     * Associates the specified thresholds with the appropriate metrics and adds an "all data" threshold to each
     * continuous metric.
     *
     * @param thresholdsByType the mapped thresholds
     * @param builder the builder to mutate
     */

    private static void addThresholdsToMetrics( Map<wres.config.yaml.components.ThresholdType, Set<Threshold>> thresholdsByType,
                                                EvaluationDeclarationBuilder builder )
    {
        Set<Metric> metrics = builder.metrics();
        Set<Metric> adjustedMetrics = new LinkedHashSet<>( metrics.size() );

        LOGGER.debug( "Discovered these metrics whose thresholds will be adjusted: {}.", metrics );

        // Adjust the metrics
        for ( Metric next : metrics )
        {
            Metric adjustedMetric = DeclarationFactory.addThresholdsToMetric( thresholdsByType, next );
            adjustedMetrics.add( adjustedMetric );
        }

        builder.metrics( adjustedMetrics );
    }

    /**
     * Associates the specified thresholds with the specified metric and adds an "all data" threshold as needed.
     *
     * @param thresholdsByType the mapped thresholds
     * @return the adjusted metric
     */

    private static Metric addThresholdsToMetric( Map<wres.config.yaml.components.ThresholdType, Set<Threshold>> thresholdsByType,
                                                 Metric metric )
    {
        MetricConstants name = metric.name();

        LOGGER.debug( "Adjusting metric {} to include thresholds, as needed.", name );

        MetricParameters nextParameters = metric.parameters();
        MetricParametersBuilder parametersBuilder = MetricParametersBuilder.builder();

        // Add existing parameters where available
        if ( Objects.nonNull( nextParameters ) )
        {
            parametersBuilder = MetricParametersBuilder.builder( nextParameters );
        }

        // Value thresholds
        if ( Objects.isNull( parametersBuilder.valueThresholds() ) || parametersBuilder.valueThresholds()
                                                                                       .isEmpty() )
        {
            Set<Threshold> valueThresholds =
                    thresholdsByType.get( wres.config.yaml.components.ThresholdType.VALUE );
            parametersBuilder.valueThresholds( valueThresholds );
        }

        // Add "all data" thresholds?
        if ( name.isContinuous() )
        {
            Set<Threshold> valueThresholds = new LinkedHashSet<>();
            if ( Objects.nonNull( parametersBuilder.valueThresholds() ) )
            {
                valueThresholds.addAll( parametersBuilder.valueThresholds() );
            }
            valueThresholds.add( ALL_DATA_THRESHOLD );
            parametersBuilder.valueThresholds( valueThresholds );
        }

        // Probability thresholds
        if ( Objects.isNull( parametersBuilder.probabilityThresholds() ) || parametersBuilder.probabilityThresholds()
                                                                                             .isEmpty() )
        {
            Set<Threshold> probThresholds =
                    thresholdsByType.get( wres.config.yaml.components.ThresholdType.PROBABILITY );
            parametersBuilder.probabilityThresholds( probThresholds );
        }

        // Classifier thresholds, which only apply to categorical measures
        if ( ( Objects.isNull( parametersBuilder.classifierThresholds() )
               || parametersBuilder.classifierThresholds()
                                   .isEmpty() )
             && ( name.isInGroup( MetricConstants.SampleDataGroup.DICHOTOMOUS )
                  || name.isInGroup( MetricConstants.SampleDataGroup.MULTICATEGORY ) ) )
        {
            Set<Threshold> classThresholds =
                    thresholdsByType.get( wres.config.yaml.components.ThresholdType.PROBABILITY_CLASSIFIER );
            parametersBuilder.classifierThresholds( classThresholds );
        }

        Metric adjustedMetric = metric;

        // Create the adjusted metric
        MetricParameters newParameters = parametersBuilder.build();

        // Something changed, which resulted in non-default parameters?
        if ( !newParameters.equals( nextParameters ) &&
             !newParameters.equals( MetricParametersBuilder.builder().build() ) )
        {
            adjustedMetric = MetricBuilder.builder( metric )
                                          .parameters( newParameters )
                                          .build();

            LOGGER.debug( "Adjusted a metric to include thresholds. The original metric was: {}. The adjusted metric "
                          + "is {}.", metric, adjustedMetric );
        }

        return adjustedMetric;
    }

    /**
     * Adds the unit to each value threshold in the set that does not have the unit defined.
     *
     * @param thresholds the thresholds
     * @return the adjusted thresholds
     */
    private static Set<Threshold> addUnitToValueThresholds( Set<Threshold> thresholds,
                                                            String unit )
    {
        Set<Threshold> adjustedThresholds = new LinkedHashSet<>( thresholds.size() );
        for ( Threshold next : thresholds )
        {
            // Value threshold without a unit string
            if ( next.type() == wres.config.yaml.components.ThresholdType.VALUE
                 && next.threshold()
                        .getThresholdValueUnits()
                        .isBlank() )
            {
                wres.statistics.generated.Threshold adjusted = next.threshold()
                                                                   .toBuilder()
                                                                   .setThresholdValueUnits( unit )
                                                                   .build();
                Threshold threshold = ThresholdBuilder.builder( next )
                                                      .threshold( adjusted )
                                                      .build();
                adjustedThresholds.add( threshold );
                LOGGER.debug( "Adjusted a value threshold to use the evaluation unit of {} because the threshold unit "
                              + "was not declared explicitly. The adjusted threshold is: {}", unit, threshold );
            }
            else
            {
                adjustedThresholds.add( next );
            }
        }

        return Collections.unmodifiableSet( adjustedThresholds );
    }

    /**
     * Resolves the type of time-series data to evaluate when required.
     *
     * @param builder the declaration builder to adjust
     */
    private static void interpolateDataTypes( EvaluationDeclarationBuilder builder )
    {
        // Resolve the left or observed data type, if required
        DeclarationFactory.resolveObservedDataTypeIfRequired( builder );

        // Resolve the predicted data type, if required
        DeclarationFactory.resolvePredictedDataTypeIfRequired( builder );

        // Baseline data type has the same as the predicted data type, by default
        DeclarationFactory.setBaselineDataTypeFromPredictedDataTypeIfRequired( builder );
    }

    /**
     * Resolves the observed data type.
     * @param builder the builder
     */
    private static void resolveObservedDataTypeIfRequired( EvaluationDeclarationBuilder builder )
    {
        Dataset observed = builder.left();

        // Resolve the left or observed data type
        if ( Objects.isNull( observed.type() ) )
        {
            String defaultMessage = "While reading the project declaration, discovered that the observed dataset had "
                                    + "no declared data 'type'. {} If this is incorrect, please declare the 'type' "
                                    + "explicitly.";

            // Analysis durations present? If so, assume analyses
            if ( DeclarationFactory.hasAnalysisDurations( builder ) )
            {
                Dataset newLeft = DatasetBuilder.builder( observed )
                                                .type( DataType.ANALYSES )
                                                .build();
                builder.left( newLeft );

                // Log the reason
                LOGGER.warn( defaultMessage,
                             "Assuming that the 'type' is 'analyses' because analysis durations "
                             + "were discovered and analyses are typically used to verify other " + "datasets." );
            }
            else
            {
                Dataset newLeft = DatasetBuilder.builder( observed )
                                                .type( DataType.OBSERVATIONS )
                                                .build();
                builder.left( newLeft );

                // Log the reason
                LOGGER.warn( defaultMessage, "Assuming that the 'type' is 'observations'." );
            }
        }
    }

    /**
     * Resolves the predicted data type.
     * @param builder the builder
     */
    private static void resolvePredictedDataTypeIfRequired( EvaluationDeclarationBuilder builder )
    {
        Dataset predicted = builder.right();

        // Resolve the right or predicted data type
        if ( Objects.isNull( predicted.type() ) )
        {
            String defaultMessage = "While reading the project declaration, discovered that the predicted dataset had "
                                    + "no declared data 'type'. {} If this is incorrect, please declare the 'type' "
                                    + "explicitly.";

            String reasonMessage;

            DataType dataType;

            // Discover hints from the declaration
            Set<String> ensembleDeclaration = DeclarationFactory.getEnsembleDeclaration( builder );
            Set<String> forecastDeclaration = DeclarationFactory.getForecastDeclaration( builder );

            // Ensemble declaration?
            if ( !ensembleDeclaration.isEmpty() )
            {
                reasonMessage = "Setting the 'type' to 'ensemble forecasts' because the following ensemble "
                                + "declaration was discovered: " + ensembleDeclaration + ".";
                dataType = DataType.ENSEMBLE_FORECASTS;
            }
            // Forecast declaration?
            else if ( !forecastDeclaration.isEmpty() )
            {
                reasonMessage = "Setting the 'type' to 'single valued forecasts' because the following forecast "
                                + "declaration was discovered and no ensemble declaration was discovered to suggest "
                                + "that the forecasts are 'ensemble forecasts': " + forecastDeclaration + ".";
                dataType = DataType.SINGLE_VALUED_FORECASTS;
            }
            // Source declaration that is a multi-type service? If so, cannot infer the type
            else if ( predicted.sources()
                               .stream()
                               .anyMatch( next -> Objects.nonNull( next.sourceInterface() )
                                                  && next.sourceInterface().getDataTypes().size() > 1 ) )
            {
                reasonMessage = "Could not infer the predicted data type because sources were declared with interfaces "
                                + "that support multiple data types.";
                dataType = null;
            }
            else
            {
                reasonMessage = "Setting the 'type' to 'simulations' because no declaration was discovered to "
                                + "suggest that any dataset contains 'single valued forecasts' or 'ensemble "
                                + "forecast'.";

                dataType = DataType.SIMULATIONS;
            }

            // Set the type
            Dataset newPredicted = DatasetBuilder.builder( predicted ).type( dataType ).build();
            builder.right( newPredicted );

            // Log the reason
            LOGGER.warn( defaultMessage, reasonMessage );
        }
    }

    /**
     * @param builder the builder
     * @param groupType the group type
     * @return whether there are any metrics with the designated type
     */
    private static Set<String> getMetricType( EvaluationDeclarationBuilder builder,
                                              MetricConstants.SampleDataGroup groupType )
    {
        return builder.metrics()
                      .stream()
                      .filter( next -> next.name().isInGroup( groupType ) )
                      .map( next -> next.name().toString() )
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
    private static boolean hasBaseline( EvaluationDeclarationBuilder builder )
    {
        return Objects.nonNull( builder.baseline() );
    }

    /**
     * Sets the baseline data type to match the data type of the predicted dataset.
     * @param builder the builder
     */
    private static void setBaselineDataTypeFromPredictedDataTypeIfRequired( EvaluationDeclarationBuilder builder )
    {
        if ( DeclarationFactory.hasBaseline( builder ) )
        {
            Dataset predicted = builder.right();
            BaselineDataset baseline = builder.baseline();
            Dataset baselineDataset = baseline.dataset();

            // Set the baseline data type, if required
            if ( Objects.isNull( baselineDataset.type() ) )
            {
                Dataset newBaselineDataset = DatasetBuilder.builder( baselineDataset )
                                                           .type( predicted.type() )
                                                           .build();
                BaselineDataset newBaseline =
                        BaselineDatasetBuilder.builder( baseline )
                                              .dataset( newBaselineDataset )
                                              .build();
                builder.baseline( newBaseline );

                LOGGER.warn( "While reading the project declaration, discovered that the baseline dataset had no "
                             + "declared data 'type'. Assuming that the 'type' is '{}' to match the type of the "
                             + "predicted dataset. If this is incorrect, please declare the 'type' explicitly.",
                             newBaselineDataset.type() );
            }
        }
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
                 && "persistence" .equals( baselineConfig.getTransformation()
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
        Set<GeometryTuple> geometries = DeclarationFactory.migrateFeatures( features );
        Features wrappedFeatures = new Features( geometries );
        builder.features( wrappedFeatures );
    }

    /**
     * Migrates the feature groups from the old declaration to the new declaration builder.
     * @param featureGroups the feature groups to migrate
     * @param builder the new declaration builder
     */

    private static void migrateFeatureGroups( List<FeaturePool> featureGroups, EvaluationDeclarationBuilder builder )
    {
        Set<GeometryGroup> geometryGroups =
                featureGroups.stream()
                             .map( DeclarationFactory::migrateFeatureGroup )
                             .collect( Collectors.toSet() );
        FeatureGroups wrappedGroups = new FeatureGroups( geometryGroups );
        builder.featureGroups( wrappedGroups );
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
            String wkt = DeclarationFactory.migrateSpatialMask( mask );
            SpatialMask spatialMask = SpatialMaskBuilder.builder()
                                                        .wkt( wkt )
                                                        .build();
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
                DeclarationFactory.migrateThresholds( globalThresholds, builder );
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
        FormatsBuilder formatsBuilder = FormatsBuilder.builder();
        if ( Objects.nonNull( builder.formats() ) )
        {
            formatsBuilder = FormatsBuilder.builder( builder.formats() );
        }

        switch ( output.getType() )
        {
            case CSV, NUMERIC -> formatsBuilder.csvFormat( Formats.CSV_FORMAT );
            case CSV2 -> formatsBuilder.csv2Format( Formats.CSV2_FORMAT );
            case PNG, GRAPHIC -> DeclarationFactory.migratePngFormat( output, formatsBuilder, builder );
            case SVG -> DeclarationFactory.migrateSvgFormat( output, formatsBuilder, builder );
            case NETCDF -> formatsBuilder.netcdfFormat( Formats.NETCDF_FORMAT );
            case NETCDF_2 -> formatsBuilder.netcdf2Format( Formats.NETCDF2_FORMAT );
            case PAIRS -> formatsBuilder.pairsFormat( Formats.PAIR_FORMAT );
            case PROTOBUF -> formatsBuilder.protobufFormat( Formats.PROTOBUF_FORMAT );
        }

        // Migrate the formats
        builder.formats( formatsBuilder.build() );
    }

    /**
     * Migrates a PNG output format.
     * @param output the output
     * @param builder the builder to mutate
     * @param evaluationBuilder the evaluation builder whose metric parameters may need to be updated
     */

    private static void migratePngFormat( DestinationConfig output,
                                          FormatsBuilder builder,
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
        builder.pngFormat( pngBuilder.build() );
    }

    /**
     * Migrates a SVG output format.
     * @param output the output
     * @param builder the builder to mutate
     * @param evaluationBuilder the evaluation builder whose metric parameters may need to be updated
     */

    private static void migrateSvgFormat( DestinationConfig output,
                                          FormatsBuilder builder,
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
        builder.svgFormat( svgBuilder.build() );
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
                MetricParametersBuilder parBuilder = MetricParametersBuilder.builder( metric.parameters() );

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

        List<Source> sources = DeclarationFactory.migrateSources( dataSource.getSource(),
                                                                  dataSource.getExistingTimeScale(),
                                                                  dataSource.getUrlParameter() );
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
                             .build();
    }

    /**
     * Migrates a collection of {@link DataSourceConfig.Source} to a collection of {@link Source}.
     *
     * @param sources the data sources
     * @param timeScale the timescale
     * @param parameters the optional URL parameters
     * @return the migrated sources
     */

    private static List<Source> migrateSources( List<DataSourceConfig.Source> sources,
                                                TimeScaleConfig timeScale,
                                                List<UrlParameter> parameters )
    {
        return sources.stream().map( next -> DeclarationFactory.migrateSource( next, timeScale, parameters ) ).toList();
    }

    /**
     * Migrates a {@link DataSourceConfig.Source} to a {@link Source}.
     *
     * @param source the data sources
     * @param timeScale the timescale
     * @param parameters the optional URL parameters
     * @return the migrated source
     */

    private static Source migrateSource( DataSourceConfig.Source source,
                                         TimeScaleConfig timeScale,
                                         List<UrlParameter> parameters )
    {
        SourceBuilder builder = SourceBuilder.builder().uri( source.getValue() ).pattern( source.getPattern() );

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

        if ( Objects.nonNull( source.getZoneOffset() ) )
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
        GeometryTuple.Builder builder = GeometryTuple.newBuilder();
        Geometry left = Geometry.newBuilder()
                                .setName( feature.getLeft() )
                                .build();
        Geometry right = Geometry.newBuilder()
                                 .setName( feature.getRight() )
                                 .build();

        String baselineName = feature.getBaseline();
        if ( Objects.nonNull( baselineName ) )
        {
            Geometry baseline = Geometry.newBuilder()
                                        .setName( baselineName )
                                        .build();
            builder.setBaseline( baseline );
        }

        return builder.setLeft( left ).setRight( right ).build();
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
     * @return the WKT string
     */

    private static String migrateSpatialMask( List<UnnamedFeature> features )
    {
        org.locationtech.jts.geom.Geometry unionGeometry = null;

        if ( features.isEmpty() )
        {
            LOGGER.debug( "No spatial mask features to migrate." );
            return null;
        }

        if ( features.size() > 1 )
        {
            for ( UnnamedFeature next : features )
            {
                org.locationtech.jts.geom.Geometry nextGeometry = DeclarationFactory.migrateGeometry( next );
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
            unionGeometry = DeclarationFactory.migrateGeometry( features.get( 0 ) );
        }

        String wkt = null;
        if ( Objects.nonNull( unionGeometry ) )
        {
            WKTWriter writer = new WKTWriter();
            wkt = writer.write( unionGeometry );
        }

        return wkt;
    }

    /**
     * Migrates a {@link UnnamedFeature} to a {@link org.locationtech.jts.geom.Geometry}.
     * @param feature the feature
     * @return the migrated geometry
     */

    private static org.locationtech.jts.geom.Geometry migrateGeometry( UnnamedFeature feature )
    {
        org.locationtech.jts.geom.Geometry geometry = null;

        if ( Objects.nonNull( feature.getCircle() ) )
        {
            GeometricShapeFactory shapeMaker = new GeometricShapeFactory();
            Circle circle = feature.getCircle();
            shapeMaker.setWidth( circle.getDiameter() );
            shapeMaker.setBase( new CoordinateXY( circle.getLongitude(), circle.getLatitude() ) );
            geometry = shapeMaker.createCircle();
        }
        else if ( Objects.nonNull( feature.getPolygon() ) && feature.getPolygon()
                                                                    .getPoint()
                                                                    .size() > 1 )
        {
            Polygon polygon = feature.getPolygon();
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

        return geometry;
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
        return new TimeInterval( Instant.parse( dateCondition.getEarliest() ),
                                 Instant.parse( dateCondition.getLatest() ) );
    }

    /**
     * Migrates a {@link TimePools} to a {@link PoolingWindowConfig}.
     * @param poolingWindow the pooling window
     * @return the time pool
     */
    private static TimePools migrateTimePools( PoolingWindowConfig poolingWindow )
    {
        ChronoUnit unit = ChronoUnit.valueOf( poolingWindow.getUnit().name() );
        return new TimePools( poolingWindow.getPeriod(), poolingWindow.getFrequency(), unit );
    }

    /**
     * Migrates the thresholds to the declaration builder.
     * @param thresholds the thresholds to migrate
     * @param builder the declaration builder
     */

    private static void migrateThresholds( Map<wres.config.generated.ThresholdType, ThresholdsConfig> thresholds,
                                           EvaluationDeclarationBuilder builder )
    {
        // Probability thresholds
        if ( thresholds.containsKey( wres.config.generated.ThresholdType.PROBABILITY ) )
        {
            ThresholdsConfig probabilityThresholdsConfig =
                    thresholds.get( wres.config.generated.ThresholdType.PROBABILITY );
            Set<Threshold> probabilityThresholds = DeclarationFactory.migrateThresholds( probabilityThresholdsConfig );
            builder.probabilityThresholds( probabilityThresholds );
            LOGGER.debug( "Migrated these probability thresholds for all metrics: {}.", probabilityThresholds );
        }

        // Value thresholds
        if ( thresholds.containsKey( wres.config.generated.ThresholdType.VALUE ) )
        {
            ThresholdsConfig valueThresholdsConfig =
                    thresholds.get( wres.config.generated.ThresholdType.VALUE );
            Set<Threshold> valueThresholds = DeclarationFactory.migrateThresholds( valueThresholdsConfig );
            builder.valueThresholds( valueThresholds );
            LOGGER.debug( "Migrated these value thresholds for all metrics: {}.", valueThresholds );
        }

        // Probability classifier thresholds
        if ( thresholds.containsKey( wres.config.generated.ThresholdType.PROBABILITY_CLASSIFIER ) )
        {
            ThresholdsConfig classifierThresholdsConfig =
                    thresholds.get( wres.config.generated.ThresholdType.PROBABILITY_CLASSIFIER );
            Set<Threshold> classifierThresholds = DeclarationFactory.migrateThresholds( classifierThresholdsConfig );
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
                // Acquire the parameters
                MetricParameters parameters =
                        DeclarationFactory.migrateMetricParameters( next, projectConfig, addThresholdsPerMetric );

                Set<Metric> innerMetrics = metricNames.stream()
                                                      .map( nextName -> new Metric( nextName, parameters ) )
                                                      .collect( Collectors.toSet() );
                builder.metrics( innerMetrics );
            }
            else
            {
                LOGGER.debug( "The following metrics declaration had no explicit metrics to migrate: {}", next );
            }
        }
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
     * @param projectConfig the overall declaration used as context when migrating metrics
     * @param addThresholdsPerMetric whether the thresholds declared in each metric group should be added to each metric
     */
    private static MetricParameters migrateMetricParameters( MetricsConfig metric,
                                                             ProjectConfig projectConfig,
                                                             boolean addThresholdsPerMetric )
    {
        Set<MetricConstants> metricNames = MetricConstantsFactory.getMetricsFromConfig( metric, projectConfig );
        MetricParametersBuilder parametersBuilder = MetricParametersBuilder.builder();

        // Set the thresholds, if needed
        if ( addThresholdsPerMetric && Objects.nonNull( metric.getThresholds() ) )
        {
            List<ThresholdsConfig> thresholds = metric.getThresholds();
            Set<Threshold> migratedThresholds = thresholds.stream()
                                                          .map( DeclarationFactory::migrateThresholds )
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

        // Time-series summary statistics?
        Set<MetricConstants> timeSeriesSummaryStats
                = metricNames.stream()
                             .filter( nextName -> nextName.isInGroup( MetricConstants.StatisticType.DURATION_SCORE ) )
                             .collect( Collectors.toSet() );
        if ( !timeSeriesSummaryStats.isEmpty() )
        {
            Set<DurationScoreMetric.DurationScoreMetricComponent.ComponentName> componentNameSet
                    = timeSeriesSummaryStats.stream()
                                            .map( MetricConstants::name )
                                            .map( DurationScoreMetric.DurationScoreMetricComponent.ComponentName::valueOf )
                                            .collect( Collectors.toSet() );

            parametersBuilder.summaryStatistics( componentNameSet );
        }

        return parametersBuilder.build();
    }

    /**
     * Migrates old-style declaration of thresholds to canonical thresholds.
     * @param thresholds the thresholds to migrate
     * @return the migrated thresholds
     */

    private static Set<Threshold> migrateThresholds( ThresholdsConfig thresholds )
    {
        // Either an internal list of thresholds in CSV format or an external point, such as a file or service call,
        // which are not migrated here
        Object internalOrExternal = thresholds.getCommaSeparatedValuesOrSource();

        Set<Threshold> migrated = new LinkedHashSet<>();

        // Concrete thresholds?
        if ( internalOrExternal instanceof String )
        {
            // Default threshold

            wres.statistics.generated.Threshold.Builder builder = DEFAULT_CANONICAL_THRESHOLD.toBuilder();

            // Need to map enums
            wres.config.generated.ThresholdType type = DeclarationFactory.getThresholdType( thresholds );
            ThresholdType newType = ThresholdType.valueOf( type.name() );

            if ( Objects.nonNull( thresholds.getOperator() ) )
            {
                wres.config.generated.ThresholdOperator operator = thresholds.getOperator();
                wres.statistics.generated.Threshold.ThresholdOperator canonicalOperator =
                        ProjectConfigs.getThresholdOperator( operator );
                builder.setOperator( canonicalOperator );
            }

            if ( Objects.nonNull( thresholds.getApplyTo() ) )
            {
                ThresholdDataType dataType = thresholds.getApplyTo();
                wres.statistics.generated.Threshold.ThresholdDataType canonicalDataType =
                        DeclarationFactory.getThresholdDataType( dataType.name() );
                builder.setDataType( canonicalDataType );
            }

            // Read the threshold values
            String csvString = internalOrExternal.toString();
            double[] values = Arrays.stream( csvString.split( "," ) )
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
        }
        // Service call to obtain concrete thresholds
        else
        {
            LOGGER.debug( "Discovered an external source of thresholds, but no explicitly declared thresholds to "
                          + "migrate from: {}.", thresholds );
        }

        return Collections.unmodifiableSet( migrated );
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
