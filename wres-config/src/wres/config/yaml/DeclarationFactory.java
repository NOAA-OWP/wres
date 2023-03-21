package wres.config.yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
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
import wres.config.yaml.components.BaselineDataset;
import wres.config.yaml.components.BaselineDatasetBuilder;
import wres.config.yaml.components.DataType;
import wres.config.yaml.components.Dataset;
import wres.config.yaml.components.DatasetBuilder;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.FeatureGroups;
import wres.config.yaml.components.Features;
import wres.config.yaml.components.Metric;
import wres.config.yaml.components.EvaluationDeclarationBuilder;
import wres.config.yaml.components.FormatsBuilder;
import wres.statistics.generated.GeometryGroup;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.Outputs;

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
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( DeclarationFactory.class );

    /** Mapper to map a YAML string to POJOs. */
    private static final ObjectMapper MAPPER = YAMLMapper.builder()
                                                         .enable( MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS )
                                                         .build()
                                                         .registerModule( new ProtobufModule() )
                                                         .registerModule( new JavaTimeModule() )
                                                         .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
                                                                     true );

    /** To stringify protobufs into presentable JSON. */
    public static final Function<MessageOrBuilder, String> PROTBUF_STRINGIFIER = message ->
    {
        if ( Objects.nonNull( message ) )
        {
            try
            {
                return JsonFormat.printer()
                                 .omittingInsignificantWhitespace()
                                 .print( message );
            }
            catch ( InvalidProtocolBufferException e )
            {
                throw new IllegalStateException( "Failed to stringify a protobuf message." );
            }
        }

        return "null";
    };

    /** Schema file name on the classpath. */
    private static final String SCHEMA = "schema.yml";

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
     * @throws SchemaValidationException if the project declaration could not be validated against the schema
     * @throws DeclarationValidationException if the declaration is invalid
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
     * Deserializes a YAML string into a POJO and performs validation against the schema only. Does not interpolate any
     * options that require external calls, such as calls to feature services or threshold services, or options that
     * may be informed by data ingest, such as interpolation of the "type" of time-series data to evaluate. To perform
     * resolve implicit declaration via "interpolation", use {@link #interpolate(EvaluationDeclaration)}. Also, does
     * not perform any high-level validation of the declaration for mutual consistency and coherence. To perform
     * high-level validation, see the {@link DeclarationValidator}.
     *
     * @see DeclarationValidator#validateAndNotify(EvaluationDeclaration)
     * @see DeclarationValidator#validate(EvaluationDeclaration)
     * @param yaml the yaml string
     * @return an evaluation declaration
     * @throws IllegalStateException if the project declaration schema could not be found on the classpath
     * @throws IOException if the schema could not be read
     * @throws SchemaValidationException if the project declaration could not be validated against the schema
     * @throws NullPointerException if the yaml string is null
     */

    public static EvaluationDeclaration from( String yaml ) throws IOException
    {
        Objects.requireNonNull( yaml );

        // Get the schema from the classpath
        URL schema = DeclarationFactory.class.getClassLoader()
                                             .getResource( SCHEMA );

        LOGGER.debug( "Read the declaration schema from classpath resource '{}'.", SCHEMA );

        if ( Objects.isNull( schema ) )
        {
            throw new IllegalStateException( "Could not find the project declaration schema " + SCHEMA
                                             + "on the classpath." );
        }

        try ( InputStream stream = schema.openStream() )
        {
            String schemaString = new String( stream.readAllBytes(), StandardCharsets.UTF_8 );

            // Map the schema to a json node
            JsonNode schemaNode = MAPPER.readTree( schemaString );

            // Unfortunately, Jackson does not currently resolve anchors/aliases, despite using SnakeYAML under the
            // hood. Instead, use SnakeYAML to create a resolved string first. This is inefficient and undesirable
            // because it means  that the declaration string is being read twice
            // TODO: use Jackson to read the raw YAML string once it can handle anchors/aliases properly
            Yaml snakeYaml = new Yaml( new Constructor( new LoaderOptions() ),
                                       new Representer( new DumperOptions() ),
                                       new DumperOptions(),
                                       new CustomResolver() );
            Object resolvedYaml = snakeYaml.load( yaml );
            String resolvedYamlString = MAPPER.writerWithDefaultPrettyPrinter()
                                              .writeValueAsString( resolvedYaml );

            LOGGER.info( "Resolved a YAML declaration string: {}.", resolvedYaml );

            // Use Jackson to (re-)read the declaration string once any aliases/anchors are resolved
            JsonNode declaration = MAPPER.readTree( resolvedYamlString );

            JsonSchemaFactory factory =
                    JsonSchemaFactory.builder( JsonSchemaFactory.getInstance( SpecVersion.VersionFlag.V201909 ) )
                                     .objectMapper( MAPPER )
                                     .build();

            // Validate the declaration against the schema
            JsonSchema validator = factory.getSchema( schemaNode );

            Set<ValidationMessage> errors = validator.validate( declaration );

            LOGGER.debug( "Validated a declaration string against the schema, which produced {} errors.",
                          errors.size() );

            if ( !errors.isEmpty() )
            {
                throw new SchemaValidationException( "Encountered an error while attempting to validate a project "
                                                     + "declaration string against the schema. Please check your "
                                                     + "declaration and fix any errors. The errors encountered were: "
                                                     + errors );
            }

            LOGGER.debug( "Deserializing a declaration string into POJOs." );

            // Deserialize
            EvaluationDeclaration mappedDeclaration = MAPPER.reader()
                                                            .readValue( declaration, EvaluationDeclaration.class );

            // Handle any generation that requires the full declaration, but without performing interpolation of
            // options that are declared implicitly
            return DeclarationFactory.finalizeDeclaration( mappedDeclaration );
        }
    }

    /**
     * Interpolates the raw declaration, completing any required declaration that is missing and can be inferred from
     * other declaration. Currently, this method does not resolve into explicit "declaration" any attributes that
     * require service calls, such as features that are resolved by a feature service and thresholds that are resolved
     * by a threshold service.
     *
     * @param declaration the raw declaration to interpolate
     * @return the interpolated declaration
     */
    public static EvaluationDeclaration interpolate( EvaluationDeclaration declaration )
    {
        EvaluationDeclarationBuilder adjustedDeclarationBuilder
                = EvaluationDeclarationBuilder.builder( declaration );

        // Disambiguate the "type" of data when it is not declared
        DeclarationFactory.interpolateDataTypes( adjustedDeclarationBuilder );

        // Interpolate geospatial features when required, but without using a feature service
        DeclarationFactory.interpolateFeaturesWithoutFeatureService( adjustedDeclarationBuilder );

        // Interpolate evaluation metrics when required
        DeclarationFactory.interpolateMetrics( adjustedDeclarationBuilder );

        return adjustedDeclarationBuilder.build();
    }

    /**
     * Returns an enum-friendly name from a node whose text value corresponds to an enum.
     * @param node the enum node
     * @return the enum-friendly name
     */

    public static String getEnumFriendlyName( JsonNode node )
    {
        return node.asText()
                   .toUpperCase()
                   .replace( " ", "_" );
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
        if ( Objects.nonNull( builder.right()
                                     .ensembleFilter() ) )
        {
            ensembleDeclaration.add( "An 'ensemble_filter' was declared on the predicted dataset." );
        }

        // Ensemble filter on baseline dataset?
        if ( DeclarationFactory.hasBaseline( builder )
             && Objects.nonNull( builder.baseline()
                                        .dataset()
                                        .ensembleFilter() ) )
        {
            ensembleDeclaration.add( "An 'ensemble_filter' was declared on the baseline dataset." );
        }

        // Any source that contains an interface with the word ensemble?
        Set<String> ensembleInterfaces = DeclarationFactory.getSourcesWithEnsembleInterface( builder );
        if ( !ensembleInterfaces.isEmpty() )
        {
            ensembleDeclaration.add( "Discovered one or more data sources whose interfaces are ensemble-like: "
                                     + ensembleInterfaces
                                     + "." );
        }

        // Ensemble average declared?
        if ( Objects.nonNull( builder.ensembleAverageType() ) )
        {
            ensembleDeclaration.add( "An 'ensemble_average' was declared." );
        }

        // Ensemble metrics?
        Set<String> ensembleMetrics = DeclarationFactory.getMetricType( builder,
                                                                        MetricConstants.SampleDataGroup.ENSEMBLE );
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
        return Objects.nonNull( builder.analysisDurations() )
               && ( Objects.nonNull( builder.analysisDurations().minimum() )
                    || Objects.nonNull( builder.analysisDurations().maximumExclusive() ) );
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
                                     + forecastInterfaces + "." );
        }

        return Collections.unmodifiableSet( forecastDeclaration );
    }

    /**
     * Performs any additional generation steps that require the full declaration.
     *
     * @param declaration the mapped declaration to adjust
     * @return the adjusted declaration
     */
    private static EvaluationDeclaration finalizeDeclaration( EvaluationDeclaration declaration )
    {
        EvaluationDeclarationBuilder adjustedDeclarationBuilder
                = EvaluationDeclarationBuilder.builder( declaration );

        // Add the evaluation-wide decimal format string to each numeric format type
        DeclarationFactory.addDecimalFormatStringToNumericFormats( adjustedDeclarationBuilder );

        // Add metrics for which graphics should be ignored to the graphics format options
        DeclarationFactory.addMetricsWithoutGraphicsToGraphicsFormats( adjustedDeclarationBuilder );

        return adjustedDeclarationBuilder.build();
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
                    Set<GeometryTuple> adjustedTuples = DeclarationFactory.getAutogeneratedGeometries( nextTuples,
                                                                                                       hasBaseline );

                    GeometryGroup nextAdjustedGroup = nextGroup.toBuilder()
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
        DataType rightType = builder.right()
                                    .type();

        // No metrics defined and the type is known, so interpolate all valid metrics
        if ( builder.metrics()
                    .isEmpty()
             && Objects.nonNull( rightType ) )
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
                    Set<MetricConstants> dichotomous =
                            MetricConstants.SampleDataGroup.DICHOTOMOUS.getMetrics();
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
                    Set<MetricConstants> dichotomous =
                            MetricConstants.SampleDataGroup.DICHOTOMOUS.getMetrics();
                    metrics.addAll( dichotomous );
                }
            }

            // Remove any metrics that are incompatible with other declaration
            DeclarationFactory.removeIncompatibleMetrics( builder, metrics );

            // Wrap the metrics and set them
            Set<Metric> wrapped = metrics.stream()
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
    private static void removeIncompatibleMetrics( EvaluationDeclarationBuilder builder,
                                                   Set<MetricConstants> metrics )
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
    private static void addDecimalFormatStringToNumericFormats( EvaluationDeclarationBuilder builder )
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
            numericBuilder.setDecimalFormat( builder.decimalFormat() );
            csv2Builder.setOptions( numericBuilder );
            formatsBuilder.csv2Format( csv2Builder.build() );
        }

        if ( Objects.nonNull( formatsBuilder.csvFormat() ) )
        {
            Outputs.CsvFormat.Builder csvBuilder = formatsBuilder.csvFormat()
                                                                 .toBuilder();
            Outputs.NumericFormat.Builder numericBuilder = csvBuilder.getOptionsBuilder();
            numericBuilder.setDecimalFormat( builder.decimalFormat() );
            csvBuilder.setOptions( numericBuilder );
            formatsBuilder.csvFormat( csvBuilder.build() );
        }

        if ( Objects.nonNull( formatsBuilder.pairsFormat() ) )
        {
            Outputs.PairFormat.Builder pairsBuilder = formatsBuilder.pairsFormat()
                                                                    .toBuilder();
            Outputs.NumericFormat.Builder numericBuilder = pairsBuilder.getOptionsBuilder();
            numericBuilder.setDecimalFormat( builder.decimalFormat() );
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
    private static void addMetricsWithoutGraphicsToGraphicsFormats( EvaluationDeclarationBuilder builder )
    {
        // Something to adjust?
        if ( Objects.isNull( builder.formats() ) )
        {
            LOGGER.debug( "No graphical formats were discovered and, therefore, adjusted for metrics to ignore." );
            return;
        }

        FormatsBuilder formatsBuilder = FormatsBuilder.builder( builder.formats() );

        List<MetricName> avoid = builder.metrics()
                                        .stream()
                                        .filter( next -> Objects.nonNull( next.parameters() ) && !next.parameters()
                                                                                                      .graphics() )
                                        .map( Metric::name )
                                        .map( next -> MetricName.valueOf( next.name() ) )
                                        .toList();

        LOGGER.debug( "Discovered these metrics to avoid, which will be registered with all graphics formats: {}.",
                      avoid );

        if ( Objects.nonNull( formatsBuilder.pngFormat() ) )
        {
            Outputs.PngFormat.Builder pngBuilder = formatsBuilder.pngFormat()
                                                                 .toBuilder();
            Outputs.GraphicFormat.Builder graphicBuilder = pngBuilder.getOptionsBuilder();
            graphicBuilder.clearIgnore()
                          .addAllIgnore( avoid );
            pngBuilder.setOptions( graphicBuilder );
            formatsBuilder.pngFormat( pngBuilder.build() );
        }

        if ( Objects.nonNull( formatsBuilder.svgFormat() ) )
        {
            Outputs.SvgFormat.Builder svgBuilder = formatsBuilder.svgFormat()
                                                                 .toBuilder();
            Outputs.GraphicFormat.Builder graphicBuilder = svgBuilder.getOptionsBuilder();
            graphicBuilder.clearIgnore()
                          .addAllIgnore( avoid );
            svgBuilder.setOptions( graphicBuilder );
            formatsBuilder.svgFormat( svgBuilder.build() );
        }

        // Set the new format info
        builder.formats( formatsBuilder.build() );
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
                LOGGER.warn( defaultMessage, "Assuming that the 'type' is 'analyses' because analysis durations "
                                             + "were discovered and analyses are typically used to verify other "
                                             + "datasets." );
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
                               .anyMatch( next -> Objects.nonNull( next.api() ) && next.api()
                                                                                       .getDataTypes()
                                                                                       .size() > 1 ) )
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
            Dataset newPredicted = DatasetBuilder.builder( predicted )
                                                 .type( dataType )
                                                 .build();
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
                      .filter( next -> next.name()
                                           .isInGroup( groupType ) )
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
                                    .filter( next -> Objects.nonNull( next.api() )
                                                     && next.api()
                                                            .getDataTypes()
                                                            .equals( Set.of( DataType.ENSEMBLE_FORECASTS ) ) )
                                    .map( next -> next.api().toString() )
                                    .toList();

        Set<String> ensembleInterfaces = new TreeSet<>( right );

        if ( DeclarationFactory.hasBaseline( builder ) )
        {
            List<String> baseline = builder.right()
                                           .sources()
                                           .stream()
                                           .filter( next -> Objects.nonNull( next.api() )
                                                            && next.api()
                                                                   .getDataTypes()
                                                                   .equals( Set.of( DataType.ENSEMBLE_FORECASTS ) ) )
                                           .map( next -> next.api().toString() )
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
                                    .filter( next -> Objects.nonNull( next.api() )
                                                     && next.api()
                                                            .getDataTypes()
                                                            .stream()
                                                            .allMatch( DataType::isForecastType ) )
                                    .map( next -> next.api().toString() )
                                    .toList();

        interfaces.addAll( right );

        if ( DeclarationFactory.hasBaseline( builder ) )
        {
            List<String> baseline = builder.baseline()
                                           .dataset()
                                           .sources()
                                           .stream()
                                           .filter( next -> Objects.nonNull( next.api() )
                                                            && next.api()
                                                                   .getDataTypes()
                                                                   .stream()
                                                                   .allMatch( DataType::isForecastType ) )
                                           .map( next -> next.api().toString() )
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
                BaselineDataset newBaseline = BaselineDatasetBuilder.builder( baseline )
                                                                    .dataset( newBaselineDataset )
                                                                    .build();
                builder.baseline( newBaseline );

                LOGGER.warn(
                        "While reading the project declaration, discovered that the baseline dataset had no declared "
                        + "data 'type'. Assuming that the 'type' is '{}' to match the type of the predicted "
                        + "dataset. If this is incorrect, please declare the 'type' explicitly.",
                        newBaselineDataset.type() );
            }
        }
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
