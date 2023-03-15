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
import java.util.function.Predicate;

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
import wres.config.yaml.components.Source;
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
     * Deserializes a YAML string into a POJO and performs validation against the schema.
     *
     * @param yaml the yaml string
     * @return an evaluation declaration
     * @throws IllegalStateException if the project declaration schema could not be found on the classpath
     * @throws IOException if the schema could not be read
     * @throws SchemaValidationException if the project declaration could not be validated against the schema
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
            // hood, which does resolve them properly. Instead, use SnakeYAML to create a resolved string first
            // TODO: use Jackson to read the raw YAML string once it can handle anchors/aliases properly
            Yaml snakeYaml = new Yaml( new Constructor( new LoaderOptions() ),
                                       new Representer( new DumperOptions() ),
                                       new DumperOptions(),
                                       new CustomResolver() );
            Object resolvedYaml = snakeYaml.load( yaml );
            String resolvedYamlString = MAPPER.writerWithDefaultPrettyPrinter()
                                              .writeValueAsString( resolvedYaml );

            LOGGER.info( "Resolved a YAML declaration string: {}.", resolvedYaml );

            // Use Jackson from here
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

            // Handle any generation that requires the full declaration, such as generated features
            return DeclarationFactory.finalizeDeclaration( mappedDeclaration );
        }
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
     * Performs any additional generation steps that require the full declaration.
     *
     * @param declaration the mapped declaration to adjust
     * @return the adjusted declaration
     */
    private static EvaluationDeclaration finalizeDeclaration( EvaluationDeclaration declaration )
    {
        EvaluationDeclarationBuilder adjustedDeclarationBuilder
                = EvaluationDeclarationBuilder.builder( declaration );

        // Disambiguate the "type" of data when it is not declared
        DeclarationFactory.resolveDataTypesIfRequired( adjustedDeclarationBuilder );

        // Add autogenerated geospatial features, but only if there is no feature service to resolve them
        if ( Objects.isNull( declaration.featureService() ) )
        {
            DeclarationFactory.addAutogeneratedFeatures( adjustedDeclarationBuilder );
        }

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
    private static void addAutogeneratedFeatures( EvaluationDeclarationBuilder builder )
    {
        // Add autogenerated features
        if ( Objects.isNull( builder.featureService() ) )
        {
            boolean hasBaseline = Objects.nonNull( builder.baseline() );

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
    private static void resolveDataTypesIfRequired( EvaluationDeclarationBuilder builder )
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
                                    + "no declared data \"type\". {} If this is incorrect, please declare the \"type\" "
                                    + "explicitly.";

            // Analysis durations present? If so, assume analyses
            if ( DeclarationFactory.hasAnalysisDurations( builder ) )
            {
                Dataset newLeft = DatasetBuilder.builder( observed )
                                                .type( DataType.ANALYSES )
                                                .build();
                builder.left( newLeft );

                // Log the reason
                LOGGER.warn( defaultMessage, "Assuming that the \"type\" is \"analyses\" because analysis durations "
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
                LOGGER.warn( defaultMessage, "Assuming that the \"type\" is \"observations\"." );
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
                                    + "no declared data \"type\". {} If this is incorrect, please declare the \"type\" "
                                    + "explicitly.";

            String reasonMessage = "";

            DataType dataType = null;

            // Discover hints from the declaration
            Set<String> ensembleDeclaration = DeclarationFactory.getEnsembleDeclaration( builder );
            Set<String> forecastDeclaration = DeclarationFactory.getForecastDeclaration( builder );

            // Ensemble declaration?
            if ( !ensembleDeclaration.isEmpty() )
            {
                reasonMessage = "Setting the \"type\" to \"ensemble forecasts\" because the following ensemble "
                                + "declaration was discovered: " + ensembleDeclaration + ".";
                dataType = DataType.ENSEMBLE_FORECASTS;
            }
            // Forecast declaration?
            else if ( !forecastDeclaration.isEmpty() )
            {
                reasonMessage = "Setting the \"type\" to \"single valued forecasts\" because the following forecast "
                                + "declaration was discovered and no ensemble declaration was discovered to suggest "
                                + "that the forecasts are \"ensemble forecasts\": " + forecastDeclaration + ".";
                dataType = DataType.SINGLE_VALUED_FORECASTS;
            }
            else
            {
                reasonMessage = "Setting the \"type\" to \"simulations\" because no declaration that was discovered to "
                                + "suggest that any dataset contains \"single valued forecasts\" or \"ensemble "
                                + "forecast\".";

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
            ensembleDeclaration.add( "An \"ensemble filter\" was declared on the predicted dataset." );
        }

        // Ensemble filter on baseline dataset?
        if ( DeclarationFactory.hasBaseline( builder )
             && Objects.nonNull( builder.baseline()
                                        .dataset()
                                        .ensembleFilter() ) )
        {
            ensembleDeclaration.add( "An \"ensemble filter\" was declared on the baseline dataset." );
        }

        // Any source that contains an interface with the word ensemble?
        if ( DeclarationFactory.hasSourcesWithAnEnsembleInterface( builder ) )
        {
            ensembleDeclaration.add( "Discovered one or more data sources whose interfaces are ensemble-like." );
        }

        // Ensemble average declared?
        if ( Objects.nonNull( builder.ensembleAverageType() ) )
        {
            ensembleDeclaration.add( "An \"ensemble_average\" was declared." );
        }

        // Ensemble metrics?
        if ( DeclarationFactory.hasMetricType( builder, MetricConstants.SampleDataGroup.ENSEMBLE ) )
        {
            ensembleDeclaration.add( "Discovered metrics that focus on ensemble forecasts." );
        }

        // Discrete probability metrics?
        if ( DeclarationFactory.hasMetricType( builder, MetricConstants.SampleDataGroup.DISCRETE_PROBABILITY ) )
        {
            ensembleDeclaration.add( "Discovered metrics that focus on discrete probability forecasts and these can "
                                     + "only be obtained from ensemble forecasts, currently." );
        }

        return Collections.unmodifiableSet( ensembleDeclaration );
    }

    /**
     * @param builder the builder
     * @param groupType the group type
     * @return whether there are any metrics with the designated type
     */
    private static boolean hasMetricType( EvaluationDeclarationBuilder builder,
                                          MetricConstants.SampleDataGroup groupType )
    {
        return builder.metrics()
                      .stream()
                      .anyMatch( next -> next.name()
                                             .isInGroup( groupType ) );
    }

    /**
     * @param builder the builder
     * @return whether any source has an interface/api with an ensemble designation
     */
    private static boolean hasSourcesWithAnEnsembleInterface( EvaluationDeclarationBuilder builder )
    {
        boolean predicted = builder.right()
                                   .sources()
                                   .stream()
                                   .anyMatch( next -> Objects.nonNull( next.api() )
                                                      && next.api()
                                                             .contains( "ensemble" ) );

        boolean baseline = false;

        if ( DeclarationFactory.hasBaseline( builder ) )
        {
            baseline = builder.baseline()
                              .dataset()
                              .sources()
                              .stream()
                              .anyMatch( next -> Objects.nonNull( next.api() )
                                                 && next.api()
                                                        .contains( "ensemble" ) );
        }

        return predicted || baseline;
    }

    /**
     * @param builder the builder
     * @return whether any source has an interface/api with a forecast-like designation
     */
    private static boolean hasSourcesWithAForecastInterface( EvaluationDeclarationBuilder builder )
    {
        boolean ensemble = DeclarationFactory.hasSourcesWithAnEnsembleInterface( builder );

        // All ensembles are forecasts, currently
        if ( ensemble )
        {
            return true;
        }

        Predicate<Source> forecastMatcher = name ->
        {
            String api = name.api();
            if ( Objects.nonNull( api ) )
            {
                api = api.toLowerCase();
                return api.contains( "deterministic" ) || api.contains( "short_range" )
                       || api.contains( "medium_range" ) || api.contains( "long_range" );
            }

            return false;
        };

        boolean predicted = builder.right()
                                   .sources()
                                   .stream()
                                   .anyMatch( forecastMatcher );

        boolean baseline = false;

        if ( DeclarationFactory.hasBaseline( builder ) )
        {
            baseline = builder.baseline()
                              .dataset()
                              .sources()
                              .stream()
                              .anyMatch( forecastMatcher );
        }

        return predicted || baseline;
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
            forecastDeclaration.add( "Discovered a \"reference_dates\" filter." );
        }

        // Reference time pools?
        if ( Objects.nonNull( builder.referenceDatePools() ) )
        {
            forecastDeclaration.add( "Discovered \"reference_date_pools\"." );
        }

        // Lead times?
        if ( Objects.nonNull( builder.leadTimes() ) )
        {
            forecastDeclaration.add( "Discovered a \"lead_times\" filter." );
        }

        // Lead time pools?
        if ( Objects.nonNull( builder.leadTimePools() ) )
        {
            forecastDeclaration.add( "Discovered \"lead_time_pools\"." );
        }

        // One or more sources with a forecast-like interface
        if ( DeclarationFactory.hasSourcesWithAForecastInterface( builder ) )
        {
            forecastDeclaration.add( "Discovered one or more data sources whose interfaces are forecast-like." );
        }

        return Collections.unmodifiableSet( forecastDeclaration );
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
                        + "data \"type\". Assuming that the \"type\" is \"{}\" to match the type of the predicted "
                        + "dataset. If this is incorrect, please declare the \"type\" explicitly.",
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
