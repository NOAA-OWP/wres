package wres.config.yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

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
                Set<GeometryGroup> adjustedGeoGroups = new HashSet<>();

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
        Set<GeometryTuple> adjustedGeometries = new HashSet<>();
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
