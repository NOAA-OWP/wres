package wres.config.yaml;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.io.IOContext;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.resolver.Resolver;

import wres.config.yaml.components.DatasetOrientation;
import wres.config.yaml.components.EvaluationDeclaration;
import wres.config.yaml.components.Threshold;
import wres.config.yaml.components.ThresholdOperator;
import wres.config.yaml.components.ThresholdOrientation;
import wres.config.yaml.components.ThresholdType;
import wres.config.yaml.serializers.CustomGenerator;
import wres.statistics.generated.EvaluationStatus.EvaluationStatusEvent;

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

    /** Default threshold dataset orientation. */
    public static final DatasetOrientation DEFAULT_THRESHOLD_DATASET_ORIENTATION = DatasetOrientation.LEFT;

    /** Default threshold type (e.g., for a threshold data service). */
    public static final ThresholdType DEFAULT_THRESHOLD_TYPE = ThresholdType.VALUE;

    /** Default threshold missing value. Ugly, but the most common value seen in the wild at the time of writing. */
    public static final double DEFAULT_MISSING_VALUE = -999.0;

    /** Default canonical threshold with no values. */
    public static final wres.statistics.generated.Threshold DEFAULT_CANONICAL_THRESHOLD
            = wres.statistics.generated.Threshold.newBuilder()
                                                 .setDataType( DEFAULT_THRESHOLD_ORIENTATION.canonical() )
                                                 .setOperator( DEFAULT_THRESHOLD_OPERATOR.canonical() )
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

    /** Schema file name on the classpath. */
    private static final String SCHEMA = "schema.yml";

    /** Mapper for deserialization. */
    private static final ObjectMapper DESERIALIZER = YAMLMapper.builder()
                                                               .enable( MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS )
                                                               .enable( DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY )
                                                               .enable( DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY )
                                                               .enable( JsonParser.Feature.STRICT_DUPLICATE_DETECTION )
                                                               .build()
                                                               .registerModule( new ProtobufModule() )
                                                               .registerModule( new JavaTimeModule() )
                                                               .configure( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES,
                                                                           true );

    /** Mapper for serialization. */
    private static final ObjectMapper SERIALIZER =
            new ObjectMapper( new YamlFactoryWithCustomGenerator().disable( YAMLGenerator.Feature.WRITE_DOC_START_MARKER )
                                                                  .disable( YAMLGenerator.Feature.SPLIT_LINES )
                                                                  .enable( YAMLGenerator.Feature.MINIMIZE_QUOTES )
                                                                  .enable( YAMLGenerator.Feature.ALWAYS_QUOTE_NUMBERS_AS_STRINGS )
                                                                  .configure( YAMLGenerator.Feature.INDENT_ARRAYS_WITH_INDICATOR,
                                                                              true ) )
                    .registerModule( new JavaTimeModule() )
                    .enable( DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES )
                    .enable( SerializationFeature.WRITE_ENUMS_USING_TO_STRING )
                    .disable( SerializationFeature.WRITE_DATES_AS_TIMESTAMPS )
                    .enable( SerializationFeature.WRITE_SINGLE_ELEM_ARRAYS_UNWRAPPED )
                    .setSerializationInclusion( JsonInclude.Include.NON_NULL )
                    .setSerializationInclusion( JsonInclude.Include.NON_EMPTY );

    /**
     * Deserializes a YAML string or path containing a YAML string into a POJO and performs validation against the
     * schema. Optionally performs interpolation of missing declaration, followed by validation of the interpolated
     * declaration. Interpolation is performed with
     * {@link DeclarationInterpolator#interpolate(EvaluationDeclaration)} and validation is performed with
     * {@link DeclarationValidator#validate(EvaluationDeclaration, boolean)}, both with notifications on.
     *
     * @see #from(String)
     * @param yamlOrPath the YAML string or a path to a readable file that contains a YAML string
     * @param fileSystem a file system to use when reading a path, optional
     * @param interpolate is true to interpolate any missing declaration
     * @param validate is true to validate the declaration
     * @return an evaluation declaration
     * @throws IllegalStateException if the project declaration schema could not be found on the classpath
     * @throws IOException if the schema could not be read
     * @throws FileNotFoundException if the input string is a path, but the path points to a missing file
     * @throws DeclarationSchemaException if the project declaration could not be validated against the schema
     * @throws DeclarationException if the declaration is invalid
     * @throws NullPointerException if the input string is null
     */

    public static EvaluationDeclaration from( String yamlOrPath,
                                              FileSystem fileSystem,
                                              boolean interpolate,
                                              boolean validate ) throws IOException
    {
        Objects.requireNonNull( yamlOrPath );

        String declarationString = yamlOrPath;

        // Use the default file system when none was supplied
        if ( Objects.isNull( fileSystem ) )
        {
            fileSystem = FileSystems.getDefault();
        }

        // Does the path point to a readable file?
        if ( DeclarationUtilities.isReadableFile( fileSystem, yamlOrPath ) )
        {
            Path path = fileSystem.getPath( yamlOrPath );
            declarationString = Files.readString( path );
            LOGGER.info( "Discovered a path to a declaration string: {}", path );
        }
        // Probably a file path, but not a valid one
        else if ( declarationString.split( System.lineSeparator() ).length <= 1
                  && !DeclarationFactory.isDeclarationString( declarationString ) )
        {
            throw new FileNotFoundException( "The following declaration string appears to be a path, but the file "
                                             + "cannot be found on the default file system: "
                                             + declarationString
                                             + ". Please check that the "
                                             + "file exists and try again with a valid path or declaration string." );
        }

        if ( LOGGER.isDebugEnabled() )
        {
            LOGGER.debug( "Discovered a declaration string to read:{}{}", System.lineSeparator(), declarationString );
        }

        EvaluationDeclaration declaration;
        try
        {
            declaration = DeclarationFactory.from( declarationString );
        }
        catch ( DeclarationSchemaException s )
        {
            throw new DeclarationException( "Encountered an error when attempting to read a declaration string "
                                            + "or a path to a file that contains a declaration string. If a path "
                                            + "was supplied, please check that the path identifies a readable file "
                                            + "that contains a declaration string. Otherwise, please fix the "
                                            + "declaration string. The first line of the supplied string was: "
                                            + DeclarationUtilities.getFirstLine( yamlOrPath ), s );
        }

        // Validate? Do this before interpolation
        if ( validate )
        {
            DeclarationValidator.validate( declaration, true );
        }

        // Interpolate?
        if ( interpolate )
        {
            declaration = DeclarationInterpolator.interpolate( declaration );
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
     * @see DeclarationInterpolator#interpolate(EvaluationDeclaration)
     * @see DeclarationValidator#validate(EvaluationDeclaration, boolean)
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

        if ( LOGGER.isInfoEnabled() )
        {
            LOGGER.info( "Encountered the following declaration string:{}{}{}{}",
                         System.lineSeparator(),
                         "---",
                         System.lineSeparator(),
                         yaml.trim() );
        }

        // Get the declaration node
        JsonNode declaration = DeclarationFactory.deserialize( yaml );

        // Get the schema
        JsonSchema schema = DeclarationFactory.getSchema();

        // Validate against the schema
        Set<EvaluationStatusEvent> errors = DeclarationValidator.validate( declaration, schema );

        if ( !errors.isEmpty() )
        {
            StringJoiner message = new StringJoiner( System.lineSeparator() );
            String spacer = "    - ";
            errors.forEach( e -> message.add( spacer + e.getEventMessage() ) );

            throw new DeclarationException( "When comparing the declared evaluation to the schema, encountered "
                                            + errors.size()
                                            + " errors, which must be fixed. Hint: some of these errors may have the "
                                            + "same origin, so look for the most precise/informative error(s) among "
                                            + "them. The errors are:"
                                            + System.lineSeparator() +
                                            message );
        }

        LOGGER.debug( "Deserializing a declaration string into POJOs." );

        // Deserialize
        return DESERIALIZER.reader()
                           .readValue( declaration, EvaluationDeclaration.class );
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
     * Examines the input and determines whether it is a new-style declaration string.
     *
     * @param candidate the candidate declaration string
     * @return whether the input is a new-style declaration string
     * @throws NullPointerException if the input is null
     */

    public static boolean isDeclarationString( String candidate )
    {
        Objects.requireNonNull( candidate );

        // Could parse fully, but this should be good enough as the goal is detection for further processing
        return candidate.contains( "observed" )
               && candidate.contains( "predicted" );
    }

    /**
     * Deserializes a YAML string into a {@link JsonNode} for further processing.
     * @param yaml the yaml string
     * @return the node
     * @throws JsonProcessingException if the string could not be deserialized
     */

    static JsonNode deserialize( String yaml ) throws JsonProcessingException
    {
        // Unfortunately, Jackson does not currently resolve anchors/references, despite using SnakeYAML under the
        // hood. Instead, use SnakeYAML to create a resolved string first. This is highly inefficient and undesirable
        // because it means that the declaration string is deserialized, then serialized, then deserialized again.
        // Aside from being ugly, this will be a performance bottleneck for very large declarations.
        // TODO: use Jackson to read the raw YAML string once it can handle anchors/references properly
        DumperOptions dumperOptions = new DumperOptions();
        LoaderOptions loaderOptions = new LoaderOptions();
        // Disallow duplicate keys. This is already disallowed for Jackson in case this step can be removed in future
        loaderOptions.setAllowDuplicateKeys( false );
        Yaml snakeYaml = new Yaml( new Constructor( loaderOptions ),
                                   new Representer( dumperOptions ),
                                   dumperOptions,
                                   loaderOptions,
                                   new CustomResolver() );

        // Deserialize with SnakeYAML
        Object resolvedYaml = snakeYaml.load( yaml );

        // Serialize
        String resolvedYamlString =
                DESERIALIZER.writerWithDefaultPrettyPrinter()
                            .writeValueAsString( resolvedYaml );

        // Deserialize with Jackson now that any anchors/references are resolved
        return DESERIALIZER.readTree( resolvedYamlString );
    }

    /**
     * Looks for the schema on the classpath and deserializes it.
     * @return the schema
     * @throws IOException if the schema could not be found or read for any reason
     */

    static JsonSchema getSchema() throws IOException
    {
        // Get the schema from the classpath
        URL schema = DeclarationFactory.class.getClassLoader().getResource( SCHEMA );

        LOGGER.debug( "Read the declaration schema from classpath resource '{}'.", SCHEMA );

        if ( Objects.isNull( schema ) )
        {
            throw new IOException( "Could not find the project declaration schema "
                                   + SCHEMA
                                   + "on the classpath." );
        }

        try ( InputStream stream = schema.openStream() )
        {
            String schemaString = new String( stream.readAllBytes(), StandardCharsets.UTF_8 );

            // Map the schema to a json node
            JsonNode schemaNode = DESERIALIZER.readTree( schemaString );

            JsonSchemaFactory factory =
                    JsonSchemaFactory.builder( JsonSchemaFactory.getInstance( SpecVersion.VersionFlag.V201909 ) )
                                     .objectMapper( DESERIALIZER )
                                     .build();

            return factory.getSchema( schemaNode );
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

    /**
     * See the explanation in {@link CustomGenerator}. This is a workaround to expose the SnakeYAML
     * serialization options that Jackson can see, but fails to expose to configuration.
     *
     * @author James Brown
     */
    private static class YamlFactoryWithCustomGenerator extends YAMLFactory
    {
        @Override
        protected YAMLGenerator _createGenerator( Writer out, IOContext ctxt ) throws IOException
        {
            return new CustomGenerator( ctxt, _generatorFeatures, _yamlGeneratorFeatures,
                                        _quotingChecker, _objectCodec, out, _version );
        }
    }

}
