package wres.config.yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import io.soabase.recordbuilder.core.RecordBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import wres.statistics.generated.DurationScoreMetric.DurationScoreMetricComponent.ComponentName;
import wres.statistics.generated.GeometryTuple;
import wres.statistics.generated.MetricName;
import wres.statistics.generated.TimeScale;

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

    /** Schema file name on the classpath. */
    private static final String SCHEMA = "schema.yml";

    /** Root class for an evaluation declaration. */
    @RecordBuilder
    record EvaluationDeclaration(
            @JsonDeserialize( using = DatasetDeserializer.class ) @JsonProperty( "left" ) Dataset left,
            @JsonDeserialize( using = DatasetDeserializer.class ) @JsonProperty( "right" ) Dataset right,
            @JsonDeserialize( using = BaselineDeserializer.class ) @JsonProperty( "baseline" ) BaselineDataset baseline,
            @JsonDeserialize( using = FeaturesDeserializer.class ) @JsonProperty( "features" ) Set<GeometryTuple> features,
            @JsonProperty( "reference_dates" ) TimeInterval referenceDates,
            @JsonProperty( "reference_date_pools" ) TimePools referenceDatePools,
            @JsonProperty( "valid_dates" ) TimeInterval validDates,
            @JsonProperty( "valid_date_pools" ) TimePools validDatePools,
            @JsonProperty( "lead_times" ) LeadTimeInterval leadTimes,
            @JsonProperty( "lead_time_pools" ) TimePools leadTimePools,
            @JsonDeserialize( using = TimeScaleDeserializer.class ) @JsonProperty( "time_scale" ) TimeScale timeScale,
            @JsonDeserialize( using = ThresholdsDeserializer.class ) @JsonProperty( "probability_thresholds" ) Set<Threshold> probabilityThresholds,
            @JsonDeserialize( using = ThresholdsDeserializer.class ) @JsonProperty( "value_thresholds" ) Set<Threshold> valueThresholds,
            @JsonDeserialize( using = ThresholdsDeserializer.class ) @JsonProperty( "classifier_thresholds" ) Set<Threshold> classifierThresholds,
            @JsonDeserialize( using = MetricsDeserializer.class ) @JsonProperty( "metrics" ) List<Metric> metrics ) {}

    /** Left or right dataset. */
    @RecordBuilder
    record Dataset( @JsonProperty( "sources" ) List<Source> sources,
                    @JsonProperty( "variable" ) Variable variable,
                    @JsonProperty( "feature_authority" ) String featureAuthority,
                    @JsonProperty( "type" ) String type,
                    @JsonProperty( "label" ) String label,
                    @JsonProperty( "ensemble_filter" ) EnsembleFilter ensembleFilter,
                    @JsonProperty( "time_shift" ) Duration timeShift ) {}

    /** Baseline dataset. */
    @RecordBuilder
    record BaselineDataset( Dataset dataset,
                            @JsonProperty( "persistence" ) Integer persistence ) {}

    /** Variable. */
    @RecordBuilder
    record Variable( @JsonProperty( "name" ) String name, @JsonProperty( "label" ) String label ) {}

    /** Variable. */
    @RecordBuilder
    record EnsembleFilter( @JsonProperty( "members" ) List<String> members,
                           @JsonProperty( "exclude" ) boolean exclude ) {}

    /** A data source. */
    @RecordBuilder
    record Source( @JsonProperty( "uri" ) URI uri,
                   @JsonProperty( "interface" ) String api,
                   @JsonProperty( "pattern" ) String pattern,
                   @JsonProperty( "time_zone" ) String timeZone,
                   @JsonDeserialize( using = TimeScaleDeserializer.class ) @JsonProperty( "time_scale" ) TimeScale timeScale,
                   @JsonProperty( "missing_value" ) Double missingValue ) {}

    /** A metric. */
    @RecordBuilder
    record Metric( @JsonProperty( "name" ) MetricName name, MetricParameters parameters ) {}

    /** Metric parameters. */
    @RecordBuilder
    record MetricParameters( @JsonProperty( "probability_thresholds" ) Set<Threshold> probabilityThresholds,
                             @JsonProperty( "value_thresholds" ) Set<Threshold> valueThresholds,
                             @JsonProperty( "classifier_thresholds" ) Set<Threshold> classifierThresholds,
                             @JsonProperty( "summary_statistics" ) Set<ComponentName> summaryStatistics,
                             @JsonProperty( "minimum_sample_size" ) int minimumSampleSize ) {}

    /** Time interval. */
    @RecordBuilder
    record TimeInterval( @JsonProperty( "minimum" ) Instant minimum, @JsonProperty( "maximum" ) Instant maximum ) {}

    /** Lead time interval. */
    @RecordBuilder
    record LeadTimeInterval( @JsonProperty( "minimum" ) int minimum, @JsonProperty( "maximum" ) int maximum ) {}

    /** Time pools. */
    @RecordBuilder
    record TimePools( @JsonProperty( "period" ) int period, @JsonProperty( "frequency" ) int frequency,
                      @JsonProperty( "unit" ) ChronoUnit unit ) {}

    /** A threshold, optionally attached to a named feature. */
    @RecordBuilder
    record Threshold( wres.statistics.generated.Threshold threshold, String featureName ) {}

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
        URL schema = DeclarationFactory.class.getClassLoader().getResource( SCHEMA );

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

            // Map the declaration to a json node
            JsonNode declaration = MAPPER.readTree( yaml );

            LOGGER.debug( "Encountered a declaration string: {}.", yaml );

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
            return DeclarationFactory.finalizeDeclaration( declaration, mappedDeclaration );
        }
    }

    /**
     * Performs any additional generation steps that require the full declaration.
     *
     * @param declarationNode the parsed declaration
     * @param declaration the mapped declaration to adjust
     * @return the adjusted declaration
     */
    private static EvaluationDeclaration finalizeDeclaration( JsonNode declarationNode,
                                                              EvaluationDeclaration declaration )
    {
        EvaluationDeclaration adjustedDeclaration = declaration;

        // Add autogenerated features
        if ( declarationNode.has( "features" ) )
        {
            JsonNode featuresNode = declarationNode.get( "features" );

            Iterator<JsonNode> featureIterator = featuresNode.elements();

            Set<GeometryTuple> features = new HashSet<>( declaration.features() );
            boolean hasBaseline = Objects.nonNull( declaration.baseline() );
            while ( featureIterator.hasNext() )
            {
                JsonNode next = featureIterator.next();

                // Autogenerated feature
                if ( next.isValueNode() )
                {
                    GeometryTuple feature = FeaturesDeserializer.getGeneratedFeature( next.asText(), hasBaseline );
                    features.add( feature );
                }
            }

            adjustedDeclaration = new EvaluationDeclaration( declaration.left(),
                                                             declaration.right(),
                                                             declaration.baseline(),
                                                             features,
                                                             declaration.referenceDates(),
                                                             declaration.referenceDatePools(),
                                                             declaration.validDates(),
                                                             declaration.validDatePools(),
                                                             declaration.leadTimes(),
                                                             declaration.leadTimePools(),
                                                             declaration.timeScale(),
                                                             declaration.probabilityThresholds(),
                                                             declaration.valueThresholds(),
                                                             declaration.classifierThresholds(),
                                                             declaration.metrics() );
        }

        return adjustedDeclaration;
    }

    /**
     * Do not construct.
     */

    private DeclarationFactory()
    {
    }
}
