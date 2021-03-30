package wres.config;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import jsonvalues.JsArray;
import jsonvalues.JsInt;
import jsonvalues.JsObj;
import jsonvalues.JsPath;
import jsonvalues.JsStr;
import jsonvalues.JsValue;
import jsonvalues.spec.JsErrorPair;
import jsonvalues.spec.JsObjSpec;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static jsonvalues.JsPath.path;
import static jsonvalues.spec.JsSpecs.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class JsonValuesExplorationTest
{
    private static final Logger LOGGER = LoggerFactory.getLogger( JsonValuesExplorationTest.class );

    private static final String SAMPLE_YAML = "project: Routine NSE\n"
                                              + "left:\n"
                                              + "    siteStatus: active\n"
                                              + "    url: https://nwis.usgs.com\n"
                                              + "right:\n"
                                              + "    url: https://wrds.owp.com\n"
                                              + "    configuration: medium_range\n"
                                              + "    ensemble_member: 1\n"
                                              + "projectUnits: left\n"
                                              + "metrics:\n"
                                              + "    - NSE\n"
                                              + "outputs:\n"
                                              + "    url: file:///home/GID/WPOD/Viz\n"
                                              + "    format: CSV\n"
                                              + "referenceTimes:\n"
                                              + "    - P10D\n"
                                              + "routine: 0 */6 * * *\n";

    @Test
    void exploreWhatIsShownInReadme()
    {
        JsObj json = JsObj.parse(  "{ \"chickens\": 3 }" );
        JsObj yaml = JsObj.parseYaml( "chickens: 3" );
        JsObj person = JsObj.of( "name", JsStr.of( "Cheese" ),
                               "year", JsInt.of( 1983 ),
                               "languages", JsArray.of( "Haskell", "Scala", "Java", "Clojure" ),
                               "github", JsStr.of( "jbic" ),
                               "profession", JsStr.of( "programmer" ),
                               "address", JsObj.of("city", JsStr.of( "Tuscaloosa" ),
                                                  "location", JsArray.of(-87.565, 33.189 ),
                                                  "country",JsStr.of( "US" ) ) );

        Function<String,String> toSneakeCase = s -> s.replace( '_', '(' );

        // first level
        JsObj snakeKeys = person.mapKeys( toSneakeCase );

        // traverses all the elements
        JsObj snakeAllKeys = person.mapAllKeys( toSneakeCase );

        //person.mapAllValues( String::trim, Objects::nonNull );
        JsObj fields = person.filterAllKeys( k -> k.startsWith( "_field" ) );

        JsObj noNulls = person.filterAllValues( JsValue::isNotNull );

        //JsObj reduced = person.reduceAll( plus, JsValue::isInt );

        //RFC 6901
        JsPath path = path( "/a/b" );
        JsObj result = person.set( path, json );
        LOGGER.info( "Without added path: {}", person );
        LOGGER.info( "With added json at path {}: {}", path, result );
        LOGGER.info( "From yaml: {}", yaml );
        assertFalse( person.containsPath( path ) );
        assertTrue( result.containsPath( path ) );
    }


    @Test
    void parseSomeYaml()
    {
        JsObj yaml = JsObj.parseYaml( SAMPLE_YAML );

        // Get the metrics
        JsPath path = path( "/metrics" );
        JsValue result = yaml.get( path );
        LOGGER.info( "Metrics found: {}", result );
        assertTrue( result.isArray() );
    }

    @Test
    void parseSomeYamlMoreFluently()
    {
        // Get the metrics
        JsValue result = JsObj.parseYaml( SAMPLE_YAML )
                              .get( path( "/metrics" ) )
                              .toJsArray()
                              .get( 0 );
        LOGGER.info( "First metric found: {}", result );
        assertTrue( result.isStr() );
    }


    @Test
    void checkYamlVsSpec()
    {
        Predicate<String> isDuration = s ->
        {
            try
            {
                Duration.parse( s );
                return true;
            }
            catch ( DateTimeParseException dtpe )
            {
                return false;
            }
        };

        // This predicate will be false for values with quotes around them.
        Predicate<JsArray> allDurations = d -> d.stream()
                                                .map( pair -> pair.value )
                                                //.peek( v -> LOGGER.info( "JsValue='{}'", v ) )
                                                .map( JsValue::toString )
                                                //.peek( s -> LOGGER.info( "String='{}'", s ) )
                                                .allMatch( isDuration );
        JsObj fromYaml = JsObj.parseYaml( SAMPLE_YAML );
        JsObjSpec lrbSpec = JsObjSpec.strict( "url", str );

        // It doesn't look like more than 20 keys are supported. If we flatten,
        // maybe that could become a problem (unless there's a way to merge
        // these at the same level).
        JsObjSpec topLevelSpec = JsObjSpec.lenient( "left", lrbSpec,
                                                    "right", lrbSpec,
                                                    "baseline", lrbSpec.optional(),
                                                    "projectUnits", str,
                                                    "metrics", arrayOfStr,
                                                    "outputs", any,
                                                    "referenceTimes", arrayOfStrSuchThat( allDurations ),
                                                    "routine", str );
        Set<JsErrorPair> validationFailures = topLevelSpec.test( fromYaml );
        LOGGER.info( "Spec validation failures: {}", validationFailures );
        assertNotNull( validationFailures );
        assertFalse( validationFailures.isEmpty() );
    }
}
