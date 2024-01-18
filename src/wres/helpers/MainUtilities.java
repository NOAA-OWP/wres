package wres.helpers;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.ExecutionResult;
import wres.Functions;

/**
 * Utilities class to enable a standalone deploy to interface with the Function.java class
 */
public final class MainUtilities
{
    /** Logger. */
    private static final Logger LOGGER = LoggerFactory.getLogger( MainUtilities.class );

    // Mapping of String names to corresponding methods
    private static final Map<WresFunction, Function<Functions.SharedResources, ExecutionResult>>
            FUNCTIONS_MAP = MainUtilities.createMap();

    /**
     * Determines if there is a method for the requested operation
     *
     * @param operation The desired operation to perform
     * @return True if there is a method mapped to the operation name
     */
    public static boolean hasOperation( String operation )
    {
        Optional<Entry<WresFunction, Function<Functions.SharedResources, ExecutionResult>>> discovered
                = MainUtilities.getOperation( operation );
        return discovered.isPresent();
    }

    /**
     * Inspects the operation and determines whether it is "simple" and, therefore, involves no spin-up or tear-down.
     * @param operation the operation name
     * @return whether the operation is simple
     */
    public static boolean isSimpleOperation( String operation )
    {
        Optional<Entry<WresFunction, Function<Functions.SharedResources, ExecutionResult>>> discovered
                = MainUtilities.getOperation( operation );
        return discovered.isPresent() && discovered.get()
                                                   .getKey()
                                                   .isSimpleOperation();
    }

    /**
     * Looks for a named operation.
     * @param operation the operation
     * @return a result that may contain the operation
     */
    private static Optional<Entry<WresFunction, Function<Functions.SharedResources, ExecutionResult>>> getOperation( String operation )
    {
        Objects.requireNonNull( operation );
        String finalOperation = operation.toLowerCase();
        return FUNCTIONS_MAP.entrySet()
                            .stream()
                            .filter( next -> finalOperation.equals( next.getKey().shortName() )
                                             || finalOperation.equals( next.getKey().longName() ) )
                            .findFirst();
    }

    /**
     * Executes the operation.
     *
     * @param operation The name of the desired method to call
     * @param sharedResources The resources required, including args.
     */
    public static ExecutionResult call( String operation, Functions.SharedResources sharedResources )
    {
        // Execution began
        Instant beganExecution = Instant.now();
        // Log the operation
        if ( LOGGER.isInfoEnabled() )
        {
            StringJoiner joiner = new StringJoiner( " " );
            if ( Objects.nonNull( sharedResources ) )
            {
                if ( !sharedResources.arguments()
                                     .contains( operation ) )
                {
                    joiner.add( operation );
                }
                sharedResources.arguments()
                               .forEach( joiner::add );
            }
            String report = MainUtilities.curtail( joiner.toString() );
            LOGGER.info( "Executing: {}", report );
        }
        Optional<Entry<WresFunction, Function<Functions.SharedResources, ExecutionResult>>> discovered
                = MainUtilities.getOperation( operation );
        ExecutionResult result;
        if ( discovered.isPresent() )
        {
            result = discovered.get()
                               .getValue()
                               .apply( sharedResources );
        }
        else
        {
            result = ExecutionResult.failure( new UnsupportedOperationException( "Cannot find operation "
                                                                                 + operation ),
                                              false );
        }
        // Log timing of execution
        if ( LOGGER.isInfoEnabled() )
        {
            Instant endedExecution = Instant.now();
            Duration duration = Duration.between( beganExecution, endedExecution );
            LOGGER.info( "The function '{}' took {}", operation, duration );
        }
        return result;
    }

    /**
     * Returns only the first line of the input string, appending "..." if more than one line was found.
     * @param input the input string
     * @return the first line
     */
    public static String curtail( String input )
    {
        Objects.requireNonNull( input );
        // Only report the first line of a declaration string
        String[] split = input.split( "\\r?\\n|\\r" );
        String report = split[0];
        if ( split.length > 1 )
        {
            report = report + "...";
        }
        return report;
    }

    /**
      * Creates the "print_commands" method
      *
      * @return Method that prints all available commands by name
      */
    public static ExecutionResult printCommands( final Functions.SharedResources sharedResources )
    {
        LOGGER.info( "\tAvailable functions:" );
        for ( final WresFunction command : FUNCTIONS_MAP.keySet() )
        {
            LOGGER.info( "\t{}", command );
        }

        return ExecutionResult.success();
    }

    /**
     * Creates the mapping of operation names to their corresponding methods
     */
    private static Map<WresFunction, Function<Functions.SharedResources, ExecutionResult>> createMap()
    {
        // Map with insertion order
        Map<WresFunction, Function<Functions.SharedResources, ExecutionResult>> functions = new LinkedHashMap<>();
        functions.put( new WresFunction( "-c", "cleandatabase", "Cleans the WRES database.", false ),
                       Functions::cleanDatabase );
        functions.put( new WresFunction( "-cd", "connecttodb", "Connects to the WRES "
                                                               + "database.", false ),
                       Functions::connectToDatabase );
        functions.put( new WresFunction( "-co", "commands", "Prints this help information.", true ),
                       MainUtilities::printCommands );
        functions.put( new WresFunction( "-cn",
                                         "createnetcdftemplate",
                                         "Creates a Netcdf template with the supplied Netcdf source. Example "
                                         + "usage: createnetcdftemplate /foo/bar/source.nc", true ),
                       Functions::createNetCDFTemplate );
        functions.put( new WresFunction( "-e",
                                         "execute",
                                         "Executes an evaluation with the declaration supplied as a path or "
                                         + "string. Example usage: execute /foo/bar/evaluation.yml", false ),
                       Functions::evaluate );
        functions.put( new WresFunction( "-h", "help", "Prints this help information.", true ),
                       MainUtilities::printCommands );
        functions.put( new WresFunction( "-i",
                                         "ingest",
                                         "Ingests data supplied in a declaration path or string. Example "
                                         + "usage: ingest /foo/bar/evaluation.yml", false ),
                       Functions::ingest );
        functions.put( new WresFunction( "-m",
                                         "migrate",
                                         "Migrates a project declaration from XML (old-style) to YAML "
                                         + "(new style). Example usage: migrate /foo/bar/evaluation.yml", true ),
                       Functions::migrate );
        functions.put( new WresFunction( "-md", "migratedatabase", "Migrate the WRES database.", false ),
                       Functions::migrateDatabase );
        functions.put( new WresFunction( "-mi",
                                         "migrateinline",
                                         "Migrates a project declaration from XML (old-style) to YAML "
                                         + "(new style). In addition, if the declaration references any external "
                                         + "sources of CSV thresholds, these will be migrated inline to the "
                                         + "declaration. Example usage: migrateinline /foo/bar/evaluation.yml",
                                         true ),
                       Functions::migrateInline );
        functions.put( new WresFunction( "-r", "refreshdatabase", "Refreshes the database.", false ),
                       Functions::refreshDatabase );
        functions.put( new WresFunction( "-s", "server", "Starts a long-running worker server. Visit localhost/evaluation for further help.", true ),
                       Functions::startServer );
        functions.put( new WresFunction( "-v",
                                         "validate",
                                         "Validates the declaration supplied as a path or string. Example "
                                         + "usage: validate /foo/bar/evaluation.yml", true ),
                       Functions::validate );
        functions.put( new WresFunction( "-vg",
                                         "validategrid",
                                         "Validates a netcdf grid supplied as a path and a corresponding variable "
                                         + "name. Example usage: validategrid /foo/bar/grid.nc baz", true ),
                       Functions::validateNetcdfGrid );
        return functions;
    }

    /**
     * Small wrapper for WRES functions.
     * @param shortName the short name
     * @param longName the long name
     * @param description the description
     * @param isSimpleOperation whether the operation is "simple" and does not, therefore, require spin-up or tear-down
     */
    private record WresFunction( String shortName,
                                 String longName,
                                 String description,
                                 boolean isSimpleOperation )
    {
        @Override
        public String toString()
        {
            String returnMe = "";
            if ( Objects.nonNull( this.shortName() ) )
            {
                returnMe = this.shortName();
                if ( Objects.isNull( this.longName() ) )
                {
                    returnMe = StringUtils.rightPad( returnMe, 30 );
                }
                else
                {
                    returnMe = returnMe + ", ";
                }
            }
            if ( Objects.nonNull( this.longName() ) )
            {
                returnMe = returnMe + this.longName();
                returnMe = StringUtils.rightPad( returnMe, 30 );
            }
            if ( Objects.nonNull( this.description() ) )
            {
                returnMe = returnMe + this.description();
            }
            return returnMe;
        }
    }

    /** Do not construct. */
    private MainUtilities()
    {
    }
}
