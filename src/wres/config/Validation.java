package wres.config;

import com.sun.xml.bind.Locatable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import wres.config.generated.DataSourceConfig;
import wres.config.generated.DestinationConfig;
import wres.config.generated.DurationUnit;
import wres.config.generated.PairConfig;
import wres.config.generated.ProjectConfig;
import wres.config.generated.TimeAggregationConfig;
import wres.config.generated.TimeAggregationFunction;
import wres.io.config.ProjectConfigPlus;

import javax.xml.bind.ValidationEvent;
import java.io.File;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;


/**
 * Helps validate project configurations at a higher level than parser, with
 * detailed messaging.
 */

public class Validation
{

    private static final Logger LOGGER = LoggerFactory.getLogger( Validation.class );

    /** A message to display for programmers when null project config occurs */
    private static final String NON_NULL = "The ProjectConfigPlus must not be null";

    /** The warning message boilerplate for logger (includes 3 placeholders) */
    private static final String FILE_LINE_COLUMN_BOILERPLATE =
            "In file {}, near line {} and column {}, WRES found an issue with "
            + "the project configuration.";

    private Validation()
    {
        // prevent construction.
    }


    /**
     * Validates a list of {@link ProjectConfigPlus}. Returns true if the
     * projects validate successfully, false otherwise.
     *
     * @param projectConfiggies a list of project configurations to validate
     * @return true if the projects validate successfully, false otherwise
     */

    public static boolean validateProjects( List<ProjectConfigPlus> projectConfiggies )
    {
        boolean validationsPassed = true;

        // Validate all projects, not stopping until all are done
        for ( ProjectConfigPlus projectConfigPlus: projectConfiggies )
        {
            if ( !isProjectValid( projectConfigPlus ) )
            {
                validationsPassed = false;
            }
        }

        return validationsPassed;
    }


    /**
     * Quick validation of the project configuration, will return detailed
     * information to the user regarding issues about the configuration. Strict
     * for now, i.e. return false even on minor xml problems. Does not return on
     * first issue, tries to inform the user of all issues before returning.
     *
     * @param projectConfigPlus the project configuration
     * @return true if no issues were detected, false otherwise
     */

    private static boolean isProjectValid( ProjectConfigPlus projectConfigPlus )
    {
        // Assume valid until demonstrated otherwise
        boolean result = true;

        for ( ValidationEvent ve: projectConfigPlus.getValidationEvents() )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                if ( ve.getLocator() != null )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + " The parser said: {}",
                                 projectConfigPlus.getPath(),
                                 ve.getLocator().getLineNumber(),
                                 ve.getLocator().getColumnNumber(),
                                 ve.getMessage(),
                                 ve.getLinkedException() );
                }
                else
                {
                    LOGGER.warn( "In file {}, WRES found an issue with the "
                                 + "project configuration. The parser said: {}",
                                 projectConfigPlus.getPath(),
                                 ve.getMessage(),
                                 ve.getLinkedException() );
                }
            }

            // Any validation event means we fail.
            result = false;
        }

        // Validate data sources
        result = Validation.areDataSourceConfigsValid( projectConfigPlus )
                 && result;

        // Validate pair section
        result = Validation.isPairConfigValid( projectConfigPlus ) && result;

        // Validate outputs are writeable directories
        result = Validation.areAllOutputPathsWriteableDirectories( projectConfigPlus )
                 && result;

        // Validate graphics portion
        result = Validation.isGraphicsPortionOfProjectValid( projectConfigPlus )
                 && result;

        return result;
    }


    /**
     * Validates outputs portion of project config have writeable directories.
     *
     * @param projectConfigPlus the project configuration
     * @return true if all have writeable directories, false otherwise
     * @throws NullPointerException when projectConfigPlus is null
     */

    private static boolean areAllOutputPathsWriteableDirectories( ProjectConfigPlus projectConfigPlus )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL);

        boolean result = true;

        // No outputs specified
        if ( projectConfigPlus.getProjectConfig()
                              .getOutputs() == null )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( "In file {}, no output configuration was found.",
                             projectConfigPlus.getPath() );
            }

            return false;
        }

        final String PLEASE_UPDATE = "Please update the project configuration "
                                     + "with an existing writeable directory "
                                     + "or create the directory already "
                                     + "specified.";

        for ( DestinationConfig d : projectConfigPlus.getProjectConfig()
                                                     .getOutputs()
                                                     .getDestination() )
        {
            Path destinationPath;
            try
            {
                destinationPath = Paths.get( d.getPath() );
            }
            catch ( InvalidPathException ipe )
            {
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + " The path {} could not be found. "
                                 + PLEASE_UPDATE,
                                 projectConfigPlus.getPath(),
                                 d.sourceLocation().getLineNumber(),
                                 d.sourceLocation().getColumnNumber(),
                                 d.getPath() );
                }

                result = false;
                continue;
            }

            File destinationFile = destinationPath.toFile();

            if ( !destinationFile.canWrite() || !destinationFile.isDirectory() )
            {
                if ( LOGGER.isWarnEnabled() )
                {
                    LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                                 + " The path {} was not a writeable directory."
                                 + " " + PLEASE_UPDATE,
                                 projectConfigPlus.getPath(),
                                 d.sourceLocation().getLineNumber(),
                                 d.sourceLocation().getColumnNumber(),
                                 d.getPath() );
                }

                result = false;
            }
        }

        return result;
    }

    /**
     * Validates graphics portion, similar to isProjectValid, but targeted.
     *
     * @param projectConfigPlus the project configuration
     * @return true if the graphics configuration is valid, false otherwise
     */

    private static boolean isGraphicsPortionOfProjectValid( ProjectConfigPlus projectConfigPlus )
    {

        boolean result = true;

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        for ( DestinationConfig d: projectConfig.getOutputs()
                                                .getDestination() )
        {
            String customString = projectConfigPlus.getGraphicsStrings()
                                                   .get( d );

            if ( customString != null )
            {
                result = Validation.isCustomGraphicsStringValid( projectConfigPlus,
                                                                 d,
                                                                 customString )
                         && result;
            }
        }

        return result;
    }


    /**
     * Validates a single custom graphics string from a given destination config
     * @param projectConfigPlus the project configuration
     * @param d the destination config we are validating
     * @param customString the non-null string we have already gotten from d
     * @return true if the string is valid, false otherwise
     * @throws NullPointerException when any args are null
     */

    private static boolean isCustomGraphicsStringValid( ProjectConfigPlus projectConfigPlus,
                                                        DestinationConfig d,
                                                        String customString )
    {
        Objects.requireNonNull( projectConfigPlus, NON_NULL);
        Objects.requireNonNull( d, NON_NULL);
        Objects.requireNonNull( customString, NON_NULL);

        boolean result = true;

        final String BEGIN_TAG = "<chartDrawingParameters>";
        final String END_TAG = "</chartDrawingParameters>";
        final String BEGIN_COMMENT = "<!--";
        final String END_COMMENT = "-->";

        // For to give a helpful message, find closeby tag without NPE
        Locatable nearbyTag = Validation.getNearbyTag( d );

        // If a custom vis config was provided, make sure string either
        // starts with the correct tag or starts with a comment.
        String trimmedCustomString = customString.trim();

        if( !trimmedCustomString.startsWith( BEGIN_TAG )
            && !trimmedCustomString.startsWith( BEGIN_COMMENT ) )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " If custom graphics configuration is "
                             + "provided, please start it with "
                             + BEGIN_TAG,
                             projectConfigPlus.getPath(),
                             nearbyTag.sourceLocation().getLineNumber(),
                             nearbyTag.sourceLocation()
                                      .getColumnNumber() );
            }

            result = false;
        }

        if( !trimmedCustomString.endsWith( END_TAG )
            && !trimmedCustomString.endsWith( END_COMMENT ) )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " If custom graphics configuration is "
                             + "provided, please end it with "
                             + END_TAG,
                             projectConfigPlus.getPath(),
                             nearbyTag.sourceLocation().getLineNumber(),
                             nearbyTag.sourceLocation()
                                      .getColumnNumber() );
            }

            result = false;
        }

        return result;
    }

    private static Locatable getNearbyTag( DestinationConfig d )
    {
        Locatable nearbyTag;

        if ( d.getGraphical() != null
             && d.getGraphical().getConfig() != null )
        {
            // Best case
            nearbyTag = d.getGraphical().getConfig();
        }
        else if ( d.getGraphical() != null )
        {
            // Not as targeted but close
            nearbyTag = d.getGraphical();
        }
        else
        {
            // Destination tag.
            nearbyTag = d;
        }

        return nearbyTag;
    }

    private static boolean isPairConfigValid( ProjectConfigPlus projectConfigPlus )
    {
        boolean result = true;

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        PairConfig pairConfig = projectConfig.getPair();

        TimeAggregationConfig aggregationConfig =
                pairConfig.getDesiredTimeAggregation();

        if ( aggregationConfig != null
             && aggregationConfig.getUnit() == DurationUnit.INSTANT )
        {
            if ( LOGGER.isWarnEnabled() )
            {
                String msg = FILE_LINE_COLUMN_BOILERPLATE
                             + " In the pair configuration, the aggregation "
                             + "duration provided for pairing is prescriptive "
                             + "so it cannot be 'instant' it needs to be "
                             + "one of the other time units such as 'hour'.";

                LOGGER.warn( msg,
                             projectConfigPlus.getPath(),
                             aggregationConfig.sourceLocation().getLineNumber(),
                             aggregationConfig.sourceLocation()
                                              .getColumnNumber() );
            }

            result = false;
        }

        return result;
    }

    private static boolean areDataSourceConfigsValid( ProjectConfigPlus projectConfigPlus )
    {
        boolean result = true;

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        DataSourceConfig left = projectConfig.getInputs().getLeft();
        DataSourceConfig right = projectConfig.getInputs().getRight();
        DataSourceConfig baseline = projectConfig.getInputs().getBaseline();

        result = Validation.isDataSourceConfigValid( projectConfigPlus,
                                                     left )
                 && result;

        result = Validation.isDataSourceConfigValid( projectConfigPlus,
                                                     right )
                 && result;

        if ( baseline != null )
        {
            result = Validation.isDataSourceConfigValid( projectConfigPlus,
                                                         baseline )
                     && result;
        }

        return result;
    }

    private static boolean isDataSourceConfigValid( ProjectConfigPlus projectConfigPlus,
                                                    DataSourceConfig dataSourceConfig )
    {
        boolean result = true;

        TimeAggregationConfig timeAggregation = dataSourceConfig.getExistingTimeAggregation();

        if ( timeAggregation != null
             && timeAggregation.getUnit() == DurationUnit.INSTANT )
        {
            boolean instantMakesSense = true;

            if ( timeAggregation.getPeriod() != 1 )
            {
                instantMakesSense = false;
            }

            if ( timeAggregation.getFrequency() != null
                 && timeAggregation.getFrequency() != 1 )
            {
                instantMakesSense = false;
            }

            // The message is the same whether for period or duration
            if ( !instantMakesSense && LOGGER.isWarnEnabled() )
            {
                LOGGER.warn( FILE_LINE_COLUMN_BOILERPLATE
                             + " When using 'instant' duration, the period (and"
                             + " frequency, if specified) must be 1.",
                             projectConfigPlus.getPath(),
                             timeAggregation.sourceLocation().getLineNumber(),
                             timeAggregation.sourceLocation().getColumnNumber() );
            }

            result = instantMakesSense && result;
        }

        return result;
    }
}
