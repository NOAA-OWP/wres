package wres.config;

import com.sun.xml.bind.Locatable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wres.config.generated.DestinationConfig;
import wres.config.generated.ProjectConfig;
import wres.io.config.ProjectConfigPlus;

import javax.xml.bind.ValidationEvent;
import java.util.List;


/**
 * Helps validate project configurations at a higher level than parser, with
 * detailed messaging.
 */

public class Validation
{

    private static final Logger LOGGER = LoggerFactory.getLogger( Validation.class );

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
                    LOGGER.warn( "In file {}, near line {} and column {}, WRES "
                                 + "found an issue with the project "
                                 + "configuration. The parser said:",
                                 projectConfigPlus.getPath(),
                                 ve.getLocator().getLineNumber(),
                                 ve.getLocator().getColumnNumber(),
                                 ve.getLinkedException() );
                }
                else
                {
                    LOGGER.warn( "In file {}, WRES found an issue with the "
                                 + "project configuration. The parser said:",
                                 projectConfigPlus.getPath(),
                                 ve.getLinkedException() );
                }
            }

            // Any validation event means we fail.
            result = false;
        }

        // Validate graphics portion
        result = result && isGraphicsPortionOfProjectValid( projectConfigPlus );

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
        final String BEGIN_TAG = "<chartDrawingParameters>";
        final String END_TAG = "</chartDrawingParameters>";
        final String BEGIN_COMMENT = "<!--";
        final String END_COMMENT = "-->";

        boolean result = true;

        ProjectConfig projectConfig = projectConfigPlus.getProjectConfig();

        for ( DestinationConfig d: projectConfig.getOutputs()
                                                .getDestination() )
        {
            String customString = projectConfigPlus.getGraphicsStrings()
                                                   .get( d );

            if ( customString != null )
            {
                // For to give a helpful message, find closeby tag without NPE
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

                // If a custom vis config was provided, make sure string either
                // starts with the correct tag or starts with a comment.
                String trimmedCustomString = customString.trim();
                result = result && checkTrimmedString( projectConfigPlus,
                                                       trimmedCustomString,
                                                       BEGIN_TAG,
                                                       BEGIN_COMMENT,
                                                       nearbyTag );
                result = result && checkTrimmedString( projectConfigPlus,
                                                       trimmedCustomString,
                                                       END_TAG,
                                                       END_COMMENT,
                                                       nearbyTag );
            }
        }

        return result;
    }


    /**
     * Checks a trimmed string in the graphics configuration.
     *
     * @param projectConfigPlus the configuration
     * @param trimmedCustomString the trimmed string
     * @param tag the tag
     * @param comment the comment
     * @param nearbyTag a nearby tag
     * @return true if the tag is valid, false otherwise
     */

    private static boolean checkTrimmedString( ProjectConfigPlus projectConfigPlus,
                                               String trimmedCustomString,
                                               String tag,
                                               String comment,
                                               Locatable nearbyTag )
    {
        if( !trimmedCustomString.endsWith( tag )
            && !trimmedCustomString.endsWith( comment ) )
        {
            final String msg = "In file {}, near line {} and column {}, "
                               + "WRES found an issue with the project "
                               + " configuration in the area of custom "
                               + "graphics configuration. If customization is "
                               + "provided, please end it with " + tag;

            LOGGER.warn( msg,
                         projectConfigPlus.getPath(),
                         nearbyTag.sourceLocation().getLineNumber(),
                         nearbyTag.sourceLocation().getColumnNumber() );

            return false;
        }

        return true;
    }

}
