package wres.config;

import java.util.Objects;

import com.sun.xml.bind.Locatable;

/**
 * When any part of the system cannot proceed with processing due to incomplete,
 * missing, ambiguous, or otherwise problematic configuration, it is appropriate
 * to throw this exception.
 *
 * The throwing code should include a specific message, appropriate to display
 * to a user, at least explaining why the software could not proceed, or at best
 * explaining the steps the user could take to solve the issue.
 *
 * Additionally the throwing code must include an element where the issue was
 * detected. The purpose of this is so that the location can be included in the
 * message.
 *
 * The thrower does not need to print the location in the message, it is
 * inserted automatically at the beginning of the exception message.
 */

public class ProjectConfigException extends Exception
{

    public ProjectConfigException(final Locatable problemElement,
            final String s,
            final Throwable t)
    {
        super( getMessagePrefix( problemElement ) + s, t);
    }

    public ProjectConfigException(final Locatable problemElement,
            final String s)
    {
        super( getMessagePrefix( problemElement ) + s);
    }

    private static String getMessagePrefix(final Locatable problemElement)
    {
        // If there is no sourceLocation available, use an empty prefix.
        if ( Objects.isNull( problemElement ) || problemElement.sourceLocation() == null )
        {
            return "";
        }
        return "Near line " + problemElement.sourceLocation().getLineNumber()
               + ", column " + problemElement.sourceLocation().getLineNumber()
               + ": ";
    }
}
