package wres.config.xml;

import java.util.Objects;

import com.sun.xml.bind.Locatable;

/**
 * <p>When any part of the system cannot proceed with processing due to incomplete,
 * missing, ambiguous, or otherwise problematic configuration, it is appropriate
 * to throw this exception.
 *
 * <p>The throwing code should include a specific message, appropriate to display
 * to a user, at least explaining why the software could not proceed, or at best
 * explaining the steps the user could take to solve the issue.
 *
 * <p>Additionally, the throwing code must include an element where the issue was
 * detected. The purpose of this is so that the location can be included in the
 * message.
 *
 * <p>The thrower does not need to print the location in the message, it is
 * inserted automatically at the beginning of the exception message.
 * @deprecated
 */
@Deprecated( since = "6.14", forRemoval = true )
public class ProjectConfigException extends RuntimeException
{
    /**
     * Creates an instance
     * @param problemElement the problem element
     * @param message the message
     * @param cause the cause
     */
    public ProjectConfigException( final Locatable problemElement,
                                   final String message,
                                   final Throwable cause )
    {
        super( ProjectConfigException.getMessagePrefix( problemElement ) + message, cause );
    }

    /**
     * Creates an instance
     * @param problemElement the problem element
     * @param message the message
     */
    public ProjectConfigException( final Locatable problemElement,
                                   final String message )
    {
        super( ProjectConfigException.getMessagePrefix( problemElement ) + message );
    }

    /**
     * Creates a message prefix.
     * @param problemElement the problem element
     * @return the message prefix
     */
    private static String getMessagePrefix( final Locatable problemElement )
    {
        // If there is no sourceLocation available, use an empty prefix.
        if ( Objects.isNull( problemElement ) || problemElement.sourceLocation() == null )
        {
            return "";
        }
        return "Near line " + problemElement.sourceLocation().getLineNumber()
               + ", column " + problemElement.sourceLocation().getColumnNumber()
               + ": ";
    }
}
