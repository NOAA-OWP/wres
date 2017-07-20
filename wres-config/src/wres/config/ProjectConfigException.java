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
 * detected. The purpose of this is so that the code receiving/catching this
 * exception can add to the message the estimated location in the config file.
 *
 * The thrower does not need to include the location in the config file,it is
 * expected that the catcher will display the location information by using the
 * first param. All the elements in the generated code should implement
 * Locatable, so the best guess of which element caused the issue is acceptable.
 */
public class ProjectConfigException extends Exception
{
    private final Locatable problemElement;

    public ProjectConfigException(final Locatable problemElement,
            final String s,
            final Throwable t)
    {
        super(s, t);
        this.problemElement = problemElement;
    }

    public ProjectConfigException(final Locatable problemElement,
            final String s)
    {
        super(s);
        this.problemElement = problemElement;
    }
    public Locatable getProblemElement()
    {
        return problemElement;
    }
}
