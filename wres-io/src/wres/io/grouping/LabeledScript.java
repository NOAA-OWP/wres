package wres.io.grouping;

import java.util.Objects;

/**
 * @author Christopher Tubbs
 *
 */
public final class LabeledScript implements Comparable<LabeledScript>
{
    /**
     * Creates the immutable pair of two values
     * @param label the label
     * @param script the script
     */
    public LabeledScript(String label, String script) {
        this.label = label;
        this.script = script;
    }

    public String getLabel()
    {
        return this.label;
    }

    public String getScript() {
        return this.script;
    }
    
    @Override
    public String toString()
    {
        return "Label: " + this.label + System.lineSeparator() +
               "Script:" + System.lineSeparator() +
               this.script + System.lineSeparator();
    }
    
    @Override
    public int hashCode()
    {
        return Objects.hash(this.label, this.script);
    }

    @Override
    public int compareTo(final LabeledScript other)
    {
        int equality = this.label.compareTo(other.label);
        if (equality == 0)
        {
            equality = this.script.compareTo(other.script);
        }

        return equality;
    }
    
    private final String script;
    private final String label;
}
