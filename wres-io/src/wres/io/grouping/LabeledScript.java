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
    public LabeledScript(String label, String script)
    {
        pair = new DualString( label, script );
    }

    public String getLabel()
    {
        return this.pair.getFirst();
    }

    public String getScript() {
        return this.pair.getSecond();
    }
    
    @Override
    public String toString()
    {
        return "Label: " + String.valueOf(this.getLabel()) + System.lineSeparator() +
               "Script:" + System.lineSeparator() +
               String.valueOf(this.getScript()) + System.lineSeparator();
    }
    
    @Override
    public int hashCode()
    {
        return pair.hashCode();
    }

    @Override
    public boolean equals( Object obj )
    {
        return !Objects.isNull( obj ) && obj.hashCode() == this.hashCode();
    }

    @Override
    public int compareTo(final LabeledScript other)
    {
        int equality = 1;

        if (other != null)
        {
            equality = this.pair.compareTo( other.pair );
        }

        return equality;
    }

    private final DualString pair;
}
