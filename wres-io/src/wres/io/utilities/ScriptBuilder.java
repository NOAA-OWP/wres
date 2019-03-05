package wres.io.utilities;

public class ScriptBuilder
{
    protected static final String NEWLINE = System.lineSeparator();
    private final StringBuilder script;

    public ScriptBuilder()
    {
        this.script = new StringBuilder(  );
    }

    public ScriptBuilder (String beginning)
    {
        this.script = new StringBuilder( beginning );
    }

    /**
     * Adds a collection of objects to the script
     * @param details a collection of objects whose string representations will
     *                be added to the script
     * @return The updated ScriptBuilder
     */
    public ScriptBuilder add(Object... details)
    {
        for (Object detail : details)
        {
            this.script.append(detail);
        }

        return this;
    }

    /**
     * Ends the current line of the script
     * @return The updated ScriptBuilder
     */
    public ScriptBuilder addLine()
    {
        return this.add(NEWLINE);
    }

    /**
     * Adds a collection of objects to the scripts and ends the line
     * @param details A collection of objects whose string representations will
     *                be added to the script
     * @return The updated ScriptBuilder
     */
    public ScriptBuilder addLine(Object... details)
    {
        return this.add(details).addLine();
    }

    /**
     * Adds the specified number of tabs greater than 0 to the script
     *  <p>
     *      One tab is equivalent to four whitespace characters
     *  </p>
     * @param numberOfTabs The number of tabs to add to the script. If the
     *                     number is less than one, no tabs will be added.
     * @return The updated ScriptBuilder
     */
    public ScriptBuilder addTab(int numberOfTabs)
    {
        for ( int i = 0; i < numberOfTabs; i++ )
        {
            this.add("    ");
        }

        return this;
    }

    /**
     * Adds a single tab to the script
     *  <p>
     *      One tab is equivalent to four whitespace characters
     *  </p>
     * @return The updated ScriptBuilder
     */
    public ScriptBuilder addTab()
    {
        return addTab(1);
    }

    @Override
    public String toString()
    {
        return this.script.toString();
    }

}
