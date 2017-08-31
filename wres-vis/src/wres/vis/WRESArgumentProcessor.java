package wres.vis;

import ohd.hseb.hefs.utils.arguments.DefaultArgumentsProcessor;
import ohd.hseb.hefs.utils.plugins.UniqueGenericParameterList;

/**
 * WRES implementation of {@link DefaultArgumentsProcessor}. After constructing, arguments can be populated by calling
 * {@link UniqueGenericParameterList#addParameter(String, String)} on the return value of {@link #getArguments()}.
 * 
 * @author Hank.Herr
 */
public class WRESArgumentProcessor extends DefaultArgumentsProcessor
{
    //TODO This needs to be fleshed out.  Specifically, the constructor should take some kind of input to the charting process that 
    //includes metadata and then map that metadata to arguments included here so that the XML template can use those arguments.
    public WRESArgumentProcessor()
    {
        super();

        //Add arguments here.

        //See the ExporterArgumentsProcessor for how to added functions arguments if needed, here.
    }

    /**
     * Convenience wrapper on {@link UniqueGenericParameterList#addParameter(String, String)} for the return of
     * {@link #getArguments()}.
     * 
     * @param key the argument key
     * @param value the argument value
     */
    public void addArgument(final String key, final String value)
    {
        getArguments().addParameter(key, value);
    }
}
