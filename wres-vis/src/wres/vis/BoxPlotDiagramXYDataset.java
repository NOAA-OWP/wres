package wres.vis;

import org.jfree.data.xy.XYDataset;

import wres.datamodel.BoxPlotOutput;

/**
 * The {@link XYDataset} for use in building the reliability diagram portion of the reliability diagram plot (the other
 * being the sample size portion).
 * 
 * @author Hank.Herr
 */
public class BoxPlotDiagramXYDataset extends
        WRESAbstractXYDataset<BoxPlotOutput, BoxPlotOutput>
{
    private static final long serialVersionUID = 4254109136599641286L;

    public BoxPlotDiagramXYDataset( final BoxPlotOutput input)
    {
        super( input );
    }

    @Override
    protected void preparePlotData( BoxPlotOutput rawData )
    {
        //This check should not be necessary, since the conditions should be impossible.  I'll do it anyway just to be sure.
        if (rawData.getData().isEmpty())
        {
            throw new IllegalStateException( "The box-plot data provided is empty." );
        }

        //Check the series counts.
        setPlotData( rawData );
    }

    @Override
    public int getItemCount( final int series )
    {
        return getPlotData().getData().size();
    }

    @Override
    public Number getX( final int series, final int item )
    {
        return getPlotData().getData().get( item ).getItemOne();
    }

    @Override
    public Number getY( final int series, final int item )
    {
        return getPlotData().getData().get( item ).getItemTwo()[series];
    }

    @Override
    public int getSeriesCount()
    {
        //The prepare method will fail if the data is empty.  So there must be at least one item; hence hard coded 0.
        return getPlotData().getData().get( 0 ).getItemTwo().length;
    }

    @Override
    public Comparable<String> getSeriesKey( final int series )
    {
        return "Probability " + getPlotData().getProbabilities().getDoubles()[series];
    }

}
