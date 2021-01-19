package wres.vis;

import java.util.List;
import java.util.stream.Collectors;

import org.jfree.data.xy.XYDataset;

import wres.datamodel.statistics.BoxplotStatisticOuter;
import wres.statistics.generated.BoxplotStatistic.Box;

/**
 * The {@link XYDataset} for use in building a box plot.
 * 
 * @author Hank.Herr
 */
class BoxPlotDiagramXYDataset extends
        WRESAbstractXYDataset<List<BoxplotStatisticOuter>, List<BoxplotStatisticOuter>>
{
    private static final long serialVersionUID = 4254109136599641286L;

    private final int itemCount;

    private final int seriesCount;

    private final List<Box> boxes;

    BoxPlotDiagramXYDataset( final List<BoxplotStatisticOuter> input )
    {
        super( input );

        this.itemCount = this.getPlotData()
                             .stream()
                             .mapToInt( next -> next.getData().getStatisticsCount() )
                             .sum();

        List<Box> innerBoxes = this.getPlotData()
                                   .get( 0 )
                                   .getData()
                                   .getStatisticsList();

        if ( innerBoxes.isEmpty() )
        {
            throw new IllegalArgumentException( "Cannot write a box plot series with no boxes." );
        }
        
        this.seriesCount = innerBoxes.get( 0 )
                .getQuantilesCount();

        this.boxes = this.getPlotData()
                         .stream()
                         .map( next -> next.getData().getStatisticsList() )
                         .flatMap( List::stream )
                         .collect( Collectors.toList() );
    }

    @Override
    protected void preparePlotData( List<BoxplotStatisticOuter> rawData )
    {
        //Check the series counts.
        setPlotData( rawData );
    }

    @Override
    public int getItemCount( final int series )
    {
        return this.itemCount;
    }

    @Override
    public Number getX( final int series, final int item )
    {
        return this.boxes.get( item )
                         .getLinkedValue();
    }

    @Override
    public Number getY( final int series, final int item )
    {
        return this.boxes.get( item )
                         .getQuantiles( series );
    }

    @Override
    public int getSeriesCount()
    {
        //The prepare method will fail if the data is empty.  So there must be at least one item; hence hard coded 0.
        return this.seriesCount;
    }

    @Override
    public Comparable<String> getSeriesKey( final int series )
    {
        return "Probability " + this.getPlotData()
                                    .get( 0 )
                                    .getData()
                                    .getMetric()
                                    .getQuantiles( series );
    }

}
