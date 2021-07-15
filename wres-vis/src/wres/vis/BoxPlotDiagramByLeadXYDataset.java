package wres.vis;

import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;

import org.jfree.data.xy.XYDataset;

import wres.datamodel.statistics.BoxplotStatisticOuter;

/**
 * The {@link XYDataset} for use in building a box plot by lead duration.
 */
class BoxPlotDiagramByLeadXYDataset extends BoxPlotDiagramXYDataset
{

    private static final long serialVersionUID = 2818742289181492762L;

    /**
     * The duration units.
     */

    private final ChronoUnit durationUnits;

    /**
     * Build the data type with an input source and the type of duration units.
     * 
     * @param input the input data
     * @param durationUnits the duration units
     * @throws NullPointerException if either input is null
     */

    BoxPlotDiagramByLeadXYDataset( List<BoxplotStatisticOuter> input, ChronoUnit durationUnits )
    {
        super( input );

        Objects.requireNonNull( durationUnits );

        this.durationUnits = durationUnits;
    }

    @Override
    public Number getX( final int series, final int item )
    {
        // Time in prescribed units for the specified item number of the single dataset        
        return GraphicsUtils.durationToLongUnits( this.getPlotData()
                                                  .get( item )
                                                  .getMetadata()
                                                  .getTimeWindow()
                                                  .getLatestLeadDuration(),
                                              this.durationUnits );
    }

}
