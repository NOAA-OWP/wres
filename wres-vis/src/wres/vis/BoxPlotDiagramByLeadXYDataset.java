package wres.vis;

import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.OptionalInt;

import org.jfree.data.xy.XYDataset;

import wres.datamodel.statistics.BoxPlotStatistics;
import wres.util.TimeHelper;

/**
 * The {@link XYDataset} for use in building a box plot by lead duration.
 */
public class BoxPlotDiagramByLeadXYDataset extends BoxPlotDiagramXYDataset
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
    
    public BoxPlotDiagramByLeadXYDataset( BoxPlotStatistics input, ChronoUnit durationUnits )
    {
        super( input );
        
        Objects.requireNonNull( durationUnits );

        this.durationUnits = durationUnits;    
    }

    @Override
    public Number getX( final int series, final int item )
    {
        // Time in prescribed units for the specified item number of the single dataset        
        return TimeHelper.durationToLongUnits( this.getPlotData()
                                               .getData()
                                               .get( item )
                                               .getMetadata()
                                               .getSampleMetadata()
                                               .getTimeWindow()
                                               .getLatestLeadDuration(),
                                           this.durationUnits );
    }

}
