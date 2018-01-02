package wres.vis;

import wres.datamodel.outputs.BoxPlotOutput;

/**
 * 
 * @author hank.herr@***REMOVED***
 * @version 0.1
 * @since 0.1
 */
public class BoxPlotDiagramXYChartDataSource
        extends
        WRESXYChartDataSource<BoxPlotOutput>
{
    public BoxPlotDiagramXYChartDataSource( final int orderIndex,
                                            final BoxPlotOutput input )
    {
        super( orderIndex, input, input.getProbabilities().size() );

        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultDomainAxisTitle( input.getDomainAxisDimension()
                                                                                              .toString() + "@inputUnitsLabelSuffix@" );
        getDefaultFullySpecifiedDataSourceDrawingParameters().setDefaultRangeAxisTitle( input.getRangeAxisDimension()
                                                                                             .toString()  + "@outputUnitsLabelSuffix@" );
        WRESTools.applyDefaultJFreeChartColorSequence( getDefaultFullySpecifiedDataSourceDrawingParameters() );
    }

    public BoxPlotDiagramXYChartDataSource( final int orderIndex,
                                            final BoxPlotOutput input,
                                            final int subPlotIndex )
    {
        this( orderIndex, input );
        getDefaultFullySpecifiedDataSourceDrawingParameters().setSubPlotIndex( subPlotIndex );
    }

    @Override
    protected BoxPlotDiagramXYChartDataSource instantiateCopyOfDataSource()
    {
        return new BoxPlotDiagramXYChartDataSource( getDataSourceOrderIndex(),
                                                    getInput() );
    }

    @Override
    protected BoxPlotDiagramXYDataset instantiateXYDataset()
    {
        return new BoxPlotDiagramXYDataset( getInput() );
    }
}
