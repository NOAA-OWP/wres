-- First delete associated project and source rows (and cascade).

delete from wres.Project
where project_id in
(
    select project_id
    from wres.ProjectSource
    where source_id in
    (
        select distinct( source_id )
        from wres.Observation
        where variablefeature_id in
        (
            select distinct( variablefeature_id )
            from wres.VariableFeature
            where feature_id in
            (
                select feature_id
                from wres.Feature
                where lid in ( 'HDGA4', 'LSNO2' )
            )
        )
        union
        select distinct( source_id )
        from wres.TimeSeriesSource
        where timeseries_id in
        (
            select timeseries_id
            from wres.TimeSeries
            where variablefeature_id in
            (
                select variablefeature_id
                from wres.VariableFeature
                where feature_id in
                (
                    select feature_id
                    from wres.Feature
                    where lid in ( 'HDGA4', 'LSNO2' )
                )
            )
        )
    )
);

delete
from wres.Source
where source_id in
(
    select distinct( source_id )
    from wres.Observation
    where variablefeature_id in
    (
        select distinct( variablefeature_id )
        from wres.VariableFeature
        where feature_id in
        (
            select feature_id
            from wres.Feature
            where lid in ( 'HDGA4', 'LSNO2' )
        )
    )
    union
    select distinct( source_id )
    from wres.TimeSeriesSource
    where timeseries_id in
    (
        select timeseries_id
        from wres.TimeSeries
        where variablefeature_id in
        (
            select variablefeature_id
            from wres.VariableFeature
            where feature_id in
            (
                select feature_id
                from wres.Feature
                where lid in ( 'HDGA4', 'LSNO2' )
            )
        )
    )
);

-- Second, delete the feature rows (and cascade).
delete from wres.Feature
where lid in ( 'HDGA4', 'LSNO2' );


-- Third: create new feature rows.
insert into wres.Feature
( comid, lid, gage_id, region, state, huc, feature_name, latitude, longitude )
values
( 21773045, 'LSNO2', '07190400', 'ABRFC', 'OK', '11070209', 'Neosho River (spillway) near Langley, OK', 36.4375, -95.0455555555555555555556 ),
( 8588002, 'HDGA4', '07049000', 'LMRFC', 'AR', '11010001', 'War Eagle Creek near Hindsville, AR', 36.2, -93.855 );


-- Fourth: clean up orphaned data from removed sources
delete from wres.TimeSeries
where timeseries_id not in
(
    select timeseries_id
    from wres.TimeSeriesSource
);

delete from wres.TimeSeriesValue
where timeseries_id not in
(
    select timeseries_id
    from wres.TimeSeries
);
