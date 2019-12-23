-- First delete associated project and source rows (and cascade).

delete from wres.project
where project_id in
(
    select project_id
    from wres.projectsource
    where source_id in
    (
        select distinct( source_id )
        from wres.observation
        where variablefeature_id in
        (
            select distinct( variablefeature_id )
            from wres.variablefeature
            where feature_id in
            (
                select feature_id
                from wres.feature
                where lid in ( 'HDGA4', 'LSNO2' )
            )
        )
        union
        select distinct( source_id )
        from wres.timeseriessource
        where timeseries_id in
        (
            select timeseries_id
            from wres.timeseries
            where variablefeature_id in
            (
                select variablefeature_id
                from wres.variablefeature
                where feature_id in
                (
                    select feature_id
                    from wres.feature
                    where lid in ( 'HDGA4', 'LSNO2' )
                )
            )
        )
    )
);

delete
from wres.source
where source_id in
(
    select distinct( source_id )
    from wres.observation
    where variablefeature_id in
    (
        select distinct( variablefeature_id )
        from wres.variablefeature
        where feature_id in
        (
            select feature_id
            from wres.feature
            where lid in ( 'HDGA4', 'LSNO2' )
        )
    )
    union
    select distinct( source_id )
    from wres.timeseriessource
    where timeseries_id in
    (
        select timeseries_id
        from wres.timeseries
        where variablefeature_id in
        (
            select variablefeature_id
            from wres.variablefeature
            where feature_id in
            (
                select feature_id
                from wres.feature
                where lid in ( 'HDGA4', 'LSNO2' )
            )
        )
    )
);

-- Second, delete the feature rows (and cascade).
delete from wres.feature
where feature_id in
(
    select feature_id
    from wres.feature
    where lid in ( 'HDGA4', 'LSNO2' )
);

-- Third: create new feature rows.
insert into wres.Feature
( comid, lid, gage_id, region, state, huc, feature_name, latitude, longitude )
values
( 21773045, 'LSNO2', '07190400', 'ABRFC', 'OK', '11070209', 'Neosho River (spillway) near Langley, OK', 36.4375, -95.0455555555555555555556 ),
( 8588002, 'HDGA4', '07049000', 'LMRFC', 'AR', '11010001', 'War Eagle Creek near Hindsville, AR', 36.2, -93.855 );
