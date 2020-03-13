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

delete from wres.Source
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
);

-- Leave orphaned data in TimeSeriesValue to avoid OOMkiller
-- See #75226 and #76229
