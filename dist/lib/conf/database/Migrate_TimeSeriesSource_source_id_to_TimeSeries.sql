-- First, delete Project, TimeSeries, Source rows (and cascade) where
-- leads are present or where there are multiple sources per timeseries.

delete from wres.Project
where project_id in
(
    select project_id
    from wres.ProjectSource
    where source_id in
    (
        select source_id
        from wres.TimeSeriesSource
        where lead is not null
        union
        select distinct( source_id )
        from wres.TimeSeriesSource
        where timeseries_id in
        (
            select timeseries_id
            from wres.TimeSeriesSource
            group by timeseries_id
            having count( source_id ) > 1
        )
    )
);

delete from wres.TimeSeries
where timeseries_id in
(
    select timeseries_id
    from wres.TimeSeriesSource
    where lead is not null
);

delete from wres.TimeSeries
where timeseries_id in
(
    select timeseries_id
    from wres.TimeSeriesSource
    group by timeseries_id
    having count( source_id ) > 1
);

delete from wres.Source
where source_id not in
(
    select source_id
    from wres.TimeSeriesSource
    union
    select source_id
    from wres.Observation
);

-- Second, update wres.TimeSeries with the one source_id from whence it came.
update wres.TimeSeries ts
set source_id =
(
    select tss.source_id
    from wres.TimeSeriesSource tss
    where ts.timeseries_id = tss.timeseries_id
);

-- The remaining steps are for good measure, to clean up after 20191223 change.

-- Third, remove any remaining wres.TimeSeries rows without a reference at all
-- in wres.TimeSeriesSource
delete from wres.TimeSeries
where timeseries_id not in
(
    select timeseries_id
    from wres.TimeSeriesSource
);

-- Fourth, remove any remaining wres.TimeSeries rows without source_id
delete from wres.TimeSeries
where source_id is null;

-- Fifth, remove any orphaned rows from wres.TimeSeriesValues
delete from wres.TimeSeriesValue
where timeseries_id not in
(
    select timeseries_id
    from wres.TimeSeries
);

-- It should now be safe to drop the wres.TimeSeriesSource table (liquibase).
-- Note that there may be sources remaining with rows in wres.Observation that
-- have no project. A future project may come and use those observations safely.
