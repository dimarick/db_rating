delete from player where id in (
    '69a0e085-7a70-4d61-9dfd-d57c37830da2',
    '3d3d6f44-f72b-415f-a290-18932d8475e3',
    '22990839-9bc1-4bdc-8e93-a288aad08526',
    '6fddaed5-95ab-4b91-8ec5-916381bde3f7',
    'b6dad0c4-bb83-45f7-877e-d168acc0156e',
    '1ac51874-d8df-4623-a987-a727a447dcfd'
);

insert into player (id, country, username, email, registered_at, disabled)
values
    ('69a0e085-7a70-4d61-9dfd-d57c37830da2', 'RU', 'test1', 'test1@example.com', '2024-08-20 20:11:00.124153', false),
    ('3d3d6f44-f72b-415f-a290-18932d8475e3', 'RU', 'test2', 'test2@example.com', '2024-08-20 20:11:00.124153', true),
    ('22990839-9bc1-4bdc-8e93-a288aad08526', 'RU', 'test3', null, '2024-08-20 20:11:00.124153', false),
    ('6fddaed5-95ab-4b91-8ec5-916381bde3f7', 'RU', null, 'test2@example.com', '2024-08-20 20:11:00.124153', false),
    ('b6dad0c4-bb83-45f7-877e-d168acc0156e', 'RU', null, null, '2024-08-20 20:11:00.124153', false),
    ('1ac51874-d8df-4623-a987-a727a447dcfd', 'RU', null, null, '2024-08-20 20:11:00.124153', true);

insert into player_session (player_id, mode_id, started_at, finished_at, score, rating_points)
values
    ('69a0e085-7a70-4d61-9dfd-d57c37830da2', 1, '2024-08-21 20:00:00.124153', '2024-08-21 20:11:00.124153', 134, 2),
    ('69a0e085-7a70-4d61-9dfd-d57c37830da2', 1, '2024-08-21 20:01:00.124153', '2024-08-21 20:01:42.124154', 0, 3),
    ('69a0e085-7a70-4d61-9dfd-d57c37830da2', 1, '2024-08-21 21:32:00.124153', '2024-08-21 21:33:01.124153', 152, 5),
    ('69a0e085-7a70-4d61-9dfd-d57c37830da2', 2, '2024-08-21 20:12:00.124153', '2024-08-21 20:13:00.124153', 135, 2),
    ('69a0e085-7a70-4d61-9dfd-d57c37830da2', 2, '2024-08-21 20:02:00.124153', '2024-08-21 20:05:42.124154', 22, 3),
    ('69a0e085-7a70-4d61-9dfd-d57c37830da2', 2, '2024-08-21 21:34:00.124153', '2024-08-21 21:38:01.124153', 153, 5),
    ('3d3d6f44-f72b-415f-a290-18932d8475e3', 1, '2024-08-21 20:00:00.124153', '2024-08-21 20:11:00.124153', 0, 0),
    ('3d3d6f44-f72b-415f-a290-18932d8475e3', 1, '2024-08-21 20:01:00.124153', '2024-08-21 20:01:42.124154', 42, 3),
    ('3d3d6f44-f72b-415f-a290-18932d8475e3', 1, '2024-08-21 21:32:00.124153', '2024-08-21 21:33:01.124153', 0, 0),
    ('b6dad0c4-bb83-45f7-877e-d168acc0156e', 3, '2024-08-22 21:32:00.124153', '2024-08-22 21:33:02.124153', 0, 0),
    ('b6dad0c4-bb83-45f7-877e-d168acc0156e', 3, '2024-08-22 21:32:00.124153', '2024-08-22 21:34:01.134153', 1, 1),
    ('b6dad0c4-bb83-45f7-877e-d168acc0156e', 3, '2024-08-22 21:32:00.124153', '2024-08-22 22:33:01.124153', 3, 1),
    ('b6dad0c4-bb83-45f7-877e-d168acc0156e', 3, '2024-08-22 21:32:00.124153', '2024-08-23 21:33:01.124163', 5, 2)
;

do $$
declare
    max_score int;
    sum_score int;
    min_duration double precision;
    sum_rating_points int;
    count int;
    count_win int;
    last_activity_at timestamp;
    disabled bool;
begin
    select m.max_score, m.sum_score, m.min_duration, m.sum_rating_points, m.count, m.count_win, m.last_activity_at, m.disabled
    into max_score, sum_score, min_duration, sum_rating_points, count, count_win, last_activity_at, disabled
    from player_metric m where player_id = '69a0e085-7a70-4d61-9dfd-d57c37830da2' and mode_id = 1;

    raise notice 'Case 1: max_score = %, sum_score = %, min_duration = %, sum_rating_points = %, count = %, count_win = %, last_activity_at = %, disabled = %',
        max_score, sum_score, min_duration, sum_rating_points, count, count_win, last_activity_at, disabled;

    assert max_score = 152, 'max_score = 152';
    assert sum_score = 152 + 134, 'sum_score = 152 + 134';
    assert min_duration = 42.000001, 'min_duration = 42.000001';
    assert sum_rating_points = 2 + 3 + 5, 'sum_rating_points = 2 + 3 + 5';
    assert count = 3, 'count = 0';
    assert count_win = 2, 'count_win = 2';
    assert last_activity_at = '2024-08-21 21:33:01.124153', 'last_activity_at = "2024-08-21 21:33:01.124153"';
    assert disabled = false, 'disabled = false';

    select m.max_score, m.sum_score, m.min_duration, m.sum_rating_points, m.count, m.count_win, m.last_activity_at, m.disabled
    into max_score, sum_score, min_duration, sum_rating_points, count, count_win, last_activity_at, disabled
    from player_metric m where player_id = '69a0e085-7a70-4d61-9dfd-d57c37830da2' and mode_id = 2;

    raise notice 'Case 2: max_score = %, sum_score = %, min_duration = %, sum_rating_points = %, count = %, count_win = %, last_activity_at = %, disabled = %',
        max_score, sum_score, min_duration, sum_rating_points, count, count_win, last_activity_at, disabled;

    assert max_score = 153, 'max_score = 153';
    assert sum_score = 153 + 135 + 22, 'sum_score = 153 + 135 + 22';
    assert min_duration = 60, 'min_duration = 60';
    assert sum_rating_points = 2 + 3 + 5, 'sum_rating_points = 2 + 3 + 5';
    assert count = 3, 'count = 152 + 21 + 134';
    assert count_win = 3, 'count_win = 152 + 21 + 134';
    assert last_activity_at = '2024-08-21 21:38:01.124153', 'last_activity_at = "2024-08-21 21:38:01.124153"';
    assert disabled = false, 'disabled = false';

    select m.max_score, m.sum_score, m.min_duration, m.sum_rating_points, m.count, m.count_win, m.last_activity_at, m.disabled
    into max_score, sum_score, min_duration, sum_rating_points, count, count_win, last_activity_at, disabled
    from player_metric m where player_id = '3d3d6f44-f72b-415f-a290-18932d8475e3' and mode_id = 1;

    raise notice 'Case 3: max_score = %, sum_score = %, min_duration = %, sum_rating_points = %, count = %, count_win = %, last_activity_at = %, disabled = %',
        max_score, sum_score, min_duration, sum_rating_points, count, count_win, last_activity_at, disabled;

    assert max_score = 42, 'max_score = 42';
    assert sum_score = 42, 'sum_score = 42';
    assert min_duration = 42.000001, 'min_duration = 42.000001';
    assert sum_rating_points = 3, 'sum_rating_points = 3';
    assert count = 3, 'count = 3';
    assert count_win = 1, 'count_win = 1';
    assert last_activity_at = '2024-08-21 21:33:01.124153', 'last_activity_at = "2024-08-21 21:33:01.124153"';
    assert disabled = true, 'disabled = true';
end
$$;

delete from player_session where player_id in ('69a0e085-7a70-4d61-9dfd-d57c37830da2', '3d3d6f44-f72b-415f-a290-18932d8475e3');

do $$
declare
    count int;
begin
    select count(*) into count
    from player_metric m where player_id in ('69a0e085-7a70-4d61-9dfd-d57c37830da2', '3d3d6f44-f72b-415f-a290-18932d8475e3');

    raise notice 'Case 4: count = %', count;

    assert count = 0, 'count = 0';
end
$$;

update player_session set score = 11, rating_points = rating_points + 1 where player_id = 'b6dad0c4-bb83-45f7-877e-d168acc0156e' and finished_at = '2024-08-22 22:33:01.124153';

do $$
declare
    max_score int;
    sum_score int;
    min_duration double precision;
    sum_rating_points int;
    count int;
    count_win int;
    last_activity_at timestamp;
    disabled bool;
begin
    select m.max_score, m.sum_score, m.min_duration, m.sum_rating_points, m.count, m.count_win, m.last_activity_at, m.disabled
    into max_score, sum_score, min_duration, sum_rating_points, count, count_win, last_activity_at, disabled
    from player_metric m where player_id = 'b6dad0c4-bb83-45f7-877e-d168acc0156e' and mode_id = 3;

    raise notice 'Case 5: max_score = %, sum_score = %, min_duration = %, sum_rating_points = %, count = %, count_win = %, last_activity_at = %, disabled = %',
        max_score, sum_score, min_duration, sum_rating_points, count, count_win, last_activity_at, disabled;

    assert max_score = 11, 'max_score = 11';
    assert sum_score = 1 + 11 + 5, 'sum_score = 1 + 11 + 5';
    assert min_duration = 62, 'min_duration = 62';
    assert sum_rating_points = 1 + 2 + 2, 'sum_rating_points = 1 + 2 + 2';
    assert count = 4, 'count = 4';
    assert count_win = 3, 'count_win = 3';
    assert last_activity_at = '2024-08-23 21:33:01.124163', 'last_activity_at = "2024-08-23 21:33:01.124163"';
    assert disabled = false, 'disabled = false';
end
$$;

update player set disabled = true where id in ('b6dad0c4-bb83-45f7-877e-d168acc0156e') ;

do $$
declare
    max_score int;
    sum_score int;
    min_duration double precision;
    sum_rating_points int;
    count int;
    count_win int;
    last_activity_at timestamp;
    disabled bool;
begin
    select m.max_score, m.sum_score, m.min_duration, m.sum_rating_points, m.count, m.count_win, m.last_activity_at, m.disabled
    into max_score, sum_score, min_duration, sum_rating_points, count, count_win, last_activity_at, disabled
    from player_metric m where player_id = 'b6dad0c4-bb83-45f7-877e-d168acc0156e' and mode_id = 3;

    raise notice 'Case 6: max_score = %, sum_score = %, min_duration = %, sum_rating_points = %, count = %, count_win = %, last_activity_at = %, disabled = %',
        max_score, sum_score, min_duration, sum_rating_points, count, count_win, last_activity_at, disabled;

    assert max_score = 11, 'max_score = 11';
    assert sum_score = 1 + 11 + 5, 'sum_score = 1 + 11 + 5';
    assert min_duration = 62, 'min_duration = 62';
    assert sum_rating_points = 1 + 2 + 2, 'sum_rating_points = 1 + 2 + 2';
    assert count = 4, 'count = 4';
    assert count_win = 3, 'count_win = 3';
    assert last_activity_at = '2024-08-23 21:33:01.124163', 'last_activity_at = "2024-08-23 21:33:01.124163"';
    assert disabled = true, 'disabled = true';
end
$$;

delete from player where id in ('b6dad0c4-bb83-45f7-877e-d168acc0156e') ;

do $$
declare
    count int;
begin
    select count(*) into count
    from player_metric m where player_id in ('b6dad0c4-bb83-45f7-877e-d168acc0156e');

    raise notice 'Case 7: count = %', count;

    assert count = 0, 'count = 0';
end
$$;
