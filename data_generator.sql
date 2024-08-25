select setseed(0.5);

insert into player (country, username, email, registered_at, disabled)
select
    upper(substring(regexp_replace(md5(random()::text)::text || md5(random()::text)::text, '\d', '', 'g'), 0, 3)) as country,
    case when random() > 0.3 then substring(md5(random()::text)::text, 0, (random() * 5 + 4)::int) end as username,
    case when random() > 0.3 then substring(md5(random()::text)::text, 0, (random() * 5 + 4)::int) || '@' || substring(md5(random()::text)::text, 0, (random() * 5 + 4)::int) || '.com' end as email,
    now() - interval '1 microsecond' * 1e14 * random() as registered_at,
    random() > 0.95 as disabled
-- здесь можно указать количество генерируемых пользователей чтобы проверить работу на разных объемах данных
from generate_series(1, 1e5::int);

insert into player_session (player_id, mode_id, started_at, finished_at, score, rating_points)

select player_id,
       mode_id,
       started_at,
       a.started_at + interval '1 hour' * random() as finished_at,
       case win when true then (random() * 1000)::int else 0 end as score,
       case win when true then (random() * 10)::int else 0 end as rating_points
from (
    select p.id as player_id,
           mode.id as mode_id,
           now() - (now() - p.registered_at) * random() as started_at,
           random() > 0.27 as win
    from (select *, random() as r from player) p
    left join lateral (
        select *, random() as r2 from generate_series(0, 5.17521^(p.r * 2.5)::int)
    ) on true
    left join lateral (
        select id from game_mode where id = (r2 * 4 + 1)::int limit 1
    ) mode on true
) a;

insert into player_session (player_id, mode_id, started_at, finished_at, score, rating_points)

select player_id,
       mode_id,
       started_at,
       a.started_at + interval '1 hour' * random() as finished_at,
       case win when true then (random() * 1000)::int else 0 end as score,
       case win when true then (random() * 10)::int else 0 end as rating_points
from (
    select p.id as player_id,
           mode.id as mode_id,
           now() - (now() - p.registered_at) * random() as started_at,
           random() > 0.27 as win
    from (select *, random() as r from player) p
    left join lateral (
        select *, random() as r2 from generate_series(0, 5.17521^(p.r * 2.5)::int)
    ) on true
    left join lateral (
        select id from game_mode where id = (r2 * 4 + 1)::int limit 1
    ) mode on true
) a;

-- Генерируем достижения
insert into player_achievement (player_session_id, achievement_id, unlocked_at)
select min(first_session) as player_session_id, 1 as achievement_id, min(first_finished_at) as finished_at from (
select player_id, first_value(id) over (partition by player_id order by finished_at) as first_session, first_value(finished_at) over (partition by player_id order by finished_at) as first_finished_at
from player_session
) a
group by player_id;

insert into player_achievement (player_session_id, achievement_id, unlocked_at)
select min(first_session) as player_session_id, mode_id as achievement_id, min(first_finished_at) as finished_at from (
select mode_id, player_id, first_value(id) over (partition by player_id order by finished_at) as first_session, first_value(finished_at) over (partition by player_id order by finished_at) as first_finished_at
from player_session where mode_id in (2, 3, 4, 5)
) a
group by player_id, mode_id;

insert into player_achievement (player_session_id, achievement_id, unlocked_at)
select min(first_session) as player_session_id, 6 as achievement_id, min(first_finished_at) as finished_at from (
select player_id, first_value(id) over (partition by player_id order by finished_at) as first_session, first_value(finished_at) over (partition by player_id order by finished_at) as first_finished_at
from player_session where started_at > finished_at - interval '20 minutes'
) a
group by player_id;

insert into player_achievement (player_session_id, achievement_id, unlocked_at)
select min(first_session) as player_session_id, 7 as achievement_id, min(first_finished_at) as finished_at from (
select player_id, first_value(id) over (partition by player_id order by finished_at) as first_session, first_value(finished_at) over (partition by player_id order by finished_at) as first_finished_at
from player_session where started_at > finished_at - interval '1 minutes'
) a
group by player_id;

insert into player_achievement (player_session_id, achievement_id, unlocked_at)
select min(first_session) as player_session_id, 8 as achievement_id, min(first_finished_at) as finished_at from (
select player_id, first_value(id) over (partition by player_id order by finished_at) as first_session, first_value(finished_at) over (partition by player_id order by finished_at) as first_finished_at
from player_session where started_at > finished_at - interval '20 seconds'
) a
group by player_id;

-- Предрасчитываем рейтинг для того чтобы быстро показывать пользователю его позицию в общем рейтинге. Для окончательного вывода останется вычесть отключенных пользователей, а это очень быстрая операция
-- Производительность решения - порядка миллиона строк в минуту по всем 6 метрикам
-- План оптимизации: обновлять по каждому режиму игры отдельно
insert into player_rating (player_id, mode_id, r_max_score, r_sum_score, r_min_duration, r_sum_rating_points, r_count, r_count_win, disabled)
select m.* from (
    select
       m.player_id,
       m.mode_id,
       row_number() over (partition by mode_id order by max_score desc, last_activity_at desc) as r_max_score,
       row_number() over (partition by mode_id order by sum_score desc, last_activity_at desc) as r_sum_score,
       row_number() over (partition by mode_id order by min_duration, last_activity_at desc) as r_min_duration,
       row_number() over (partition by mode_id order by sum_rating_points desc, last_activity_at desc) as r_sum_rating_points,
       row_number() over (partition by mode_id order by count desc, last_activity_at desc) as r_count,
       row_number() over (partition by mode_id order by count_win desc, last_activity_at desc) as r_count_win,
       m.disabled
    from player_metric m
) m
left join player_rating r on m.player_id = r.player_id and m.mode_id = r.mode_id
where r.player_id is null or (
    r.r_max_score != m.r_max_score or
    r.r_sum_score != m.r_sum_score or
    r.r_min_duration != m.r_min_duration or
    r.r_sum_rating_points != m.r_sum_rating_points or
    r.r_count != m.r_count or
    r.r_count_win != m.r_count_win
)
on conflict(player_id, mode_id) do update
set
    r_max_score = excluded.r_max_score,
    r_sum_score = excluded.r_sum_score,
    r_min_duration = excluded.r_min_duration,
    r_sum_rating_points = excluded.r_sum_rating_points,
    r_count = excluded.r_count,
    r_count_win = excluded.r_count_win
;
