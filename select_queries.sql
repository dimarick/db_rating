-- Проще всего получить верхушку рейтинга
-- Запросы к player_metric и player_rating работают одинаково хорошо, поэтому источник выбираем по оперативности обновления
select *
from public.player_metric m
where m.mode_id = 1 and m.disabled = false
-- здесь любое поле по которому строится топ
order by max_score limit 1000;

-- Наивное решение: используем оконные функции. Решение работает очень медленно, так как каждый раз рейтинг строится заново
explain analyse
select * from (
    select
        player_id,
        mode_id,
        max_score,
        sum_score,
        min_duration,
        sum_rating_points,
        count,
        count_win,
        last_activity_at,
        disabled,
       row_number() over (partition by mode_id order by max_score desc, last_activity_at desc) as r_max_score,
       row_number() over (partition by mode_id order by sum_score desc, last_activity_at desc) as r_sum_score,
       row_number() over (partition by mode_id order by min_duration, last_activity_at desc) as r_min_duration,
       row_number() over (partition by mode_id order by sum_rating_points desc, last_activity_at desc) as r_sum_rating_points,
       row_number() over (partition by mode_id order by count desc, last_activity_at desc) as r_count,
       row_number() over (partition by mode_id order by count_win desc, last_activity_at desc) as r_count_win
    from player_metric
) a where player_id = (select id from player where email = '636600@32628.com');

-- Считаем сколько человек справились лучше заданного пользователя. Работает удовлетворительно, ответ получается за несколько секунд по рейтингу размером около миллиона и растет линейно с объемом данных
-- По одной метрике для лидеров рейтинга и вовсе быстро
explain analyse
with player_metric_summary as(
    select m3.id as mode_id, (select r_max_score from player_rating r4 where r4.mode_id = m3.id order by r4.mode_id, r4.r_max_score desc limit 1) as count from game_mode m3
)
select
    a.player_id,
    a.mode_id,
    a.max_score,
    a.sum_score,
    a.min_duration,
    a.sum_rating_points,
    a.count,
    a.count_win,
    a.last_activity_at,
    a.disabled,
    (a.r_max_score + a.r_max_score2) as r_max_score,
    (a.r_sum_score + a.r_sum_score2) as r_sum_score,
    (a.r_min_duration + a.r_min_duration2) as r_min_duration,
    (a.r_sum_rating_points + a.r_sum_rating_points2) as r_sum_rating_points,
    (a.r_count + a.r_count2) as r_count,
    (a.r_count_win + a.r_count_win2) as r_count_win,
    (a.r_max_score + a.r_max_score2)::double precision / a.max_rating as p_max_score,
    (a.r_sum_score + a.r_sum_score2)::double precision / a.max_rating as p_sum_score,
    (a.r_min_duration + a.r_min_duration2)::double precision / a.max_rating as p_min_duration,
    (a.r_sum_rating_points + a.r_sum_rating_points2)::double precision / a.max_rating as p_sum_rating_points,
    (a.r_count + a.r_count2)::double precision / a.max_rating as p_count,
    (a.r_count_win + a.r_count_win2)::double precision / a.max_rating as p_count_win
from (
    select
       m.player_id,
       m.mode_id,
       m.max_score,
       m.sum_score,
       m.min_duration,
       m.sum_rating_points,
       m.count,
       m.count_win,
       m.last_activity_at,
       m.disabled,
       s.count as max_rating,
       (select count(*) from player_metric m2 where m2.mode_id = m.mode_id and m2.max_score > m.max_score) as r_max_score,
       (select count(*) from player_metric m2 where m2.mode_id = m.mode_id and m2.max_score = m.max_score and m2.last_activity_at > m.last_activity_at) + 1 as r_max_score2,
       (select count(*) from player_metric m2 where m2.mode_id = m.mode_id and m2.sum_score > m.sum_score) as r_sum_score,
       (select count(*) from player_metric m2 where m2.mode_id = m.mode_id and m2.sum_score = m.sum_score and m2.last_activity_at > m.last_activity_at) + 1 as r_sum_score2,
       (select count(*) from player_metric m2 where m2.mode_id = m.mode_id and m2.min_duration < m.min_duration) as r_min_duration,
       (select count(*) from player_metric m2 where m2.mode_id = m.mode_id and m2.min_duration = m.min_duration and m2.last_activity_at > m.last_activity_at) + 1 as r_min_duration2,
       (select count(*) from player_metric m2 where m2.mode_id = m.mode_id and m2.sum_rating_points > m.sum_rating_points) as r_sum_rating_points,
       (select count(*) from player_metric m2 where m2.mode_id = m.mode_id and m2.sum_rating_points = m.sum_rating_points and m2.last_activity_at > m.last_activity_at) + 1 as r_sum_rating_points2,
       (select count(*) from player_metric m2 where m2.mode_id = m.mode_id and m2.count > m.count) as r_count,
       (select count(*) from player_metric m2 where m2.mode_id = m.mode_id and m2.count = m.count and m2.last_activity_at > m.last_activity_at) + 1 as r_count2,
       (select count(*) from player_metric m2 where m2.mode_id = m.mode_id and m2.count_win > m.count_win) as r_count_win,
       (select count(*) from player_metric m2 where m2.mode_id = m.mode_id and m2.count_win = m.count_win and m2.last_activity_at > m.last_activity_at) + 1 as r_count_win2
    from public.player_metric m
    inner join player_metric_summary s on s.mode_id = m.mode_id
    where player_id = (select id from player where email = '636600@32628.com')
) a;

-- Наиболее быстрое решение, единицы секунд по базе на 100М строк
-- Решение применимо к практически любому мыслимому объему данных, вопрос только в скорости обновления таблицы player_rating
explain analyse
with player_metric_summary as(
    select
        m3.id as mode_id,
        (select r_max_score from player_rating r4 where r4.mode_id = m3.id order by r4.mode_id, r4.r_max_score desc limit 1) as count
    from game_mode m3
)
select
   r.player_id,
   r.mode_id,
   s.count as max_rating,
   r_max_score - (select count(*) from player_rating r2 where r2.mode_id = r.mode_id and r2.disabled and r2.r_max_score < r.r_max_score) as r_max_score,
   r_sum_score - (select count(*) from player_rating r2 where r2.mode_id = r.mode_id and r2.disabled and r2.r_sum_score < r.r_sum_score) as r_max_score,
   r_min_duration - (select count(*) from player_rating r2 where r2.mode_id = r.mode_id and r2.disabled and r2.r_min_duration < r.r_min_duration) as r_max_score,
   r_sum_rating_points - (select count(*) from player_rating r2 where r2.mode_id = r.mode_id and r2.disabled and r2.r_sum_rating_points < r.r_sum_rating_points) as r_max_score,
   r_count - (select count(*) from player_rating r2 where r2.mode_id = r.mode_id and r2.disabled and r2.r_count < r.r_count) as r_max_score,
   r_count_win - (select count(*) from player_rating r2 where r2.mode_id = r.mode_id and r2.disabled and r2.r_count_win < r.r_count_win) as r_max_score
from player_rating r
inner join player_metric_summary s on s.mode_id = r.mode_id
where player_id = (select id from player where email = '636600@32628.com');
