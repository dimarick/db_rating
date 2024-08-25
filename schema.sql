-- Проект простого рейтингового сервера бд простой однопользовательской логической онлайн-игры (подобной sudoku, minesweeper, etc). Однако для pvp схема тоже подходит с некоторыми допущениями.

-- Часть OLTP: представление данных в нормальной форме

drop table if exists player cascade;
drop table if exists game_mode cascade;
drop table if exists achievement cascade;
drop table if exists player_session cascade;
drop table if exists player_achievement cascade;
drop table if exists player_metric cascade;
drop table if exists player_rating cascade;

create table player (
    id uuid primary key default gen_random_uuid() not null,
    country char(2) not null,
    username varchar(200),
    email varchar(200),
    registered_at timestamp not null,
    disabled bool default false not null,
    check ( upper(country) = country and length(country) = 2 )
);

comment on table player is 'Пользователь, игрок';
comment on column player.id is 'Идентификатор';
comment on column player.country is 'Код страны';
comment on column player.username is 'Имя или null, для анонимных пользователей';
comment on column player.email is 'емейл или null для незарегистрированных пользователей';
comment on column player.registered_at is 'дата регистрации или первого входа для анонимных пользователей';
comment on column player.disabled is 'Отключен, забанен (shadow ban)';

-- Объявляем таблицы-словари. Используем суррогатный ключ для более компактного хранения
create table game_mode (
    id serial primary key,
    name text not null
);

comment on table game_mode is 'Режимы игры, уровни сложности';

create table achievement (
    id serial primary key,
    name text not null
);

comment on table achievement is 'Доступные достижения';

create table player_session (
    id bigserial primary key not null,
    player_id uuid not null,
    mode_id int not null,
    started_at timestamp not null,
    finished_at timestamp not null,
    score int not null,
    rating_points int not null,
    -- констрейнты - это не только гарантия целостности данных, но и своего рода документация, имеющая гарантию актуальности
    foreign key (player_id) references player(id) on delete cascade on update restrict,
    foreign key (mode_id) references game_mode(id) on delete restrict on update restrict,
    check ( score >= 0 ),
    check ( rating_points >= 0 ),
    check ( finished_at >= started_at )
);

create index player_session_user_id on player_session(player_id, mode_id);
create index player_session_mode_id on player_session(mode_id);

comment on table player_session is 'Сеанс игры';
comment on column player_session.started_at is 'Начало, используется для рекордов времени и хронологии игр';
comment on column player_session.finished_at is 'Окончание игры, используется для рекордов времени и хронологии игр';
comment on column player_session.score is 'Количество очков';
comment on column player_session.rating_points is 'Количество очков рейтинга (на самом деле не важно, важно что очки нескольких видов)';

create table player_achievement (
    player_session_id bigint not null,
    achievement_id int not null,
    unlocked_at timestamp not null,
    primary key (player_session_id, achievement_id),
    foreign key (player_session_id) references player_session(id) on delete cascade on update restrict,
    foreign key (achievement_id) references achievement(id) on delete cascade on update restrict
);

create index player_achievement_achievement_id on player_achievement(achievement_id);

comment on table player_achievement is 'Полученные достижения';

-- Часть OLAP: денормализованое представление данных в форме, пригодной для анализа

create table player_metric (
    player_id uuid not null,
    mode_id int not null,
    max_score int not null,
    sum_score int not null,
    min_duration double precision not null,
    sum_rating_points int not null,
    count int not null,
    count_win int not null,
    last_activity_at timestamp,
    disabled bool default false,
    primary key(player_id, mode_id),
    foreign key (player_id) references player(id) on delete cascade on update restrict,
    foreign key (mode_id) references game_mode(id) on delete cascade on update restrict,
    check ( max_score >= 0 ),
    check ( sum_score >= 0 and sum_score >= max_score ),
    check ( min_duration >= 0.0 ),
    check ( sum_rating_points >= 0 ),
    check ( count >= 0 ),
    check ( count_win >= 0 and count_win <= count )
);

create index player_metric_max_score on player_metric (mode_id, max_score desc);
create index player_metric_sum_score on player_metric (mode_id, sum_score desc);
-- только здесь результат означает меньше - значит лучше, поэтому в построении рейтинга, будет сортировка по возрастанию
create index player_metric_min_duration on player_metric (mode_id, min_duration asc);
create index player_metric_sum_rating_points on player_metric (mode_id, sum_rating_points desc);
create index player_metric_count on player_metric (mode_id, count desc);
create index player_metric_count_win on player_metric (mode_id, count_win desc);

comment on table player_metric is 'Метрики пользователя для рейтинга';
comment on column player_metric.max_score is 'Максимальное количество очков';
comment on column player_metric.sum_score is 'Общее количество очков';
comment on column player_metric.min_duration is 'Рекордное время';
comment on column player_metric.sum_rating_points is 'Общий рейтинг, используется для оценки активности пользователя';
comment on column player_metric.count is 'Число игр';
comment on column player_metric.count_win is 'Число побед';
comment on column player_metric.last_activity_at is 'Время последней активности пользователя';
comment on column player_metric.disabled is 'Пользователь отключен (shadow ban)';

-- Расчитанная позиция в рейтинге по каждому пользователю и по каждой метрике
-- поскольку "неудобное" изменение одного значения метрики может затронуть значительную часть таблицы, она наполняется вручную, а не триггерами, чтобы избежать эффектов write amplification, и нестабильного времени обновления
-- В реальном приложении обновление должно быть запланировано через систему очередей, например любой брокер AMQP и т.п.
create table player_rating (
    player_id uuid not null,
    mode_id int not null,
    r_max_score int not null,
    r_sum_score int not null,
    r_min_duration int not null,
    r_sum_rating_points int not null,
    r_count int not null,
    r_count_win int not null,
    disabled bool default false,
    primary key(player_id, mode_id),
    foreign key (player_id) references player(id) on delete cascade on update restrict,
    foreign key (mode_id) references game_mode(id) on delete cascade on update restrict,
    check ( r_max_score >= 0 ),
    check ( r_sum_score >= 0 ),
    check ( r_min_duration >= 0.0 ),
    check ( r_sum_rating_points >= 0 ),
    check ( r_count >= 0 ),
    check ( r_count_win >= 0 )
);

-- По условиям задачи, отключенных пользователей незначительное количество. Это значит что размер индекса и скорость доступа к нему многократно будет выше.
create index player_rating_r_max_score on player_rating(mode_id, r_max_score desc) where disabled = true;
create index player_rating_r_sum_score on player_rating(mode_id, r_sum_score desc) where disabled = true;
create index player_rating_r_min_duration on player_rating(mode_id, r_min_duration desc) where disabled = true;
create index player_rating_r_sum_rating_points on player_rating(mode_id, r_sum_rating_points desc) where disabled = true;
create index player_rating_r_count on player_rating(mode_id, r_count desc) where disabled = true;
create index player_rating_r_count_win on player_rating(mode_id, r_count_win desc) where disabled = true;
-- Для быстрого расчета перцентиля нужно знать сколько всего участников в рейтинге. Самый быстрый способ - взять по индексу первое значение
create index player_rating_r_max_score_all on player_rating(mode_id, r_max_score desc);

-- Предполагаемая нагрузка предполагается будет состоять из 99% инсертов и <1% остальных операций
-- Поэтому insert делаем эффективнее: исключаем проход по всем соседним сессиям игрока
drop function if exists insert_metrics();
create or replace function insert_metrics() returns trigger
as $$

-- updated_data содержит массив вставленных данных из которого нам требуется только список затронутых player_id
-- Алгоритм: агрегируем вставленные данные. Если до этого данных по игроку не было, то просто вставляем, иначе "прибавляем" новые агрегированные данные к старым

declare updated_data player_session%rowtype;
begin
    insert into player_metric (
        player_id,
        mode_id,
        max_score,
        sum_score,
        min_duration,
        sum_rating_points,
        count,
        count_win,
        last_activity_at,
        disabled
    )
    select
        player.id as player_id,
        session.mode_id,
        max(session.score) as max_score,
        sum(session.score) as sum_score,
        min(extract(epoch from session.finished_at - session.started_at)) as min_duration,
        sum(session.rating_points) as sum_rating_points,
        count(*) as count,
        count(*) filter ( where session.score > 0 ) as count_win,
        max(session.finished_at) as last_activity_at,
        player.disabled
    from updated_data session
    left join player on session.player_id = player.id
    group by player.id, session.mode_id
    on conflict (player_id, mode_id) do update
    set
        max_score = greatest(excluded.max_score, player_metric.max_score),
        sum_score = excluded.sum_score + player_metric.sum_score,
        min_duration = least(excluded.min_duration, player_metric.min_duration),
        sum_rating_points = excluded.sum_rating_points + player_metric.sum_rating_points,
        count = excluded.count + player_metric.count,
        count_win = excluded.count_win + player_metric.count_win,
        last_activity_at = greatest(excluded.last_activity_at, player_metric.last_activity_at),
        disabled = excluded.disabled;

    return null;
end;
$$ language plpgsql;

-- Поскольку нагрузка будет состаоять из <1% операций обновления/удаления,
-- то обработчик разумнее всего оптимизировать по простоте и надежности.
-- Алгоритм: для каждого игрока, упомянутого в данных обновления пересчитываем статистику целиком.
-- Удаление (а update = insert + delete в общем случае) данных невозможно реализовать без полного пересчета, так как уравнение a = max(b, x) не имеет решения относительно x.

drop function if exists update_metrics();
create or replace function update_metrics() returns trigger
as $$
declare
    updated_data player_session%rowtype;
begin
    insert into player_metric (
        player_id,
        mode_id,
        max_score,
        sum_score,
        min_duration,
        sum_rating_points,
        count,
        count_win,
        last_activity_at,
        disabled
    )
    select
        player.id as player_id,
        p.mode_id,
        coalesce(max(session.score), 0) as max_score,
        coalesce(sum(session.score), 0) as sum_score,
        coalesce(min(extract(epoch from session.finished_at - session.started_at)), 0) as min_duration,
        coalesce(sum(session.rating_points), 0) as sum_rating_points,
        coalesce(count(score), 0) as count,
        coalesce(count(*) filter ( where score > 0 ), 0) as count_win,
        coalesce(max(session.finished_at), player.registered_at) as last_activity_at,
        player.disabled
    from (select distinct player_id as id, mode_id from updated_data) p
    inner join player on p.id = player.id
    left join public.player_session session on player.id = session.player_id and p.mode_id = session.mode_id
    group by player.id, p.mode_id
    on conflict (player_id, mode_id) do update
    set
        max_score = excluded.max_score,
        sum_score = excluded.sum_score,
        min_duration = excluded.min_duration,
        sum_rating_points = excluded.sum_rating_points,
        count = excluded.count,
        count_win = excluded.count_win,
        last_activity_at = excluded.last_activity_at,
        disabled = excluded.disabled
    where
        player_metric.max_score != excluded.max_score or
        player_metric.sum_score != excluded.sum_score or
        player_metric.min_duration != excluded.min_duration or
        player_metric.sum_rating_points != excluded.sum_rating_points or
        player_metric.count != excluded.count or
        player_metric.count_win != excluded.count_win or
        player_metric.last_activity_at != excluded.last_activity_at or
        player_metric.disabled != excluded.disabled;

    delete from player_metric where count = 0;

    return null;
end;
$$ language plpgsql;

drop function if exists update_player();
create or replace function update_player() returns trigger
as $$
declare
    updated_data player%rowtype;
    disabled bool;
begin
    select u.disabled into disabled from updated_data u;
    raise notice 'update_player %', disabled;

    update player_metric m set disabled = u.disabled from updated_data u where m.player_id = u.id;
    update player_rating m set disabled = u.disabled from updated_data u where m.player_id = u.id;
    return null;
end;
$$ language plpgsql;

create or replace trigger player_metric_insert
       after insert on player_session
    referencing new table as updated_data
    for each statement
       execute function insert_metrics();

create or replace trigger player_metric_update
       after update on player_session
    referencing new table as updated_data
    for each statement
       execute function update_metrics();

create or replace trigger player_metric_delete
       after delete on player_session
    referencing old table as updated_data
    for each statement
       execute function update_metrics();

create or replace trigger player_update
       after update on player
    referencing new table as updated_data
    for each statement
       execute function update_player();

insert into game_mode(id, name)
values
(1, 'Новичек'),
(2, 'Любитель'),
(3, 'Профессионал'),
(4, 'Эксперт'),
(5, 'Невозможный')
;

insert into achievement(id, name)
values
(1, 'Пройдена первая игра'),
(2, 'Пройден уровень "Любитель"'),
(3, 'Пройден уровень "Профессионал"'),
(4, 'Пройден уровень "Эксперт"'),
(5, 'Пройден невозможный уровень'),
(6, 'Уровень пройден за 20 минут'),
(7, 'Уровень пройден за 1 минуту'),
(8, 'Уровень пройден за 20 секунд')
;