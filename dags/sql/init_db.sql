create table if not exists laptop (
    id varchar(6),
    name_laptop varchar,
    link varchar,
    primary key (id)
);


create table if not exists price (
    id_laptop varchar(6),
    dt date,
    price int,
    primary key (id_laptop, dt)
);


ALTER TABLE price DROP CONSTRAINT IF EXISTS fk_price_id_laptop;
ALTER TABLE price ADD constraint fk_price_id_laptop FOREIGN KEY(id_laptop) REFERENCES laptop(id);