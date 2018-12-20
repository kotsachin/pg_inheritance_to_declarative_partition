

CREATE SCHEMA IF NOT EXISTS sales;
CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
SET search_path = sales, pg_catalog,public;


CREATE TABLE inventory (
    product character varying(10),
    units_sold character varying(10),
    invoice_date date,
    units_remain character varying(10)
);

CREATE TABLE inv_quarter1 (
    CONSTRAINT inv_quarter1_invoice_date_check CHECK (((invoice_date >= '2017-01-01'::date) AND (invoice_date < '2017-04-01'::date)))
)
INHERITS (inventory);
CREATE TABLE inv_quarter2 (
    CONSTRAINT inv_quarter2_invoice_date_check CHECK (((invoice_date >= '2017-04-01'::date) AND (invoice_date < '2017-07-01'::date)))
)
INHERITS (inventory);

CREATE TABLE inv_quarter3 (
    CONSTRAINT inv_quarter3_invoice_date_check CHECK (((invoice_date >= '2017-07-01'::date) AND (invoice_date < '2017-10-01'::date)))
)
INHERITS (inventory);
CREATE TABLE inv_quarter4 (
    CONSTRAINT inv_quarter4_invoice_date_check CHECK (((invoice_date >= '2017-10-01'::date) AND (invoice_date < '2018-01-01'::date)))
)
INHERITS (inventory);

CREATE FUNCTION orders_insert_simple_inv() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF ( NEW.invoice_date >= DATE '2017-01-01' and NEW.invoice_date < DATE '2017-04-01') THEN
        INSERT INTO inv_quarter1 VALUES (NEW.*);
    ELSIF ( NEW.invoice_date >= DATE '2017-04-01' and NEW.invoice_date < DATE '2017-07-01') THEN
        INSERT INTO inv_quarter2 VALUES (NEW.*);
    ELSIF ( NEW.invoice_date >= DATE '2017-07-01' and NEW.invoice_date < DATE '2017-10-01') THEN
        INSERT INTO inv_quarter3 VALUES (NEW.*);
    ELSIF ( NEW.invoice_date >= DATE '2017-10-01' and NEW.invoice_date < DATE '2018-01-01') THEN
        INSERT INTO inv_quarter4 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Date out of range.  Fix the orders_insert_simple()  function!';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER orders_insert_trigger BEFORE INSERT ON inventory FOR EACH ROW EXECUTE PROCEDURE orders_insert_simple_inv();

DO $$ 
declare
v_product  integer := 1;
v_units_sold integer:= 1;
v_units_remain integer := 100000;
v_invoice_date date='2017-01-01';
begin
      while v_product<> 10000 loop
         insert into sales.inventory values (v_product,v_units_sold,v_invoice_date,v_units_remain);
          v_product:=v_product+1;
          v_units_sold:=v_units_sold+1;
          v_units_remain:=v_units_remain-1;
          v_invoice_date:=v_invoice_date+1;
              if v_invoice_date >'2017-12-31' then
              v_invoice_date := '2017-01-01';
              end if;
     end loop;
end $$;



CREATE TABLE sales (
    product character varying(10),
    units_sold character varying(10),
    invoice_date date,
    units_remain character varying(10)
);

CREATE TABLE sales_quarter1 (
    CONSTRAINT sales_quarter1_invoice_date_check CHECK (((invoice_date >= '2017-01-01'::date) AND (invoice_date < '2017-04-01'::date)))
)
INHERITS (sales);
CREATE TABLE sales_quarter2 (
    CONSTRAINT sales_quarter2_invoice_date_check CHECK (((invoice_date >= '2017-04-01'::date) AND (invoice_date < '2017-07-01'::date)))
)
INHERITS (sales);



CREATE FUNCTION orders_insert_simple_sales() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF ( NEW.invoice_date >= DATE '2017-01-01' and NEW.invoice_date < DATE '2017-04-01') THEN
        INSERT INTO sales_quarter1 VALUES (NEW.*);
    ELSIF ( NEW.invoice_date >= DATE '2017-04-01' and NEW.invoice_date < DATE '2017-07-01') THEN
        INSERT INTO sales_quarter2 VALUES (NEW.*);
    ELSE
        RAISE EXCEPTION 'Date out of range.  Fix the orders_insert_simple_sales()  function!';
    END IF;
    RETURN NULL;
END;
$$;

CREATE TRIGGER orders_insert_trigger_sales BEFORE INSERT ON sales FOR EACH ROW EXECUTE PROCEDURE orders_insert_simple_sales();


DO $$ 
declare
v_product  integer := 1;
v_units_sold integer:= 1;
v_units_remain integer := 100000;
v_invoice_date date='2017-01-01';
begin
      while v_product<> 10000 loop
         insert into sales.sales values (v_product,v_units_sold,v_invoice_date,v_units_remain);
          v_product:=v_product+1;
          v_units_sold:=v_units_sold+1;
          v_units_remain:=v_units_remain-1;
          v_invoice_date:=v_invoice_date+1;
              if v_invoice_date >'2017-06-30' then
              v_invoice_date := '2017-01-01';
              end if;
     end loop;
end $$;

-------------------

SET search_path = public, sales, pg_catalog;

CREATE TABLE events (
    id serial primary key,
    cta_point integer,
    decay_factor integer,
    created_at timestamp without time zone);

CREATE TABLE events_201708 (LIKE events INCLUDING ALL, check(created_at >= '2017-08-01' and created_at < '2017-09-01')) INHERITS (events);
CREATE TABLE events_201709 (LIKE events INCLUDING ALL, check(created_at >= '2017-09-01' and created_at < '2017-10-01')) INHERITS (events);
CREATE TABLE events_201710 (LIKE events INCLUDING ALL, check(created_at >= '2017-10-01' and created_at < '2017-11-01')) INHERITS (events);
CREATE TABLE events_201711 (LIKE events INCLUDING ALL, check(created_at >= '2017-11-01' and created_at < '2017-12-01')) INHERITS (events);

CREATE OR REPLACE FUNCTION fn_insert_events() RETURNS TRIGGER AS
    $function$
    BEGIN 
    EXECUTE format($$INSERT INTO %I VALUES ($1.*)$$, 'events_'||to_char(NEW.created_at, 'YYYYMM')) USING NEW;
    RETURN NULL;
    END;
    $function$ LANGUAGE plpgsql;

CREATE TRIGGER events_trigger BEFORE INSERT ON events FOR EACH ROW EXECUTE PROCEDURE fn_insert_events();

INSERT INTO events (cta_point,decay_factor,created_at)
SELECT (random() * 80 + 60)::int,(random() * 100 + 400)::int,i
FROM generate_series('2017-08-01'::timestamp,'2017-11-30','10 minutes') gs(i);


CREATE TABLE events1 (
    id serial primary key,
    cta_point integer,
    decay_factor integer,
    created_at timestamp without time zone);

CREATE TABLE events1_201708 (LIKE events1 INCLUDING ALL, check(created_at BETWEEN '2017-08-01'::TIMESTAMP and '2017-09-01'::TIMESTAMP)) INHERITS (events1);
CREATE TABLE events1_201709 (LIKE events1 INCLUDING ALL, check(created_at BETWEEN '2017-09-01'::TIMESTAMP and '2017-10-01'::TIMESTAMP)) INHERITS (events1);
CREATE TABLE events1_201710 (LIKE events1 INCLUDING ALL, check(created_at BETWEEN '2017-10-01'::TIMESTAMP and '2017-11-01'::TIMESTAMP)) INHERITS (events1);
CREATE TABLE events1_201711 (LIKE events1 INCLUDING ALL, check(created_at BETWEEN '2017-11-01'::TIMESTAMP and '2017-12-01'::TIMESTAMP)) INHERITS (events1);

CREATE OR REPLACE FUNCTION fn_insert_events1() RETURNS TRIGGER AS
    $function$
    BEGIN
    EXECUTE format($$INSERT INTO %I VALUES ($1.*)$$, 'events1_'||to_char(NEW.created_at, 'YYYYMM')) USING NEW;
    RETURN NULL;
    END;
    $function$ LANGUAGE plpgsql;

CREATE TRIGGER events1_trigger BEFORE INSERT ON events1 FOR EACH ROW EXECUTE PROCEDURE fn_insert_events1();

INSERT INTO events1 (cta_point,decay_factor,created_at)
SELECT (random() * 80 + 60)::int,(random() * 100 + 400)::int,i
FROM generate_series('2017-08-01'::timestamp,'2017-11-30','10 minutes') gs(i);





CREATE TABLE events_list(
    id int primary key,
    cta_point integer);

CREATE TABLE events_list_1 (LIKE events_list INCLUDING ALL, check(id IN ( 1,2,3,4,5 ) )) INHERITS (events_list);
CREATE TABLE events_list_2 (LIKE events_list INCLUDING ALL, check(id IN ( 6,7,8,9,10 ) )) INHERITS (events_list);
CREATE TABLE events_list_3 (LIKE events_list INCLUDING ALL, check(id IN ( 11,12,13,14,15 ) )) INHERITS (events_list);
CREATE TABLE events_list_4 (LIKE events_list INCLUDING ALL, check(id IN ( 16,17,18,19,20 ) )) INHERITS (events_list);


CREATE OR REPLACE FUNCTION fn_insert_events_list() RETURNS TRIGGER AS
    $function$
    BEGIN
    IF (NEW.id IN (1,2,3,4,5)) THEN 
        EXECUTE format($$INSERT INTO %I VALUES ($1.*)$$, 'events_list_1') USING NEW;
    ELSIF (NEW.id IN (6,7,8,9,10)) THEN 
        EXECUTE format($$INSERT INTO %I VALUES ($1.*)$$, 'events_list_2') USING NEW;
    ELSIF (NEW.id IN (11,12,13,14,15)) THEN
        EXECUTE format($$INSERT INTO %I VALUES ($1.*)$$, 'events_list_3') USING NEW; 
    ELSE 
        EXECUTE format($$INSERT INTO %I VALUES ($1.*)$$, 'events_list_4') USING NEW;
    END IF;
    RETURN NULL;
    END;
    $function$ LANGUAGE plpgsql;

CREATE TRIGGER events_list_trigger BEFORE INSERT ON events_list FOR EACH ROW EXECUTE PROCEDURE fn_insert_events_list();

INSERT INTO events_list values(generate_series(1,20),generate_series(1,20));

CREATE TABLE events_text(
    city text ,
    cta_point integer);

CREATE TABLE events_text_pune (LIKE events_text INCLUDING ALL, check(city = 'pune' )) INHERITS (events_text);
CREATE TABLE events_text_nashik (LIKE events_text INCLUDING ALL, check(city = 'nashik' )) INHERITS (events_text);
CREATE TABLE events_text_nagar (LIKE events_text INCLUDING ALL, check(city = 'nagar' )) INHERITS (events_text);
CREATE TABLE events_text_mumbai (LIKE events_text INCLUDING ALL, check(city = 'mumbai' )) INHERITS (events_text);


CREATE OR REPLACE FUNCTION fn_insert_events_text() RETURNS TRIGGER AS
    $function$
    BEGIN
    IF (NEW.city = 'pune' ) THEN
        EXECUTE format($$INSERT INTO %I VALUES ($1.*)$$, 'events_text_pune') USING NEW;
    ELSIF (NEW.city = 'nashik' ) THEN
        EXECUTE format($$INSERT INTO %I VALUES ($1.*)$$, 'events_text_nashik') USING NEW;
    ELSIF (NEW.city = 'nagar' ) THEN
        EXECUTE format($$INSERT INTO %I VALUES ($1.*)$$, 'events_text_nagar') USING NEW;
    ELSE
        EXECUTE format($$INSERT INTO %I VALUES ($1.*)$$, 'events_text_mumbai') USING NEW;
    END IF;
    RETURN NULL;
    END;
    $function$ LANGUAGE plpgsql;

CREATE TRIGGER events_text_trigger BEFORE INSERT ON events_text FOR EACH ROW EXECUTE PROCEDURE fn_insert_events_text();

INSERT INTO events_text values('pune',generate_series(1,20));
INSERT INTO events_text values('nashik',generate_series(1,20));
INSERT INTO events_text values('nagar',generate_series(1,20));
INSERT INTO events_text values('mumbai',generate_series(1,20));


