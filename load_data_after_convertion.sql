

CREATE SCHEMA IF NOT EXISTS sales;
CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
SET search_path = sales, pg_catalog,public;

DELETE FROM sales.inventory;
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

DELETE FROM sales.sales;
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

DELETE FROM events;
INSERT INTO events (cta_point,decay_factor,created_at)
SELECT (random() * 80 + 60)::int,(random() * 100 + 400)::int,i
FROM generate_series('2017-08-01'::timestamp,'2017-11-30','10 minutes') gs(i);

DELETE FROM events1;
INSERT INTO events1 (cta_point,decay_factor,created_at)
SELECT (random() * 80 + 60)::int,(random() * 100 + 400)::int,i
FROM generate_series('2017-08-01'::timestamp,'2017-11-30','10 minutes') gs(i);

DELETE FROM events_list;
INSERT INTO events_list values(generate_series(1,20),generate_series(1,20));

DELETE FROM events_text;
INSERT INTO events_text values('pune',generate_series(1,20));
INSERT INTO events_text values('nashik',generate_series(1,20));
INSERT INTO events_text values('nagar',generate_series(1,20));
INSERT INTO events_text values('mumbai',generate_series(1,20));


