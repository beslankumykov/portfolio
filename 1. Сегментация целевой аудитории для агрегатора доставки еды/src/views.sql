-- Создание представлений в схеме analysis
create or replace view analysis.orders as select * from production.orders o;
create or replace view analysis.orderitems as select * from production.orderitems oi;
create or replace view analysis.orderstatuses as select * from production.orderstatuses os;
create or replace view analysis.orderstatuslog as select * from production.orderstatuslog osl;
create or replace view analysis.products as select * from production.products p;
create or replace view analysis.users as select * from production.users u;
