-- creaate database for urban comapny
create database urban_company;

-- use database
use urban_company;

-- create tables and load data
-- 1. customers
create table customers (
cust_id varchar(20),
cust_name varchar(100),
age int,
city  varchar(50),
gender varchar(10),
email varchar(100),	
address varchar(255),
phone_num  varchar(50),
singup_channel varchar(50),
singup_date date,
signup_time time);

-- 2. subs
create table subs (
sub_id varchar(20),
cust_id varchar(20),
subs_type varchar(70),
start_date varchar(20));

-- 3. services
create table services (
service_id varchar(20),
service varchar(50),
subservice_name varchar(50),
service_options varchar(50),
subservice_charge int);

-- 4. del_id
create table del_id (
del_id varchar(20),
full_name varchar(50),
gender varchar(20),
email varchar(50),
onboarding_channel varchar(30),
category varchar(30),
join_date varchar(30),
age int);

-- 5. cart_items
create table cart_items (
cust_id varchar(20),
cart_id varchar(20),
service_id varchar(50),
add_to_cart_date varchar(30),
source_channel varchar(30));

-- 6.  payments
create table payments (
order_id varchar(20),
cart_id varchar(50),
coupon_applied varchar(30),
coupon_discount varchar(30),
payment_mode varchar(30),
payment_date varchar(30));

-- 7. del_assigned
create table del_assigned (
service_id varchar(50),
order_id varchar(30),
del_id_assigned varchar(30),
datebooked varchar(30),
slot_booked varchar(30),
order_status varchar(30));

-- 8. feedback
create table feedback (
feedback_id varchar(30),
order_id varchar(30),
submitted_on varchar(30),
rating varchar(30),
comments varchar(150));

-- 9. refunds
create table refunds (
refund_id varchar(30),
order_id varchar(30),
refund_status varchar(50),
refund_reason varchar(70),
requested_on varchar(30),
processed_on  varchar(30));

-- 10. cost_pct
CREATE TABLE cost_pct (
  category VARCHAR(50) PRIMARY KEY,
  cost_pct DECIMAL(5,4) 
);

INSERT INTO cost_pct (category, cost_pct) VALUES
('Spa', 0.65 ),
('Beauty & Salon', 0.60         ),
('Cleaning & Pest Control', 0.70     ),
('Appliance Repari And Service',0.65),
('Repair', 0.60);

-- Importing Data
set global local_infile = 1;

load data local infile 'd:/data analytics/cp_1/urban company/data/data2/cart items.csv'
into table cart_items
fields terminated by ',' enclosed by '"'
lines terminated by '\r\n'         
ignore 1 lines;



/* ----------------------------------- DATA CLEANING, DATA TYPE CONVERSION AND ESTABLISHING RELATIONSHIPS ----------------------------------*/

-- 1) CUSTOMERS 

-- check data from table
select * from customers;

-- check cols type, and constraints
describe customers;

-- extracting city from address
alter table customers add column city varchar(20);

-- inserting into customers
update customers set 
	city = trim(substring_index(address,',',-1));

-- dropping address, email and phone_num
alter table customers drop column address, drop column email, drop column phone_num;

-- establishing primay key
alter table customers add constraint primary key(cust_id);

-- renaming cols
alter table customers rename column singup_date to signup_date;

-- extract year, month etc from date cols
select signup_date,
	extract(year from signup_date) as year,
    extract(month from signup_date) as month,
    monthname (signup_date) as month_name,
    extract(day from signup_date) as day,
    dayname(signup_date) as day_name from customers;

-- updating tables with new cols
alter table customers
	add column signup_year int,
    add column signup_month int,
    add column signup_monthN varchar(30),
    add column signup_day int, 
    add column signup_dayN varchar(30);
    
update customers set 
	signup_year = extract(year from signup_date), 
	signup_month = extract(month from signup_date),
    signup_monthN = monthname (signup_date),
    signup_day = extract(day from signup_date),
    signup_dayN = dayname(signup_date);
    
select signup_time,
	extract(hour from signup_time) as hour,
    extract(minute from signup_time) as min
from customers;

alter table customers 
	add column signup_hour int,
    add column signup_min int;
    
update customers set
	signup_hour = extract(hour from signup_time),
    signup_min = extract(minute from signup_time);
    
-- dropping unnecessary cols
alter table customers drop column signup_time;

select left(gender,1), left(signup_monthN,3), left(signup_dayN,3) from customers;

update customers set 
	gender =  left(gender,1),
    signup_monthN = left(signup_monthN,3),
    signup_dayN = left(signup_dayN,3);

alter table customers rename column singup_channel to signup_channel;

select signup_channel from customers
group by signup_channel;
-- all are standardized


-- 2) SUBSCRIBERS

select * from subs;

describe subs;

alter table subs
add constraint primary key (sub_id),
add constraint foreign key (cust_id) references customers(cust_id);

update subs
set sub_date = str_to_date(start_date,'%d-%m-%Y');

alter table subs
modify sub_date date;

select trim(substring_index(subs_type,'-',-1)) as Sub_type, sub_date,
	extract(year from sub_date) as sub_year, 
	extract(month from sub_date) as sub_month,
	left(monthname(sub_date),3) as sub_monthN,
	extract(day from sub_date) as sub_day,
	left(dayname(sub_date),3) as sub_dayN
from subs;

update subs
set subs_type = case
	when subs_type = 'yearly' then 'half-yearly'
	else 'annual'
end;

alter table subs
	add column sub_year int,
    add column sub_month int,
    add column sub_monthN varchar(30),
    add column sub_day int, 
    add column sub_dayN varchar(30);

update subs set 
	subs_type = trim(substring_index(subs_type,'-',-1)),
    sub_year = extract(year from sub_date), 
	sub_month = extract(month from sub_date),
    sub_monthN = left(monthname(sub_date),3),
    sub_day = extract(day from sub_date),
    sub_dayN = left(dayname(sub_date),3);
    
-- 3) SERVICES

select * from services;

describe services;

alter table services
add constraint primary key (service_id);

-- 4) DELIVERY PERSON ID'S

select * from del_id;

describe del_id;

alter table del_id
add constraint primary key(del_id);

update del_id
set join_date = str_to_date(join_date,'%d-%m-%Y');

alter table del_id
modify join_date date;

select left(gender,1) as gender, join_date,
	extract(year from join_date) as join_year, 
	extract(month from join_date) as join_month,
	left(monthname(join_date),3) as join_monthN,
	extract(day from join_date) as join_day,
	left(dayname(join_date),3) as join_dayN
from del_id;

alter table del_id
	add column join_year int,
    add column join_month int,
    add column join_monthN varchar(30),
    add column join_day int, 
    add column join_dayN varchar(30);

update del_id set 
	gender = left(gender,1),
    join_year = extract(year from join_date), 
	join_month = extract(month from join_date),
    join_monthN = left(monthname(join_date),3),
    join_day = extract(day from join_date),
    join_dayN = left(dayname(join_date),3);

alter table del_id drop column email;

select onboarding_channel from del_id 
group by onboarding_channel;

-- all are standardized

select category from del_id 
group by category;

-- all are standardized



-- 5) CART ITEMS

select * from cart_items;

describe cart_items;

alter table cart_items modify column addC_date varchar(50);
describe cart_items;

alter table cart_items
add constraint foreign key(cust_id) references customers(cust_id),
add constraint foreign key (service_id) references services(service_id);

update cart_items
set addC_date = str_to_date(addC_date,'%d-%m-%Y');

alter table  cart_items
modify addC_date date;

select addC_date,
	extract(year from addC_date) as addC_year, 
	extract(month from addC_date) as addC_month,
	left(monthname(addC_date),3) as addC_monthN,
	extract(day from addC_date) as addC_day,
	left(dayname(addC_date),3) as addC_dayN
from cart_items;

alter table cart_items
	add column addC_year int,
    add column addC_month int,
    add column addC_monthN varchar(30),
    add column addC_day int, 
    add column addC_dayN varchar(30);

update cart_items set 
    addC_year = extract(year from addC_date), 
	addC_month = extract(month from addC_date),
    addC_monthN = left(monthname(addC_date),3),
    addC_day = extract(day from addC_date),
    addC_dayN = left(dayname(addC_date),3);
    
select source_channel from cart_items
group by source_channel;
-- all are standardized

select * from cart_items;

-- 6) PAYMENTS

select * from payments;

describe payments;

alter table payments
add constraint primary key(order_id);

update payments
set pay_date = str_to_date(pay_date,'%d-%m-%Y');

alter table payments
modify pay_date date;

select pay_date,
	extract(year from pay_date) as pay_year, 
	extract(month from pay_date) as pay_month,
	left(monthname(pay_date),3) as pay_monthN,
	extract(day from pay_date) as pay_day,
	left(dayname(pay_date),3) as pay_dayN
from payments;

alter table payments
	add column pay_year int,
    add column pay_month int,
    add column pay_monthN varchar(30),
    add column pay_day int, 
    add column pay_dayN varchar(30);

update payments set 
    pay_year = extract(year from pay_date), 
	pay_month = extract(month from pay_date),
    pay_monthN = left(monthname(pay_date),3),
    pay_day = extract(day from pay_date),
    pay_dayN = left(dayname(pay_date),3);
    
select payment_mode from payments
group by payment_mode;
-- all are standardized

select coupon_applied from payments
group by coupon_applied;
-- all are standardized

select substring_index(coupon_discount,'%',1) from payments
group by coupon_discount;

alter table payments add column `cou_dis_%`  int;

update payments
	set `cou_dis_%` = trim(substring_index(coupon_discount,'%',1));
	
alter table payments drop column coupon_discount;

select order_id, case 
	when refund_reason is null then 'Completed'
	else 'Refunded'
    end as order_status 
from (
	select p.*,r.refund_reason from payments p
	left join refunds r 
	on p.order_id = r.order_id)t;
		
alter table payments
add column order_status varchar(30);

update payments p
left join refunds r 
on p.order_id = r.order_id
set p.order_status = case when 
							r.refund_reason is null then 'Completed'
							else 'Refunded'
                            end;


select * from payments;

-- 7) DELIVERY ID ASSIGNED

select * from del_id_assigned; 

describe del_assigned;

update del_id_assigned
set del_id_assigned = null
where del_id_assigned  = '';

describe del_id_assigned;

alter table del_id_assigned
modify datebooked date;

select del_id_assigned,
	extract(year from datebooked) as S_booked_year, 
	extract(month from datebooked) as S_booked_month,
	left(monthname(datebooked),3) as S_booked_monthN,
	extract(day from datebooked) as S_booked_day,
	left(dayname(datebooked),3) as S_booked_dayN
from del_id_assigned;

alter table del_id_assigned
	add column S_booked_year int,
    add column S_booked_month int,
    add column S_booked_monthN varchar(30),
    add column S_booked_day int, 
    add column S_booked_dayN varchar(30);

update del_id_assigned set 
    S_booked_year = extract(year from datebooked), 
	S_booked_month = extract(month from datebooked),
    S_booked_monthN = left(monthname(datebooked),3),
    S_booked_day = extract(day from datebooked),
    S_booked_dayN = left(dayname(datebooked),3);

select slot_booked, 
	trim(substring_index(slot_booked,'-',1)) as slot_startT, 
	trim(substring_index(slot_booked,'-',-1)) as slot_endT
from del_id_assigned;

alter table del_id_assigned 
	add column slot_startT time,
	add column slot_endT time;

update del_id_assigned set
	slot_startT = trim(substring_index(slot_booked,'-',1)),
    slot_endT = trim(substring_index(slot_booked,'-',-1));
	
alter table del_id_assigned 
    drop column slot_booked;

select order_status from del_id_assigned
group by order_status;

-- all are standardized

select * from del_id_assigned;

-- 8) FEEDBACK

select * from feedback;

describe feedback;

update feedback
set submitted_on = str_to_date(submitted_on,'%d-%m-%Y');

alter table feedback
modify submitted_on date;

alter table feedback
	add constraint primary key(feedback_id),
	add constraint foreign key (order_id) references payments(order_id);

select rating from feedback 
group by rating;
-- ranges from 0-5

select submitted_on,
	extract(year from submitted_on) as feedback_year, 
	extract(month from submitted_on) as feedback_month,
	left(monthname(submitted_on),3) as feedback_monthN,
	extract(day from submitted_on) as feedback_day,
	left(dayname(submitted_on),3) as feedback_dayN
from feedback;

alter table feedback
	add column feedback_year int,
    add column feedback_month int,
    add column feedback_monthN varchar(30),
    add column feedback_day int, 
    add column feedback_dayN varchar(30);

update feedback set 
    feedback_year = extract(year from submitted_on), 
	feedback_month = extract(month from submitted_on),
    feedback_monthN = left(monthname(submitted_on),3),
    feedback_day = extract(day from submitted_on),
    feedback_dayN = left(dayname(submitted_on),3);

alter table feedback 
	drop column submitted_on,
    drop column comments;

select * from feedback;

-- 9) REFUNDS

select * from refunds;

describe refunds;

update refunds
set requested_on = str_to_date(requested_on,'%d-%m-%Y'),
processed_on = str_to_date(processed_on,'%d-%m-%Y');

alter table refunds
modify requested_on date,
modify	processed_on date;

select refund_reason from refunds
group by refund_reason;

select refund_status from refunds
group by refund_status;

select extract(year from requested_on) as year from refunds;

alter table refunds
	add column refund_req_year int;

update refunds
	set refund_req_year  = extract(year from requested_on);
    
select * from refunds;



/* --------------------------------------------------------------------EDA ----------------------------------------------------------------*/

select count(distinct(cust_id)) as unique_custid, count(cust_id) as all_cust_id, count(*) as total_rows from customers;
-- 1502

select min(age) as min_age, max(age) as max_age  from customers;
-- age range 18 - 65

select signup_channel from customers
group by signup_channel;

select city from customers
group by city;
-- only Delhi

select min(signup_year) as min_year, max(signup_year) as max_year from customers;
-- 2023 - 205

select signup_year, max(signup_month) as max_year, min(signup_month) as min_year from customers
group by signup_year;
-- Aug 2023 - Aug 2025 

select signup_dayN, count(signup_dayN) as count from customers
group by signup_dayN 
order by count desc ;
-- highest singup's on - sat, lowest - Friday

select count(distinct(sub_id)) as sub_id , count(sub_id) as all_sub_id,count(*) as total_rows
from subs;
-- 328, no null and duplicates

select subs_type, count(subs_type) as count from subs
group by subs_type
order by count desc;
-- 281 (half yearly), 47(annual)

select count(distinct(del_id)), count(del_id) as all_del_id , count(*) as total_rows from del_id;
-- 149 , no null and duplicates

select join_year from del_id
group by join_year;
-- 2020

select gender, count(gender) as count from del_id
group by gender
order by count desc;
-- M(71), F(61), O(17)

select category, count(category) as count from del_id
group by category 
order by count desc;
-- 5 categories, salon(35 max), pest_control(34), home_repair(24 min)

select count(distinct(service_ID)) as unique_service_id, count(service_ID) as all_service_id, count(*) as total_rows 
from services;
-- 89

select service, count(service) as count from services
group by service
order by count desc;
-- appliance repair service = 33(max). 5 services

select max(subservice_charge) as max_charge, min(subservice_charge) as min_charge from services;
-- range 29-4399

select max(subservice_charge), min(subservice_charge) from services
where subservice_charge > 2000;
-- 9 (2500 - 2399)

select * from services
where subservice_charge > 2000;
-- 3 services are overated

select count(distinct(cart_id)) from cart_items;
-- 4277 cart items

select count(order_id) from payments;
-- 3475 payments received

select count(feedback_id) from feedback;
-- 3475

select count(refund_id) from refunds;
-- 361

/*-------------------------------------- FEATURE ENGINEERING - DERIVING BILL, REVENUE, PROFIT --------------------------------------------*/

-- Real business problem.
-- Whenever a discount is applied at the order level but costs live at the service (line-item) 
-- level, we must allocate the order discount back to each service before calculating profit.
-- Marketplaces (e-com, food delivery, travel) routinely discount at order level while costs 
-- are per product/service.
-- 🔹 The Core Issue
-- Discount → applied at the order level (after summing all services).
-- Cost → exists at the service level (each service has its own category %).
-- so to calculate net profit for the company we can not take order amount, we have to derive item net amount and then build 
-- up to order amount

-- step 1 :- getting cart_id, service_id, service_name, price

create or replace view pnl_preview as 
select c.cust_id, c.cart_id, c.service_id, s.service,c.add_to_cart_date, c.source_channel,
s.subservice_charge as price, p.cost_pct
from cart_items c
join services s 
on c.service_id = s.service_id
join cost_pct p
on p.category = s.service
where cart_id in
(select cart_id from payments);

-- step 2 :- finding out best possible discount

-- 2.1 :- find out subscription amount whereever applied

create or replace view discount_preview as
select distinct * from (
with cte1 as 
(select p.cart_id, p.`cou_dis_%`,c.cust_id,p.pay_date,s.subs_type,s.sub_date,
case when
	s.subs_type = 'half-yearly' then date_add(s.sub_date,interval 6 month)
    else date_add(s.sub_date, interval 12 month)
    end as sub_end_date
from payments p
join cart_items c on p.cart_id = c.cart_id
left join subs s on s.cust_id = c.cust_id)
select cust_id, cart_id, pay_date,`cou_dis_%`,sub_date, sub_end_date,
case when
	pay_date between sub_date and sub_end_date then 'In_Sub'
    else 'Subs_NA'
    end as sub_status
from cte1)t;

-- 2.2. using best discount
create or replace view discount as
select *
from(
select c.cust_id, c.cart_id,round(d.`cou_dis_%`/100,2) as offer_dis,d.sub_status
from cart_amount c 
join discount_preview d on c.cart_id = d.cart_id
and c.cust_id = d.cust_id
order by c.cust_id, c.cart_id, d.pay_date)t;

select * from discount;

-- step 3 :- deriving price, cost, bill

create or replace view pnl_preview2 as
with cte1 as 
(select pp.*,
round(pp.price * pp.cost_pct,2) as item_cost,
sum(pp.price) over(partition by pp.cart_id) as order_gross,
d.offer_dis, d.sub_status,
case when sub_status = 'In_Sub' then 0.10 else 0.00 end as sub_discount
from pnl_preview pp
join discount d 
on d.cart_id = pp.cart_id)
select cust_id, cart_id, service_id, add_to_cart_date, source_channel, price, cost_pct, item_cost,
order_gross, (order_gross*0.09) as service_charge,
(order_gross + (order_gross*0.09)) as total_charge,
offer_dis, sub_status, sub_discount
from cte1;

select * from pnl_preview2
where offer_dis <> 0;
-- results -> service_wise_profit

create or replace view service_wise_profit as
with cte1 as	
(with cte as
(select *,
case when offer_dis > sub_discount then total_charge*offer_dis
	when offer_dis < sub_discount then  round(least(total_charge*sub_discount,100),2)
    else 0.0
    end as discount_amt,
price/total_charge as item_share
from pnl_preview2)
select *,
round(discount_amt * item_share,2) as item_discount_allocated,
round(price - (discount_amt * item_share),2) as item_net
from cte)
select *, item_net - item_cost as item_profit from cte1;


-- results -> Order_wise_profit
create or replace view  order_wise_profit as
select cust_id,cart_id, source_channel,order_gross,service_charge,total_charge,order_profit
from (
select * ,
sum(item_profit) over(partition by cart_id) as order_profit,
row_number() over(partition by cart_id) as rn
from service_wise_profit)t
where rn = 1;

-- results -> service category wise profit

select service, category_profit from (
select sp.cust_id, sp.cart_id, sp.source_channel, sp.order_gross, sp.service_charge,
sp.total_charge,sp.item_profit, s.service,
sum(item_profit) over(partition by service) as category_profit,
row_number() over(partition by service) as rn
from service_wise_profit sp
join services s on s.service_id = sp.service_id)t
where rn=1 ;

/*--------------------------------------------------------------- Analysis -----------------------------------------------------------------*/

-- 1) CUSTOMER

-- TOTAL CUSTOMERS --

select count(cust_id) as total_customers from customers; 

-- ACTIVE CUSTOMERS --

select count(distinct(cust_id)) as active_customers from cart_items 
where cart_id in 
	(select cart_id from payments);

-- LIST OF NON-ACTIVE CUST_ID FOR FIRST PURCHASE OFFER

select count(cust_id) from customers 
where cust_id not in (
	select cust_id from cart_items 
	where cart_id in 
		(select cart_id from payments));

-- REPEAT CUSTOMERS % --

# compared to total_customers - 66%

select round(100*(select count(*) as repeat_customers from(
select cust_id , count(cart_id) as count_of_orders_placed
from cart_items 
where cart_id in (
select cart_id from payments)
group by cust_id
having count_of_orders_placed>1)as repeat_customers ) /
(select count(cust_id) as total_customers from customers),2) as cust_per_rate; 

# compared to active customers - 79%

select round(100*(select count(*) as repeat_customers from(
select cust_id , count(cart_id) as count_of_orders_placed
from cart_items 
where cart_id in (
select cart_id from payments)
group by cust_id
having count_of_orders_placed>1)as repeat_customers ) /
(select count(distinct(cust_id)) as active_customers from cart_items 
where cart_id in 
	(select cart_id from payments)),2) as cust_per_rate; 

-- CLV -- 
-- CLV=Average Order Value (AOV)×Purchase Frequency×Customer Lifespan (in periods)×Profit Margin

-- 2) SUBSCRIBERS

-- TOTAL SUBSCRIBERS --

select count(*) from subs;

-- Subscriber % -- 

select round((select count(cust_id) from customers)/(select count(cust_id) from subs),2) as `subs_%` ;

-- SUBSCRIPTION REVENUE --

set @annual := 299;
set @`half-yearly` :=249;

select sum(subs_fees) as subs_fees from (
select case when 
	subs_type = 'Annual' then @annual
    else @`half-yearly`
    end as subs_fees
 from subs)t;

-- CHURN RATE -- --I AM HERE - SUBS NOT RENEWED



-- 3) ORDERS

-- Total Orders --

select count(order_id) from payments;

-- COMPLETED ORDERS --

select round(100*
(select count(order_id) from payments
where order_status = 'completed')
/
(select count(order_id) from payments),2) as competed_order_per;
-- 90

-- ORDERS PER CUSTOMERS --

create or replace view orders as 
select distinct p.order_id,c.cust_id,  p.cart_id, p.order_status from payments p
right join cart_items c
on p.cart_id = c.cart_id
where order_id is not null;

select round(
(select count(order_id) from orders)
/
(select count(distinct(cust_id)) from orders),2) as order_per_customer;

-- Cart-to-Order Conversion Rate -- 

select round(100*
(select count(cart_id) from orders) / (select count(distinct(cart_id)) from cart_items),2) as CTOCR;


-- 4) SERVICES

-- TOP SERVICES BY REVENUE -- 

select service, concat(round(sum(total_bill)/1000000,2),' M') total_revnue	,
rank() over (order by  sum(total_bill) desc) as rank_num from (
select distinct c.cart_id,s.service_ID,s.service,b.total_bill from cart_items c
join payments p on p.cart_id = c.cart_id
join bill b on p.cart_id = b.cart_id
join services s on c.service_id = s.service_id
where p.order_status = 'completed')t
group by service;

select service, subservice_name,
concat(round(sum(total_bill)/1000000,2),' M') total_revnue	,
row_number() over (partition by service order by  sum(total_bill) desc) as rank_num from (
select distinct c.cart_id,s.service_ID,s.service,b.total_bill,s.subservice_name from cart_items c
join payments p on p.cart_id = c.cart_id
join bill b on p.cart_id = b.cart_id
join services s on c.service_id = s.service_id
where p.order_status = 'completed')t
group by service,subservice_name;

-- AVG SERVICE CHARGE --

select service, round(avg(subservice_charge),2) as avg_charge
from services
group by service
order by avg_charge desc;

# categorisation by subservice_name
select service, subservice_name, round(avg(subservice_charge),2) as avg_charge
from services
group by service, subservice_name;


-- 5) PAYMENTS

-- GROSS REVENUE --

select concat(round(sum(total_bill)/1000000,2),'M') as gross_revenue from bill;

-- AVERAGE ORDER VALUE --

select round(
(select sum(total_bill)as gross_revenue from bill)
/
(select count(cart_id) as total_orders from bill),2) as avg_order_value;

-- REFUND % --

select round(
(select sum(total_bill) as refund_amount  from bill
where cart_id in
(select cart_id from payments
where order_status != 'Completed'))
/
(select sum(total_bill)as gross_revenue from bill),2) as `refund_%`;

-- NET REVENUE --

select round(
(select sum(total_bill) as gross_revenue from bill)
- 
(select sum(total_bill) as refund_amount  from bill
where cart_id in
(select cart_id from payments
where order_status != 'Completed')),2) as net_revenue; 

-- DISCOUNT IMPACT % --

select round(100*
(select sum(base_amount - amount_after_discount) as savings from bill)
/
(select sum(base_amount) from bill),2) as discount_impact_per;

-- 6) DELIVERY ID'S

-- TOTAL DEL ID'S REGISTERED

select count(del_id) from del_id;

-- ACTIVE SERVICE PROVIDERS --

select count(distinct(del_id_assigned)) from del_id_assigned;

-- ORDERS PER PROVIDERS --

select  del_id_assigned, count(distinct(order_id)) as orders_completed
from del_id_assigned
where del_id_assigned != 'null'
group by del_id_assigned
order by orders_completed desc;

-- 7) FEEDBACK

-- AVG CUSTOMER RATING --

select round(avg(rating),2) avg_rating from feedback;

-- AVG RATING WHEN ORDERS WERE COMPLETED --

select round(avg(rating),2) avg_rating from feedback
where rating !=0;

-- LOW RATING % ORDERS	 --

# all orders
select round(100*
(select count(*) as low_rated_orders from feedback
where rating <= 3 )
/
(select count(*) as total_orders from feedback),2) as `low_rating_%`;


# compeleted orders
select round(100*
(select count(*) as low_rated_orders from feedback
where rating <= 3 and rating !=0)
/
(select count(*) as total_orders from feedback),2) as `low_rating_%`;

