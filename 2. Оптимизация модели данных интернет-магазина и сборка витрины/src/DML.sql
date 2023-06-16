INSERT INTO public.shipping_country_rates (shipping_country, shipping_country_base_rate)
select distinct shipping_country, shipping_country_base_rate from shipping s order by 1;   

-- select * from public.shipping_country_rates;

INSERT INTO public.shipping_agreement (agreementid, agreement_number, agreement_rate, agreement_commission)
SELECT vad[1]::bigint, vad[2]::text, vad[3]::numeric(3,2), vad[4]::numeric(3,2) from
(SELECT distinct regexp_split_to_array(vendor_agreement_description, ':+') AS vad FROM shipping) AS tab; 

-- select * from public.shipping_agreement;

INSERT INTO public.shipping_transfer (transfer_type, transfer_model, shipping_transfer_rate)
SELECT std[1]::text, std[2]::text, shipping_transfer_rate::numeric(4,3) from
(SELECT distinct regexp_split_to_array(shipping_transfer_description, ':+') AS std, shipping_transfer_rate FROM shipping order by 1) AS tab; 

-- select * from public.shipping_transfer;

INSERT INTO public.shipping_info (shippingid, vendorid, payment_amount, shipping_plan_datetime, transfer_type_id, shipping_country_id, agreementid)
select
	distinct s.shippingid, s.vendorid, s.payment_amount, s.shipping_plan_datetime, st.transfer_type_id, scr.shipping_country_id, sa.agreementid
from 
	shipping s
join 
	(select agreementid, agreementid::int8||':'||agreement_number::text||':'||agreement_rate::float||':'||agreement_commission::float as vendor_agreement_description from public.shipping_agreement) sa on s.vendor_agreement_description = sa.vendor_agreement_description
join
	(select shipping_country_id, shipping_country, shipping_country_base_rate from public.shipping_country_rates) scr on s.shipping_country = scr.shipping_country and s.shipping_country_base_rate = scr.shipping_country_base_rate
join 
	(select transfer_type_id, transfer_type||':'||transfer_model as shipping_transfer_description, shipping_transfer_rate from public.shipping_transfer) st on s.shipping_transfer_description = st.shipping_transfer_description and s.shipping_transfer_rate = st.shipping_transfer_rate;

-- select * from public.shipping_info;

INSERT INTO public.shipping_status (shippingid, status, state, shipping_start_fact_datetime, shipping_end_fact_datetime)
with 
	t2 as (select shippingid, state_datetime as shipping_end_fact_datetime from shipping s where state = 'recieved'),
	t3 as (select shippingid, status, state from shipping where (shippingid, state_datetime) in (select shippingid, max(state_datetime) from shipping group by shippingid))
select 
	t1.shippingid, t3.status, t3.state, t1.state_datetime shipping_start_fact_datetime, t2.shipping_end_fact_datetime 
from 
	shipping t1
left join 
	t2 on t1.shippingid = t2.shippingid
left join 
	t3 on t1.shippingid = t3.shippingid
where 
	t1.state = 'booked';
 
-- select * from public.shipping_status; 