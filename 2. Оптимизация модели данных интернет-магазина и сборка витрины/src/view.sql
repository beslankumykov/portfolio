create or replace view shipping_datamart as
select 
	ss.shippingid, 
	si.vendorid, 
	st.transfer_type, 
	date_part('day',ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime) as full_day_at_shipping,
	case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then 1 else 0 end as is_delay,
	case when ss.status = 'finished' then 1 else 0 end as is_shipping_finish,
	case when ss.shipping_end_fact_datetime > si.shipping_plan_datetime then date_part('day',AGE(ss.shipping_end_fact_datetime, si.shipping_plan_datetime)) else null end as delay_day_at_shipping,
	si.payment_amount,
	si.payment_amount * (scr.shipping_country_base_rate + sa.agreement_rate + st.shipping_transfer_rate) as vat, 
	si.payment_amount * sa.agreement_commission as profit
from 
	public.shipping_status ss
join 
	public.shipping_info si on ss.shippingid = si.shippingid
join 
	public.shipping_transfer st on si.transfer_type_id = st.transfer_type_id
join 
	public.shipping_country_rates scr on si.shipping_country_id = scr.shipping_country_id
join
	public.shipping_agreement sa on si.agreementid = sa.agreementid;

select * from public.shipping_datamart;