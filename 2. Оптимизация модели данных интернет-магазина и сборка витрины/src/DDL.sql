DROP TABLE IF EXISTS public.shipping_country_rates;
CREATE TABLE public.shipping_country_rates(
	shipping_country_id serial,
	shipping_country text,
	shipping_country_base_rate numeric(3,2),
   	PRIMARY KEY (shipping_country_id)
);

DROP TABLE IF EXISTS public.shipping_agreement;
CREATE TABLE public.shipping_agreement(
	agreementid bigint,
	agreement_number text,
	agreement_rate numeric(3,2),
	agreement_commission numeric(3,2),
   	PRIMARY KEY (agreementid)
);

DROP TABLE IF EXISTS public.shipping_transfer;
CREATE TABLE public.shipping_transfer(
	transfer_type_id serial,
	transfer_type text,
	transfer_model text,
	shipping_transfer_rate numeric(4,3),
   	PRIMARY KEY (transfer_type_id)
);

DROP TABLE IF EXISTS public.shipping_info;
CREATE TABLE public.shipping_info(
	shippingid bigint,
	vendorid bigint,
	payment_amount float4,
	shipping_plan_datetime timestamp,
	transfer_type_id bigint,
	shipping_country_id bigint,
	agreementid bigint,
   	PRIMARY KEY (shippingid),
   	FOREIGN KEY (transfer_type_id) REFERENCES shipping_transfer(transfer_type_id) ON UPDATE cascade,
   	FOREIGN KEY (shipping_country_id) REFERENCES shipping_country_rates(shipping_country_id) ON UPDATE cascade,
   	FOREIGN KEY (agreementid) REFERENCES shipping_agreement(agreementid) ON UPDATE cascade
);

DROP TABLE IF EXISTS public.shipping_status;
CREATE TABLE public.shipping_status(
	shippingid bigint,
	status text,
	state text,
	shipping_start_fact_datetime timestamp,
	shipping_end_fact_datetime timestamp,
   	PRIMARY KEY (shippingid)
);