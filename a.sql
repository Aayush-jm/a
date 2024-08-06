with transactions_with_rewards as (

    select 
        a.*,
        (coalesce(b.transaction_amount, 0) - coalesce(b.refund_amount, 0)) as jewels_earned,
        b.offer_id, 
        b.created_at as jewel_earning_at,
        b.redeemable_by as jewels_redeemable_by
    from csb.rupay_card.metabase_csb_card_transactions a

    left join jupiter.rewards.stg_rewards_transactions b
        on a.jupiter_transaction_id = b.transaction_id
),

ntj_users as (
select distinct user_id  from jupiter.brahma.report_jupiter_account_created 
where first_onboarded_product = 'Edge Rupay Credit Card'
),

final as (
select 

    o.user_id as ob_user,
    o.card_issuance_at	,
    o.is_virtual_card_activated,
    o.physical_card_ordered_at,
    o.physical_card_delivered_at,	
    o.is_physical_card_activated,	
    o.terminated_at,
    case when ntj.user_id is not null then 'NTJ' else 'ETJ' end as ntj_etj_flag,
	
    t.user_id as transacting_user,
    t.jupiter_transaction_id,
    t.network_transaction_id,
    t.amount,
    t.transaction_type,
    t.transaction_status,
    t.transaction_channel,
    lower(t.merchant_name) as merchant_name,
    t.mcc,
    upper(t.jupiter_response_code) as jupiter_response_code,
    upper(t.card_partner_response_code) as card_partner_response_code,
    upper(t.response_code) as response_code,
    t.is_international,
    t.is_repayment,
    t.is_refund,
    t.created_at as txn_date,
    t.updated_at,
    t.card_read_method,
    t.card_auth_mode,
    t.service,
    t.is_tpap_initiated,
    t.is_success_transaction,
    t.opening_date,
    t.closing_date,
    t.credit_limit,
    t.fraud_decline_code,
    t.offer_id,
    t.jewels_earned,
    t.jewel_earning_at,
    t.jewels_redeemable_by
    

from csb.rupay_card.metabase_csb_card_onboarded o
left join transactions_with_rewards t 
on o.user_id = t.user_id
left join ntj_users ntj on o.user_id = ntj.user_id
)

select * from final
