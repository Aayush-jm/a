with transactions_with_rewards as (
    select 
            jupiter_transaction_id, 
            coalesce(sum(case when action in ('transaction') then jewels_earned end),0) as transaction_jewels_earned,
            coalesce(sum(case when offer_id in ('14618','14652','14651','14653','14762') then jewels_earned end),0) as switch_jewels_earned,
            coalesce(sum(case when action not in ('transaction') then jewels_earned end),0) as other_jewels_earned,
            coalesce(sum(jewels_earned),0) as jewels_earned                          
    from 
    (select 
        a.jupiter_transaction_id,
        offer_id,
        action,
        (coalesce(b.transaction_amount, 0) - coalesce(b.refund_amount, 0)) as jewels_earned
        
    from csb.rupay_card.metabase_csb_card_transactions a

    left join jupiter.rewards.stg_rewards_transactions b on a.jupiter_transaction_id = b.transaction_id and action not in ('convert-to-gold', 'convert-to-cash')
    group by 1,2,3,4
    )
    group by 1
),

transacting_users as (
select 
        date(date_trunc('month',created_at)) as transaction_month, 
        user_id
from csb.rupay_card.metabase_csb_card_transactions 
where transaction_type = 'DEBIT'
group by 1,2
),

card_issued_base as (
    SELECT
            user_id ,
            date(card_issuance_at) as card_issued_date
    FROM csb.rupay_card.metabase_csb_card_onboarded
    GROUP BY 1,2
),


activation_users as (
select user_id, min(transaction_month) as activation_month from transacting_users
group by 1
),

previous_month_active as (
select user_id, transaction_month, lag(transaction_month,1) over(partition by user_id order by transaction_month) as previous_month from transacting_users
),

-- select * from previous_month_active where date_diff(month, previous_month, transaction_month) > 1  and previous_month is not null 


lifecycle as (
select 
        b.user_id as transacting_user,
        b.transaction_month,
        case 
            when date_diff(month, d.previous_month, b.transaction_month) = 1 then 'repeat_user'
            when date_diff(month, d.previous_month, b.transaction_month) > 1 then 'resurrected_user'
            when b.transaction_month = activation_month then 'new_user'
        else null end as lifecycle_stage,
        case
            when date_diff(month, d.previous_month, b.transaction_month) >= 1 then 'repeat_user'
            when b.transaction_month = activation_month then 'new_user'
        else null end as repeat_or_new_user
        
        
from card_issued_base a 
left join transacting_users b on a.user_id = b.user_id
left join activation_users c on a.user_id = c.user_id
left join previous_month_active d on b.user_id = d.user_id and b.transaction_month = d.transaction_month
),

switch as
(
select * from 
(select 
        rewards_subscription_id, 
        user_id, 
        created_at, 
        case 
            when rewards_subscription_id = 240 then 'DINING'
            when rewards_subscription_id = 241 then 'SHOPPING'
            when rewards_subscription_id = 242 then 'TRAVEL'
        end as category_selected,
        row_number() over(partition by user_id order by created_at asc) r 
from jupiter.rewards.stg_rewards_user_subscriptions
where rewards_subscription_id in (240, 241, 242)
)
where r = 1 
),


final as 
(select 

    o.user_id as ob_user,
    o.card_issuance_at	,
    o.is_virtual_card_activated,
    o.physical_card_ordered_at,
    o.physical_card_delivered_at,	
    o.is_physical_card_activated,	
    o.terminated_at,
    
    report_user_proxy_income.age_bucket,
    report_user_proxy_income.user_affluent_class,
    report_user_proxy_income.user_proxy_income_band,
    
	
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
    date_trunc('month', t.created_at) as txn_month,
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
    t.is_fraud_high_risk,
    t.is_fraud_low_risk,
    t.is_fraud_medium_risk,
    t.is_low_tenure,
    t.is_ultra_low_tenure,
    t.risk_tier,
    l.lifecycle_stage,
    l.repeat_or_new_user,
    r.jewels_earned,
    r.transaction_jewels_earned,
    r.other_jewels_earned,
    r.switch_jewels_earned,
    case 
        when date(t.created_at) >= date('2024-03-27') 
            and date(t.created_at) >= date(s.created_at) 
            and s.category_selected is not null then true
        when date(t.created_at) >= date('2024-03-27') 
            and date(t.created_at) >= date(s.created_at) 
            and s.category_selected is null then false
    end as switch_opted,
    
    case 
        when date(t.created_at) >= date('2024-03-27')  then 'POST_SWITCH'
        when date(t.created_at) <= date('2024-03-27') then 'PRE_SWITCH'
    end as pre_post_switch,
    
    case when switch.user_id is not null then 'SWITCH'
    else 'SWITCH_NOT_OPTED' end as switch_option,
    
    case when switch.user_id is not null then switch.category_selected
    else 'SWITCH_NOT_OPTED' end as category_option,
    
    case 
        when date(t.created_at) >= date('2024-03-27') 
            and date(t.created_at) >= date(s.created_at) 
            and s.category_selected is not null then s.category_selected
        when date(t.created_at) >= date('2024-03-27') 
            and date(t.created_at) >= date(s.created_at) 
            and s.category_selected is null then 'SWITCH_NOT_OPTED'
    end as switch_sub_category,
    
    case 
        when date(t.created_at) >= date('2024-03-27') 
            and date(t.created_at) >= date(s.created_at) 
            and s.category_selected is not null then 'SWITCH_OPTED'
        when date(t.created_at) >= date('2024-03-27') 
            and date(t.created_at) >= date(s.created_at) 
            and s.category_selected is null then 'SWITCH_NOT_OPTED'
        else 'SWITCH_NOT_OPTED'
    end as switch_category,
    
    s.user_id as switch_user,
    s.created_at as switch_introduction_at,
    case 
        when (date(t.created_at) < date('2024-04-15')) and mcc in (7996, 7933, 7932, 7929, 7922, 7841, 7833, 7832, 7829, 5815, 5814, 5813, 5812, 5811, 5499, 5462, 4899) then 'Dining'
        when (date(t.created_at) < date('2024-04-15')) and mcc in (8044, 8043, 7641, 7394, 7333, 7297, 7296, 7278, 7251, 7221, 7217, 7216, 7211, 7210, 5999, 5995, 5994, 5993, 5992, 5977, 5973, 5972, 5971, 
        5970, 5969, 5968, 5967, 5966, 5965, 5964, 5963, 5950, 5949, 5948, 5947, 5946, 5945, 5944, 5943, 5942, 5941, 5932, 5921, 5818, 5817, 5735, 5733, 
        5732, 5722, 5719, 5714, 5712, 5699, 5698, 5691, 5681, 5661, 5655, 5651, 5641, 5631, 5621, 5611, 5451, 5422, 5411, 5399, 5331, 5311, 5310, 5309, 
        5261, 5251, 5231, 5193, 5139, 5137, 5122, 5111, 5099, 5094, 4812, 5978, 5976, 5975, 5962, 5960, 5940, 5937, 5933, 5931, 5718, 5713, 5697, 5599,
        5598, 5592, 5046, 0743, 5715, 5262, 9751, 8099, 5571, 5561, 5552, 5551, 5541, 5533, 5532, 5521, 5511, 5499, 5271, 5199, 5198, 5192, 5131, 5074, 5072, 5045, 4816)
        then 'Shopping'
        when (date(t.created_at) < date('2024-04-15')) and mcc in (4131, 3268, 3265, 3264, 3258, 3257, 3255, 3250, 3249, 3244, 3237, 3232, 3230, 3227, 3225, 3224, 3214, 3210, 3209, 3208, 3207, 3205, 
                3202, 3201, 3199, 3198, 3195, 3194, 3189, 3179, 3173, 3169, 3168, 3166, 3163, 3162, 3160, 3158, 3157, 3155, 3153, 3152, 3150, 3149, 3147, 3142, 3141, 3140, 3139, 3134, 3128, 3124, 3123, 3122, 3121, 3120, 3119, 3116, 3114, 3113, 3109, 3108, 3107, 3105, 3104, 3101, 3095, 3093, 3092, 3091, 3080, 3074, 3073, 3070, 3756, 3835, 3440, 3419, 3291, 3192, 3831, 3830, 3829, 3828, 3827, 3826, 3825, 3824, 3823, 3822, 3821, 3820, 3819, 3818, 3817, 3816, 3815, 3814, 3813, 3812, 3811, 3810, 3809, 3808, 3807, 3806, 3805, 3804, 3803, 3802, 3801, 3800, 3799, 3798, 3797, 3796, 3795, 3794, 3793, 3792, 3791, 3790, 3789, 3788, 3787, 3786, 3785, 3784, 3783, 3782, 3781, 3780, 3779, 3778, 3777, 3776, 3775, 3774, 3773, 3770, 3765, 3764, 3763, 3762, 3761, 3760, 3759, 3758, 3757, 3755, 3753, 3752, 3751, 3750, 3749, 3748, 3747, 3746, 3745, 3744, 3743, 3742, 3741, 3740, 3739, 3738, 3737, 3736, 3735, 3734, 3733, 3732, 3731, 3730, 3729, 3728, 3727, 3726, 3725, 3724, 3723, 3722, 3721, 3720, 3719, 3718, 3717, 3716, 3715, 3714, 3713, 3712, 3711, 3710, 3709, 3708, 3707, 3706, 3705, 3704, 3703, 3702, 3701, 3700, 3699, 3698, 3697, 3696, 3695, 3694, 3693, 3692, 3691, 3690, 3689, 3688, 3687, 3618, 3617, 3616, 3615, 3614, 3613, 3612, 3611, 3610, 3609, 3608, 3607, 3606, 3605, 3604, 3603, 3602, 3601, 3600, 3599, 3598, 3597, 3596, 3595, 3594, 3593, 3592, 3591, 3590, 3589, 3588, 3587, 3586, 3585, 3584, 3583, 3582, 3581, 3580, 3579, 3578, 3577, 3576, 3575, 3574, 3573, 3572, 3571, 3570, 3569, 3568, 3567, 3566, 3565, 3564, 3563, 3562, 3561, 3560, 3559, 3558, 3557, 3556, 3555, 3554, 3553, 3552, 3551, 3550, 3549, 3548, 3547, 3546, 3545, 3544, 3543, 3542, 3541, 3540, 3539, 3538, 3537, 3536, 3535, 3534, 3533, 3532, 3531, 3530, 3529, 3528, 3527, 3526, 3525, 3524, 3523, 3522, 3521, 3520, 3519, 3518, 3517, 3516, 3515, 3514, 3513, 3512, 3511, 3510, 3509, 3508, 3507, 3506, 3505, 3504, 3503, 3502, 3501, 3441, 3439, 3438, 3437, 3436, 3435, 3434, 3433, 3432, 3431, 3430, 3429, 3428, 3427, 3425, 3423, 3421, 3420, 3414, 3412, 3409, 3405, 3400, 3398, 3396, 3395, 3394, 3393, 3391, 3390, 3389, 3388, 3387, 3386, 3385, 3381, 3380, 3376, 3374, 3370, 3368, 3366, 3364, 3362, 3361, 3360, 3359, 3357, 3355, 3354, 3353, 3352, 3351, 3299, 3298, 3297, 3296, 3295, 3294, 3293, 3292, 3287, 3286, 3285, 3284, 3282, 3281, 3280, 3274, 3273, 3267, 3266, 3301, 3426, 3424, 3422, 3418, 3417, 3416, 3415, 3413, 3411, 3410, 3408, 3407, 3406, 3404, 3403, 3402, 3401, 3399, 3397, 3392, 3384, 3383, 3382, 3379, 3378, 3377, 3375, 3373, 3372, 3371, 3369, 3367, 3365, 3363, 3358, 3356, 3290, 3289, 3288, 3283, 3279, 3278, 3277, 3276, 3275, 3272, 3271, 3270, 3269, 3263, 3262, 3261, 3260, 3259, 3256, 3254, 3253, 3252, 3251, 3248, 3247, 3246, 3245, 3243, 3242, 3241, 3240, 3239, 3238, 3236, 3235, 3234, 3233, 3231, 3229, 3137, 3136, 3135, 3133, 3132, 3131, 3130, 3129, 3127, 3126, 3125, 3118, 3117, 3115, 3112, 3111, 3110, 3106, 3103, 3102, 3100, 3099, 3098, 3097, 3096, 3094, 3090, 3089, 3088, 3087, 3086, 3085, 3084, 3083, 3082, 3081, 3079, 3078, 3077, 3076, 3075, 3072, 3071, 3069, 3068, 3067, 3066, 3065, 3064, 3063, 3062, 3061, 3060, 3059, 3058, 3057, 3056, 3055, 3054, 3053, 3052, 3051, 3050, 3049, 3048, 3047, 3046, 3045, 3044, 3043, 3042, 3041, 3040, 3039, 3038, 3037, 3036, 3035, 3034, 3033, 3032, 3031, 3030, 3029, 3028, 3027, 3026, 3025, 3024, 3023, 3022, 3021, 3020, 3019, 3018, 3017, 3016, 3015, 3014, 3013, 3012, 3011, 3010, 3009, 3008, 3007, 3006, 3005, 3004, 3003, 3002, 3001, 3000, 7991, 7519, 7512, 7011, 5962, 4723, 4722, 4582, 4511, 4457, 4411, 4112, 5309, 4111, 4011, 3839, 3838, 3837, 3836, 3834, 3833, 3832, 3686, 3685, 3684, 3683, 3682, 3681, 3680, 3679, 3678, 3677, 3676, 3675, 3674, 3673, 3672, 3671, 3670, 3669, 3668, 3667, 3666, 3665, 3664, 3663, 3662, 3661, 3660, 3659, 3658, 3657, 3656, 3655, 3654, 3653, 3652, 3651, 3650, 3649, 3648, 3647, 3646, 3645, 3644, 3643, 3642, 3641, 3640, 3639, 3638, 3637, 3636, 3635, 3634, 3633, 3632, 3631, 3630, 3629, 3628, 3627, 3626, 3625, 3624, 3623, 3622, 3621, 3620, 3619, 3772, 3771, 3769, 3768, 3767, 3766, 3754, 3228, 3226, 3223, 3222, 3221, 3220, 3219, 3218, 3217, 3216, 3215, 3213, 3212, 3211, 3206, 3204, 3203, 3200, 3197, 3196, 3193, 3191, 3190, 3188, 3187, 3186, 3185, 3184, 3183, 3182, 3181, 3180, 3178, 3177, 3176, 3175, 3174, 3172, 3171, 3170, 3167, 
                3165, 3164, 3161, 3159, 3156, 3154, 3151, 3148, 3146, 3145, 3144, 3143, 3138, 3308, 3303, 3302, 3300)
        then 'Travel'
        when (date(t.created_at) >= date('2024-04-15')) and mcc in (7996, 7933, 7932, 7929, 7922, 7841, 7833, 7832, 7829, 5815, 5814, 5813, 5812, 5811, 5499, 5462, 4899) then 'Dining'
        when (date(t.created_at) >= date('2024-04-15')) and mcc in (5411, 5732, 5399, 5699, 5262, 5691, 5722, 5137, 4812, 5651, 5944, 5331, 5661, 5611, 5999, 5712, 5977, 5094, 5947, 5945, 5697, 8043, 5995, 5942, 5964, 5621, 5521, 5941, 5948, 5965, 5139, 7394, 5970, 5310, 5641, 5631, 5973, 7296, 5940, 5932, 5192, 5655, 5719, 5963, 5733, 7641, 5949, 5971, 5931, 5309, 5992, 5969, 8044, 5950, 7251, 5551, 5698, 5598, 5735, 5714, 5933, 5937, 5681, 5451, 5422)
        then 'Shopping'
        when (date(t.created_at) >= date('2024-04-15')) and mcc in (4131, 3268, 3265, 3264, 3258, 3257, 3255, 3250, 3249, 3244, 3237, 3232, 3230, 3227, 3225, 3224, 3214, 3210, 3209, 3208, 3207, 3205, 3202, 3201, 3199, 3198, 3195, 3194, 3189, 3179, 3173, 3169, 3168, 3166, 3163, 3162, 3160, 3158, 3157, 3155, 3153, 3152, 3150, 3149, 3147, 3142, 3141, 3140, 3139, 3134, 3128, 3124, 3123, 3122, 3121, 3120, 3119, 3116, 3114, 3113, 3109, 3108, 3107, 3105, 3104, 3101, 3095, 3093, 3092, 3091, 3080, 3074, 3073, 3070, 3756, 3835, 3440, 3419, 3291, 3192, 3831, 3830, 3829, 3828, 3827, 3826, 3825, 3824, 3823, 3822, 3821, 3820, 3819, 3818, 3817, 3816, 3815, 3814, 3813, 3812, 3811, 3810, 3809, 3808, 3807, 3806, 3805, 3804, 3803, 3802, 3801, 3800, 3799, 3798, 3797, 3796, 3795, 3794, 3793, 3792, 3791, 3790, 3789, 3788, 3787, 3786, 3785, 3784, 3783, 3782, 3781, 3780, 3779, 3778, 3777, 3776, 3775, 3774, 3773, 3770, 3765, 3764, 3763, 3762, 3761, 3760, 3759, 3758, 3757, 3755, 3753, 3752, 3751, 3750, 3749, 3748, 3747, 3746, 3745, 3744, 3743, 3742, 3741, 3740, 3739, 3738, 3737, 3736, 3735, 3734, 3733, 3732, 3731, 3730, 3729, 3728, 3727, 3726, 3725, 3724, 3723, 3722, 3721, 3720, 3719, 3718, 3717, 3716, 3715, 3714, 3713, 3712, 3711, 3710, 3709, 3708, 3707, 3706, 3705, 3704, 3703, 3702, 3701, 3700, 3699, 3698, 3697, 3696, 3695, 3694, 3693, 3692, 3691, 3690, 3689, 3688, 3687, 3618, 3617, 3616, 3615, 3614, 3613, 3612, 3611, 3610, 3609, 3608, 3607, 3606, 3605, 3604, 3603, 3602, 3601, 3600, 3599, 3598, 3597, 3596, 3595, 3594, 3593, 3592, 3591, 3590, 3589, 3588, 3587, 3586, 3585, 3584, 3583, 3582, 3581, 3580, 3579, 3578, 3577, 3576, 3575, 3574, 3573, 3572, 3571, 3570, 3569, 3568, 3567, 3566, 3565, 3564, 3563, 3562, 3561, 3560, 3559, 3558, 3557, 3556, 3555, 3554, 3553, 3552, 3551, 3550, 3549, 3548, 3547, 3546, 3545, 3544, 3543, 3542, 3541, 3540, 3539, 3538, 3537, 3536, 3535, 3534, 3533, 3532, 3531, 3530, 3529, 3528, 3527, 3526, 3525, 3524, 3523, 3522, 3521, 3520, 3519, 3518, 3517, 3516, 3515, 3514, 3513, 3512, 3511, 3510, 3509, 3508, 3507, 3506, 3505, 3504, 3503, 3502, 3501, 3441, 3439, 3438, 3437, 3436, 3435, 3434, 3433, 3432, 3431, 3430, 3429, 3428, 3427, 3425, 3423, 3421, 3420, 3414, 3412, 3409, 3405, 3400, 3398, 3396, 3395, 3394, 3393, 3391, 3390, 3389, 3388, 3387, 3386, 3385, 3381, 3380, 3376, 3374, 3370, 3368, 3366, 3364, 3362, 3361, 3360, 3359, 3357, 3355, 3354, 3353, 3352, 3351, 3299, 3298, 3297, 3296, 3295, 3294, 3293, 3292, 3287, 3286, 3285, 3284, 3282, 3281, 3280, 3274, 3273, 3267, 3266, 3301, 3426, 3424, 3422, 3418, 3417, 3416, 3415, 3413, 3411, 3410, 3408, 3407, 3406, 3404, 3403, 3402, 3401, 3399, 3397, 3392, 3384, 3383, 3382, 3379, 3378, 3377, 3375, 3373, 3372, 3371, 3369, 3367, 3365, 3363, 3358, 3356, 3290, 3289, 3288, 3283, 3279, 3278, 3277, 3276, 3275, 3272, 3271, 3270, 3269, 3263, 3262, 3261, 3260, 3259, 3256, 3254, 3253, 3252, 3251, 3248, 3247, 3246, 3245, 3243, 3242, 3241, 3240, 3239, 3238, 3236, 3235, 3234, 3233, 3231, 3229, 3137, 3136, 3135, 3133, 3132, 3131, 3130, 3129, 3127, 3126, 3125, 3118, 3117, 3115, 3112, 3111, 3110, 3106, 3103, 3102, 3100, 3099, 3098, 3097, 3096, 3094, 3090, 3089, 3088, 3087, 3086, 3085, 3084, 3083, 3082, 3081, 3079, 3078, 3077, 3076, 3075, 3072, 3071, 3069, 3068, 3067, 3066, 3065, 3064, 3063, 3062, 3061, 3060, 3059, 3058, 3057, 3056, 3055, 3054, 3053, 3052, 3051, 3050, 3049, 3048, 3047, 3046, 3045, 3044, 3043, 3042, 3041, 3040, 3039, 3038, 3037, 3036, 3035, 3034, 3033, 3032, 3031, 3030, 3029, 3028, 3027, 3026, 3025, 3024, 3023, 3022, 3021, 3020, 3019, 3018, 3017, 3016, 3015, 3014, 3013, 3012, 3011, 3010, 3009, 3008, 3007, 3006, 3005, 3004, 3003, 3002, 3001, 3000, 7991, 7519, 7512, 7011, 5962, 4723, 4722, 4582, 4511, 4457, 4411, 4112, 5309, 4111, 4011, 3839, 3838, 3837, 3836, 3834, 3833, 3832, 3686, 3685, 3684, 3683, 3682, 3681, 3680, 3679, 3678, 3677, 3676, 3675, 3674, 3673, 3672, 3671, 3670, 3669, 3668, 3667, 3666, 3665, 3664, 3663, 3662, 3661, 3660, 3659, 3658, 3657, 3656, 3655, 3654, 3653, 3652, 3651, 3650, 3649, 3648, 3647, 3646, 3645, 3644, 3643, 3642, 3641, 3640, 3639, 3638, 3637, 3636, 3635, 3634, 3633, 3632, 3631, 3630, 3629, 3628, 3627, 3626, 3625, 3624, 3623, 3622, 3621, 3620, 3619, 3772, 3771, 3769, 3768, 3767, 3766, 3754, 3228, 3226, 3223, 3222, 3221, 3220, 3219, 3218, 3217, 3216, 3215, 3213, 3212, 3211, 3206, 3204, 3203, 3200, 3197, 3196, 3193, 3191, 3190, 3188, 3187, 3186, 3185, 3184, 3183, 3182, 3181, 3180, 3178, 3177, 3176, 3175, 3174, 3172, 3171, 3170, 3167, 3165, 3164, 3161, 3159, 3156, 3154, 3151, 3148, 3146, 3145, 3144, 3143, 3138, 3308, 3303, 3302, 3300)
        then 'Travel'
        else 'Others'
    end as mcc_category,
    case 
        when transaction_channel in ('ECOM','POS') then 'card'
        when is_tpap_initiated = false then 'off_app_upi'
        else 'upi' end as payment_channel,
    case 
        when amount < 100 then 'a.amount < 100'
        when amount >= 100 then 'b.amount >= 100'
    end as txn_amount_band,
    case 
        when round((switch_jewels_earned/5.0)*100.0/amount,1) = 0 then 0
        when round((switch_jewels_earned/5.0)*100.0/amount,1) = 0.4 then 0.4
        when round((switch_jewels_earned/5.0)*100.0/amount,1) = 2 then 2
    else round((switch_jewels_earned/5.0)*100.0/amount,1)
    end as perc_cashback
    
from csb.rupay_card.metabase_csb_card_onboarded o
left join csb.rupay_card.metabase_csb_card_transactions t on o.user_id = t.user_id
left join jupiter.user_one_view.report_user_proxy_income on o.user_id = report_user_proxy_income.user_id and update_month = date_trunc('month',o.card_issuance_at)
left join lifecycle l on t.user_id = l.transacting_user and date_trunc('month', t.created_at) = l.transaction_month
left join transactions_with_rewards r on r.jupiter_transaction_id = t.jupiter_transaction_id
left join switch s on o.user_id = s.user_id and date(t.created_at) >= date(s.created_at) and date(t.created_at) >= date('2024-03-27')
left join switch switch on o.user_id = switch.user_id 
)

select * from final
where 
    is_success_transaction = true 
    and transaction_type = 'DEBIT'
    
