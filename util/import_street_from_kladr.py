import random
import time

import requests

streets = [

    (5301700028200, 3209),
    (5301700028300, 3210),
    (5301700028600, 3211),
    (5301700028800, 3212),
    (5301700029000, 3213),
    (5301700029100, 3214),
    (5301700029400, 3215),
    (5301700029700, 3216),
    (5301700029800, 3217),
    (5301700029900, 3218),
    (5301700030000, 3219),
    (5301700030100, 3220),
    (5301700030200, 3221),
    (5301700030400, 3222),
    (5301700030500, 3223),
    (5301700030600, 3224),
    (5301700030700, 3225),
    (5301700030800, 3226),
    (5301700030900, 3227),
    (5301700031000, 3228),
    (5301700031100, 3229),
    (5301700031300, 3230),
    (5301700031400, 3231),
    (5301700031500, 3232),
    (5301700031800, 3233),
    (5301700032200, 3234),
    (5301700032300, 3235),
    (5301700032700, 3236),
    (5301700032800, 3237),
    (5301700032900, 3238),
    (5301800000300, 3239),
    (5301800000400, 3240),
    (5301800000600, 3241),
    (5301800000700, 3242),
    (5301800000800, 3243),
    (5301800000900, 3244),
    (5301800001200, 3245),
    (5301800001300, 3246),
    (5301800001500, 3247),
    (5301800001600, 3248),
    (5301800001700, 3249),
    (5301800001900, 3250),
    (5301800002000, 3251),
    (5301800002300, 3252),
    (5301800002400, 3253),
    (5301800002500, 3254),
    (5301800002600, 3255),
    (5301800002700, 3256),
    (5301800002800, 3257),
    (5301800002900, 3258),
    (5301800003100, 3259),
    (5301800003200, 3260),
    (5301800003300, 3261),
    (5301800003400, 3262),
    (5301800003600, 3263),
    (5301800003700, 3264),
    (5301800003800, 3265),
    (5301800003900, 3266),
    (5301800004000, 3267),
    (5301800004100, 3268),
    (5301800004200, 3269),
    (5301800004400, 3270),
    (5301800004500, 3271),
    (5301800004600, 3272),
    (5301800004700, 3273),
    (5301800004800, 3274),
    (5301800004900, 3275),
    (5301800005000, 3276),
    (5301800005100, 3277),
    (5301800005200, 3278),
    (5301800005300, 3279),
    (5301800005400, 3280),
    (5301800005500, 3281),
    (5301800005600, 3282),
    (5301800005700, 3283),
    (5301800005800, 3284),
    (5301800005900, 3285),
    (5301800006000, 3286),
    (5301800006100, 3287),
    (5301800006200, 3288),
    (5301800006300, 3289),
    (5301800006400, 3290),
    (5301800006500, 3291),
    (5301800006600, 3292),
    (5301800006800, 3293),
    (5301800006900, 3294),
    (5301800007000, 3295),
    (5301800007100, 3296),
    (5301800007200, 3297),
    (5301800007300, 3298),
    (5301800007400, 3299),
    (5301800007500, 3300),
    (5301800007600, 3301),
    (5301800007700, 3302),
    (5301800007800, 3303),
    (5301800007900, 3304),
    (5301800008000, 3305),
    (5301800008200, 3306),
    (5301800008500, 3307),
    (5301800008700, 3308),
    (5301800008800, 3309),
    (5301800008900, 3310),
    (5301800009000, 3311),
    (5301800009100, 3312),
    (5301800009200, 3313),
    (5301800009300, 3314),
    (5301800009400, 3315),
    (5301800009500, 3316),
    (5301800009600, 3317),
    (5301800009700, 3318),
    (5301800009800, 3319),
    (5301800009900, 3320),
    (5301800010000, 3321),
    (5301800010100, 3322),
    (5301800010200, 3323),
    (5301800010300, 3324),
    (5301800010400, 3325),
    (5301800010500, 3326),
    (5301800010600, 3327),
    (5301800010700, 3328),
    (5301800010800, 3329),
    (5301800010900, 3330),
    (5301800011000, 3331),
    (5301800011100, 3332),
    (5301800011200, 3333),
    (5301800011300, 3334),
    (5301800011400, 3335),
    (5301800011500, 3336),
    (5301800011600, 3337),
    (5301800011700, 3338),
    (5301800011800, 3339),
    (5301800011900, 3340),
    (5301800012100, 3341),
    (5301800012200, 3342),
    (5301800012300, 3343),
    (5301800012400, 3344),
    (5301800012500, 3345),
    (5301800012600, 3346),
    (5301800012700, 3347),
    (5301800012800, 3348),
    (5301800012900, 3349),
    (5301800013000, 3350),
    (5301800013100, 3351),
    (5301800013200, 3352),
    (5301800013300, 3353),
    (5301800013400, 3354),
    (5301800013500, 3355),
    (5301800013600, 3356),
    (5301800013700, 3357),
    (5301800013800, 3358),
    (5301800013900, 3359),
    (5301800014000, 3360),
    (5301800014100, 3361),
    (5301800014200, 3362),
    (5301800014300, 3363),
    (5301800014400, 3364),
    (5301800014500, 3365),
    (5301800014600, 3366),
    (5301800014700, 3367),
    (5301800014800, 3368),
    (5301800014900, 3369),
    (5301800015000, 3370),
    (5301800015200, 3371),
    (5301800015300, 3372),
    (5301800015600, 3373),
    (5301800015700, 3374),
    (5301800015800, 3375),
    (5301800015900, 3376),
    (5301800016000, 3377),
    (5301800016100, 3378),
    (5301800016300, 3379),
    (5301900000200, 3380),
    (5301900000300, 3381),
    (5301900000400, 3382),
    (5301900000500, 3383),
    (5301900000700, 3384),
    (5301900000800, 3385),
    (5301900000900, 3386),
    (5301900001000, 3387),
    (5301900001100, 3388),
    (5301900001200, 3389),
    (5301900001300, 3390),
    (5301900001400, 3391),
    (5301900001500, 3392),
    (5301900001600, 3393),
    (5301900001700, 3394),
    (5301900001800, 3395),
    (5301900002000, 3396),
    (5301900002100, 3397),
    (5301900002200, 3398),
    (5301900002300, 3399),
    (5301900002400, 3400),
    (5301900002500, 3401),
    (5301900002600, 3402),
    (5301900002800, 3403),
    (5301900002900, 3404),
    (5301900003300, 3405),
    (5301900003400, 3406),
    (5301900003500, 3407),
    (5301900003700, 3408),
    (5301900003800, 3409),
    (5301900004200, 3410),
    (5301900004400, 3411),
    (5301900004500, 3412),
    (5301900004600, 3413),
    (5301900004700, 3414),
    (5301900004800, 3415),
    (5301900004900, 3416),
    (5301900005000, 3417),
    (5301900005100, 3418),
    (5301900005200, 3419),
    (5301900005300, 3420),
    (5301900005500, 3421),
    (5301900005600, 3422),
    (5301900005700, 3423),
    (5301900005800, 3424),
    (5301900006000, 3425),
    (5301900006100, 3426),
    (5301900006200, 3427),
    (5301900006300, 3428),
    (5301900006400, 3429),
    (5301900006500, 3430),
    (5301900006600, 3431),
    (5301900006700, 3432),
    (5301900006800, 3433),
    (5301900007100, 3434),
    (5301900007200, 3435),
    (5301900007300, 3436),
    (5301900007400, 3437),
    (5301900007500, 3438),
    (5301900007700, 3439),
    (5301900007800, 3440),
    (5301900007900, 3441),
    (5301900008000, 3442),
    (5301900008100, 3443),
    (5301900008200, 3444),
    (5301900008300, 3445),
    (5301900008400, 3446),
    (5301900008500, 3447),
    (5301900008600, 3448),
    (5301900008900, 3449),
    (5301900009000, 3450),
    (5301900009100, 3451),
    (5301900009200, 3452),
    (5301900009300, 3453),
    (5301900009400, 3454),
    (5301900009500, 3455),
    (5301900009900, 3456),
    (5301900010000, 3457),
    (5301900010100, 3458),
    (5301900010300, 3459),
    (5301900010400, 3460),
    (5301900010600, 3461),
    (5301900010700, 3462),
    (5301900010900, 3463),
    (5301900011000, 3464),
    (5301900011100, 3465),
    (5301900011200, 3466),
    (5301900011300, 3467),
    (5301900011400, 3468),
    (5301900011500, 3469),
    (5301900011600, 3470),
    (5301900011700, 3471),
    (5301900011800, 3472),
    (5301900011900, 3473),
    (5301900012000, 3474),
    (5301900012100, 3475),
    (5301900012300, 3476),
    (5301900012400, 3477),
    (5301900012500, 3478),
    (5301900012600, 3479),
    (5301900012700, 3480),
    (5301900012800, 3481),
    (5301900012900, 3482),
    (5301900013000, 3483),
    (5301900013100, 3484),
    (5301900013300, 3485),
    (5301900013400, 3486),
    (5301900013500, 3487),
    (5301900013600, 3488),
    (5301900013700, 3489),
    (5301900013800, 3490),
    (5301900013900, 3491),
    (5301900014000, 3492),
    (5301900014100, 3493),
    (5301900014200, 3494),
    (5301900014400, 3495),
    (5301900014500, 3496),
    (5301900014600, 3497),
    (5301900014700, 3498),
    (5301900014800, 3499),
    (5301900014900, 3500),
    (5301900015000, 3501),
    (5301900015100, 3502),
    (5301900015200, 3503),
    (5301900015400, 3504),
    (5301900015500, 3505),
    (5301900015700, 3506),
    (5301900015800, 3507),
    (5301900015900, 3508),
    (5301900016800, 3509),
    (5301900017000, 3510),
    (5302000000400, 3511),
    (5302000000500, 3512),
    (5302000000700, 3513),
    (5302000000900, 3514),
    (5302000001000, 3515),
    (5302000001100, 3516),
    (5302000001200, 3517),
    (5302000001300, 3518),
    (5302000001500, 3519),
    (5302000001600, 3520),
    (5302000001700, 3521),
    (5302000001800, 3522),
    (5302000001900, 3523),
    (5302000002000, 3524),
    (5302000002100, 3525),
    (5302000002200, 3526),
    (5302000002300, 3527),
    (5302000002500, 3528),
    (5302000002600, 3529),
    (5302000002700, 3530),
    (5302000002800, 3531),
    (5302000002900, 3532),
    (5302000003100, 3533),
    (5302000003200, 3534),
    (5302000003300, 3535),
    (5302000003400, 3536),
    (5302000003500, 3537),
    (5302000003600, 3538),
    (5302000003700, 3539),
    (5302000003800, 3540),
    (5302000003900, 3541),
    (5302000004100, 3542),
    (5302000004200, 3543),
    (5302000004300, 3544),
    (5302000004500, 3545),
    (5302000004700, 3546),
    (5302000004800, 3547),
    (5302000004900, 3548),
    (5302000005000, 3549),
    (5302000005100, 3550),
    (5302000005200, 3551),
    (5302000005300, 3552),
    (5302000005400, 3553),
    (5302000005500, 3554),
    (5302000005600, 3555),
    (5302000005700, 3556),
    (5302000005800, 3557),
    (5302000005900, 3558),
    (5302000006000, 3559),
    (5302000006100, 3560),
    (5302000006200, 3561),
    (5302000006300, 3562),
    (5302000006400, 3563),
    (5302000006500, 3564),
    (5302000006600, 3565),
    (5302000006700, 3566),
    (5302000006800, 3567),
    (5302000006900, 3568),
    (5302000007000, 3569),
    (5302000007100, 3570),
    (5302000007200, 3571),
    (5302000007300, 3572),
    (5302000007400, 3573),
    (5302000007500, 3574),
    (5302000007600, 3575),
    (5302000007700, 3576),
    (5302000008000, 3577),
    (5302000008100, 3578),
    (5302000008200, 3579),
    (5302000008300, 3580),
    (5302000008500, 3581),
    (5302000008600, 3582),
    (5302000008800, 3583),
    (5302000008900, 3584),
    (5302000009400, 3585),
    (5302000009500, 3586),
    (5302000013500, 3587),
    (5302100000200, 3588),
    (5302100000300, 3589),
    (5302100000400, 3590),
    (5302100000500, 3591),
    (5302100000600, 3592),
    (5302100000700, 3593),
    (5302100000800, 3594),
    (5302100000900, 3595),
    (5302100001000, 3596),
    (5302100001200, 3597),
    (5302100001500, 3598),
    (5302100001600, 3599),
    (5302100001700, 3600),
    (5302100001900, 3601),
    (5302100002000, 3602),
    (5302100002100, 3603),
    (5302100002200, 3604),
    (5302100002300, 3605),
    (5302100002400, 3606),
    (5302100002500, 3607),
    (5302100002600, 3608),
    (5302100002800, 3609),
    (5302100002900, 3610),
    (5302100003000, 3611),
    (5302100003100, 3612),
    (5302100003300, 3613),
    (5302100003400, 3614),
    (5302100003500, 3615),
    (5302100003600, 3616),
    (5302100003700, 3617),
    (5302100003800, 3618),
    (5302100003900, 3619),
    (5302100004000, 3620),
    (5302100004100, 3621),
    (5302100004200, 3622),
    (5302100004300, 3623),
    (5302100004400, 3624),
    (5302100004500, 3625),
    (5302100004700, 3626),
    (5302100004800, 3627),
    (5302100004900, 3628),
    (5302100005000, 3629),
    (5302100005100, 3630),
    (5302100005200, 3631),
    (5302100005300, 3632),
    (5302100005400, 3633),
    (5302100005500, 3634),
    (5302100005600, 3635),
    (5302100005700, 3636),
    (5302100005800, 3637),
    (5302100005900, 3638),
    (5302100006000, 3639),
    (5302100006100, 3640),
    (5302100006200, 3641),
    (5302100006300, 3642),
    (5302100006400, 3643),
    (5302100006500, 3644),
    (5302100006700, 3645),
    (5302100006800, 3646),
    (5302100006900, 3647),
    (5302100007000, 3648),
    (5302100007100, 3649),
    (5302100007200, 3650),
    (5302100007300, 3651),
    (5302100007400, 3652),
    (5302100007600, 3653),
    (5302100007700, 3654),
    (5302100007800, 3655),
    (5302100007900, 3656),
    (5302100008000, 3657),
    (5302100008100, 3658),
    (5302100008200, 3659),
    (5302100008300, 3660),
    (5302100008400, 3661),
    (5302100008500, 3662),
    (5302100008600, 3663),
    (5302100008700, 3664),
    (5302100008800, 3665),
    (5302100008900, 3666),
    (5302100009100, 3667),
    (5302100009200, 3668),
    (5302100009300, 3669),
    (5302100009400, 3670),
    (5302100009500, 3671),
    (5302100009700, 3672),
    (5302100009800, 3673),
    (5302100009900, 3674),
    (5302100010000, 3675),
    (5302100010100, 3676),
    (5302100010200, 3677),
    (5302100010300, 3678),
    (5302100010400, 3679),
    (5302100010500, 3680),
    (5302100010600, 3681),
    (5302100010700, 3682),
    (5302100010800, 3683),
    (5302100010900, 3684),
    (5302100011000, 3685),
    (5302100011100, 3686),
    (5302100011200, 3687),
    (5302100011300, 3688),
    (5302100011400, 3689),
    (5302100011500, 3690),
    (5302100011600, 3691),
    (5302100011700, 3692),
    (5302100011800, 3693),
    (5302100011900, 3694),
    (5302100012100, 3695),
    (5302100012200, 3696),
    (5302100012300, 3697),
    (5302100012500, 3698),
    (5302100012600, 3699),
    (5302100012700, 3700),
    (5302100012800, 3701),
    (5302100012900, 3702),
    (5302100013000, 3703),
    (5302100013100, 3704),
    (5302100013200, 3705),
    (5302100013300, 3706),
    (5302100013500, 3707),
    (5302100013600, 3708),
    (5302100014200, 3709),
    (5301300008800, 3710),
    (5301300009400, 3711),
    (5300100000100, 3712),
    (5300100009800, 3713),
    (5300100021700, 3714),
    (5300100031900, 3715),
    (5300100032000, 3716),
    (5300100032100, 3717),
    (5300100034000, 3718),
    (5300100034100, 3719),
    (5300100400000, 3720),
    (5300100500000, 3721),
    (5300100700000, 3722),
    (5300101800000, 3723),
    (5300200015400, 3724),
    (5300200015500, 3725),
    (5300200015600, 3726),
    (5300300009900, 3727),
    (5300300030000, 3728),
    (5300300033100, 3729),
    (5300300038200, 3730),
    (5300400014900, 3731),
    (5300400020500, 3732),
    (5300400020600, 3733),
    (5300400022200, 3734),
    (5300700013800, 3735),
    (5300700014600, 3736),
    (5300700014900, 3737),
    (5300800003200, 3738),
    (5300800003300, 3739),
    (5300800003600, 3740),
    (5300900000600, 3741),
    (5301000015400, 3742),
    (5301200001100, 3743),
    (5301200002200, 3744),
    (5301200002500, 3745),
    (5301200019000, 3746),
    (5301300002000, 3747),
    (5301400017800, 3748),
    (5301400023900, 3749),
    (5301400024000, 3750),
    (5301600019100, 3751),
    (5301600019200, 3752),
    (5301600019300, 3753),
    (5301700027900, 3754),
    (5301800000500, 3755),
    (5301800001000, 3756),
    (5301800001100, 3757),
    (5301800008100, 3758),
    (5301800015400, 3759),
    (5301800016200, 3760),
    (5302000000300, 3761),
    (5302000004400, 3762),
    (5302000007800, 3763),
    (5302000007900, 3764),
    (5302000008400, 3765),
    (5302000008700, 3766),
    (5302000009000, 3767),
    (5302000009100, 3768),
    (5302000009200, 3769),
    (5302000009800, 3770),
    (5302000010000, 3771),
    (5302000012000, 3772),
    (5302000012300, 3773),
    (5302000013000, 3774),
    (5302100001800, 3775),
    (5302100006600, 3776),
    (5302100014100, 3777)
]

desktop_agents = [
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0']


def random_headers():
    return {'User-Agent': random.choice(desktop_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'}


def processTransaction(trns, num, locality_id):
    with open('street_all.sql', 'a+', encoding='utf-8') as f:
        f.write('START TRANSACTION;\n')
        f.write("USE `pathfinder`;\n")

        index = 1
        for item in trns:
            f.write(
                "INSERT INTO street (locality_id, name, zip, type, kladr_id) VALUES({} ,'{}', '{}', '{}', '{}');\n".format(
                    locality_id, item['name'], item['zip'], item['typeShort'], item['id']))
            index += 1

        f.write('COMMIT;\n')
        f.close()

    #print(trns)
    return None


def requestJson(kladr_id, locality_id):
    out = []
    start = 0
    loadUrl = 'http://kladr-api.ru/api.php?cityId={}&contentType=street&offset={}&limit=100'

    while True:
        resp = requests.get(loadUrl.format(kladr_id, start), headers=random_headers())

        if resp.status_code == 404:
            break

        if resp.status_code != 200:
            print('GET {} {}'.format(resp.status_code, resp.content))
            break

        out = resp.json()['result']

        processTransaction(out, start, locality_id)

        start += 100

        if len(resp.json()['result']) != 100:
            break

        time.sleep(10 + int(random.random() * 5))

        print(start)

    return out


if __name__ == "__main__":
    cntr  = 1
    for street in streets:
        print(cntr, street[1])
        requestJson(street[0], street[1])
        cntr += 1
        if cntr == 1500:
            break


