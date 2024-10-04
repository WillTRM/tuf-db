# returns accuracy on a scale of 0 - 100
def calculate_accuracy(te, e, ep, p, lp, l):
    perfect = p * 100
    elperfect = (ep + lp) * 75
    el = (e + l) * 40
    tooearly = te * 20

    weighted_sum = perfect + elperfect + el + tooearly
    judgement_count = te + e + ep + p + lp + l

    return weighted_sum / judgement_count


def calculate_pp(scorebase, xacc, speed, nomiss):
    if xacc < 95:
        xacc_multiplier = 1
    elif xacc < 100:
        xacc_multiplier = (-0.027 / ((xacc / 100) - 1.0054) + 0.513)
    elif xacc >= 100:
        xacc_multiplier = 10
    else:
        xacc_multiplier = 1

    if speed is None:
        speed_multiplier = 1
    elif speed < 1:
        speed_multiplier = 0
    elif speed < 1.1:
        speed_multiplier = (-3.5 * speed) + 4.5
    elif speed < 1.5:
        speed_multiplier = 0.65
    elif speed < 2:
        speed_multiplier = (0.7 * speed) - 0.4
    else:
        speed_multiplier = 1

    if nomiss:
        nm_multiplier = 1.1
    else:
        nm_multiplier = 1

    return scorebase * xacc_multiplier * speed_multiplier * nm_multiplier