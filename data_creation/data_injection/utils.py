import random


def get_values_around_middle(middle, space, num_entries=14):
    return [random.randint(middle - space, middle + space) for i in range(num_entries)]


def get_values_around_middle_anomalous(middle, space, is_spike=False, num_entries=14):
    values = [
        random.randint(middle - space, middle + space) for i in range(num_entries - 1)
    ]
    if not is_spike:
        values.append(0)
    else:
        values.append(middle + random.randint(space * 5, space * 7))
    return values


def get_values_around_middle_weekly_seasonality(
    middle, space, weekly_middle, num_entries=14 * 7 + 3
):
    return [
        random.randint(weekly_middle - space, weekly_middle + space)
        if i % 7 <= 1
        else random.randint(middle - space, middle + space)
        for i in range(num_entries)
    ]


def get_values_around_middle_anomalous_weekly_seasonality(
    middle, space, weekly_middle, is_spike=False, num_entries=14 * 7 + 3
):
    values = [
        random.randint(weekly_middle - space, weekly_middle + space)
        if i % 7 <= 1
        else random.randint(middle - space, middle + space)
        for i in range(num_entries - 1)
    ]
    if not is_spike:
        values.append(0)
    else:
        if (num_entries - 1) % 7 <= 1:
            values.append(weekly_middle + random.randint(space * 5, space * 7))
        else:
            values.append(middle + random.randint(space * 5, space * 7))
    return values
