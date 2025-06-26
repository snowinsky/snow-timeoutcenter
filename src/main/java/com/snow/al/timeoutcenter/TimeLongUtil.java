package com.snow.al.timeoutcenter;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.Optional;

public class TimeLongUtil {

    public static long currentTimeMillis() {
        return System.currentTimeMillis();
    }

    public static long currentTimeMillis(Date date) {
        return Optional.ofNullable(date).map(Date::getTime).orElse(currentTimeMillis());
    }

    public static long currentTimeMillis(Calendar calendar) {
        return Optional.ofNullable(calendar).map(Calendar::getTimeInMillis).orElse(currentTimeMillis());
    }

    public static long currentTimeMillis(LocalDateTime localDateTime) {
        return Optional.ofNullable(localDateTime).map(a -> a.toInstant(ZoneOffset.of("+8")).toEpochMilli()).orElse(currentTimeMillis());
    }

}
