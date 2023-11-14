alter table AOL_SCHEMA.TIMEDIM
    rename column "day of the month" to "day";

update AOL_SCHEMA.TIMEDIM
    set "month" = TRIM("month");
