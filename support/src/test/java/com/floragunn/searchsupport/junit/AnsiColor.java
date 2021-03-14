package com.floragunn.searchsupport.junit;

public class AnsiColor {

    public static final String RESET = "\033[0m";

    public static final String RED = "\033[0;31m";
    public static final String GREEN = "\033[0;32m";
    public static final String YELLOW = "\033[0;33m";
    public static final String BLUE = "\033[0;34m";
    public static final String PURPLE = "\033[0;35m";
    public static final String CYAN = "\033[0;36m";

    public static final String RED_BACKGROUND = "\033[41m";
    public static final String GREEN_BACKGROUND = "\033[42m";
    public static final String YELLOW_BACKGROUND = "\033[43m";
    public static final String BLUE_BACKGROUND = "\033[44m";
    public static final String PURPLE_BACKGROUND = "\033[45m";
    public static final String CYAN_BACKGROUND = "\033[46m";

    private static final String BLACK = "\033[0;30m";
    
    public static final String BLACK_ON_RED = BLACK + RED_BACKGROUND;
    public static final String BLACK_ON_GREEN = BLACK + GREEN_BACKGROUND;

}
