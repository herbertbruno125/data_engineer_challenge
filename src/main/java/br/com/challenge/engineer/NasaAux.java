package br.com.challenge.engineer;

import java.util.regex.Pattern;

public class NasaAux {

    public static Pattern hostsUnicosPattern = Pattern.compile("^\\S*");
    public static Pattern totalBytesPattern = Pattern.compile("[\\d-]+$");
    public static Pattern urlMaisCausaramError404Pattern = Pattern.compile("\"(.*?)\"");
    public static Pattern qtdErrosPorDiaPattern = Pattern.compile("(([0-9])|([0-2][0-9])|([3][0-1]))\\/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\/\\d{4}");
    public static Pattern quantidadeErros404PorDiaPattern = Pattern.compile("(([0-9])|([0-2][0-9])|([3][0-1]))\\/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\\/\\d{4}");

    public static String outputHostsUnicos = "";
    public static String outputTotalErros404 = "";
    public static String outputTotalErros404Aux = "";
    public static String outputTotalBytes = "";
    public static String outputUrlsQueMaisCausaramErro404 = "";
    public static String outputQuantidadeErros404PorDia = "";


    public static final String _ZERO = "0";
    public static final String _404 = " 404 ";


}
