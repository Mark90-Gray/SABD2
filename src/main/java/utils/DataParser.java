package utils;

import java.util.Calendar;
import java.util.Date;
import java.util.Locale;

public class DataParser {
    private static final Integer MAX_SIZE=720; //12h
    //HP: ritardo minore 720 minuti
    // Scelta progettuale, si prende solo il lowerbound dei ritardi


    //Definisce lo slot temporale del ritardo, AM: 05:00-11:59 PM: 12:00-19:00
    public static String getSlot(Date date){
        Calendar calendar= Calendar.getInstance(Locale.US);
        Calendar current= Calendar.getInstance(Locale.US);
        calendar.setTime(date);
        current.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY,5);
        calendar.set(Calendar.MINUTE,0);
        calendar.set(Calendar.SECOND,0);
        calendar.set(Calendar.MILLISECOND,0);
        if(current.before(calendar)){
            return "null";
        }
        calendar.set(Calendar.HOUR_OF_DAY,19);
        if(current.after(calendar)){
            return "null";
        }
        calendar.set(Calendar.HOUR_OF_DAY,12);
        if(current.before(calendar))
            return "AM";
        else
            return "PM";

    }

    //Compatta il nome della compagnia in un nome più leggibile ai fini della stampa,
    // es: nome (B2 -> nome
    public static String getParsedCompanyName(String company){

        // se campo inizia con carattere-> delay=-1
        // se campo vuoto , si assume ritardo nullo.
        company=company.trim(); //elimino spazio prima e dopo
        if(company.isEmpty())
            return null;
        char[] company_chars =company.toCharArray();
        boolean exit=false;
        int start=0,end=-1,i=0;
        do{
            if(company_chars[i]== ',' || company_chars[i]== '('){
                end=i;
                exit=true;
            }
            i++;
        }while(i< company_chars.length && !exit);
        if(i==company_chars.length)
            end=i;
        return company.substring(start,end);
    }

    /*Fissato il bug relativo al delay:
    es: ott-15-> min = 10*/
    public static int parsingDelayMonth(String[] s){
        String[] month ={"gen","feb","mar","apr","mag","giu","lug","ago","set","ott","nov","dic"};
        //Debug
        /*String temp=null;
        if(s.length!=0)
            temp=s[0];
        else
            temp="x";*/
        for (int i=0;i<month.length;i++){
            if(month[i].equals(s[0]))
                return (i+1);
        }
        return -1;
    }

    /*Parser del delay,
    legge la stringa finchè è un numero-> ritorna corrissptivo valore intero
    eccezione trattata nel caso in cui la stringa è un mese (bug delay)*/
    public static int getMinFromString(String delay){
        int min;
        String[] split_delay;
        // se campo inizia con carattere-> delay=-1
        // se campo vuoto , si assume ritardo nullo.
        delay=delay.trim().toLowerCase(); //elimino spazio prima e dopo
        if(delay.isEmpty())
            return 0;
        char[] delay_chars =delay.toCharArray();
        if(!Character.isDigit(delay.charAt(0))){
            //tuple con più "-"
            if(delay.contains("---"))
                return -1;
            if(delay.contains("-")){
                split_delay=delay.split("-");
                min=parsingDelayMonth(split_delay);
                return min;
            }
            else
                return -1;
        }
        boolean[] isNumber = new boolean[delay.length()];
        for(int i=0;i<delay_chars.length;i++){
            isNumber[i]=Character.isDigit(delay_chars[i]);
        }
        int start=0,end=-1,i=0;
        do{
            if(isNumber[i]){
                end=i;
            }
            i++;
        }while(i< isNumber.length && isNumber[i]);
        String result= delay.substring(start,end+1);
        min=Integer.parseInt(result);
        if(delay.contains("h"))
            return 60*min;
        if(min>MAX_SIZE)
            return -1;
        return min;
    }

    //Main Debug
   /* public static void main(String[] args) {
        System.out.println(getMinFromStringV2("----------"));

    }*/
}
