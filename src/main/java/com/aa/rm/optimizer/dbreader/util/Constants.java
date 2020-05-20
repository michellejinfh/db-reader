package com.aa.rm.optimizer.dbreader.util;

public class Constants 
{
	public final static String DEFAULT_MARKET_REFERENCE = "**"; // origin/dest
	public final static String F_CABIN = "F";	
	public final static String C_CABIN = "C";	
	public final static String W_CABIN = "W";	
	public final static String Y_CABIN = "Y";
	
	// Query Constants
	//
	public static class Query
	{
		public static final String SELECT_FDC_FARE = 
				"SELECT A.FLT_ID_I, TO_CHAR(A.FLT_DPTR_DATE_D, 'YYYYMMDD') AS FLT_DPTR_DATE, B.LEG_ORIG_S, B.LEG_DEST_S, C.CABIN_CODE_C, " +
				"CASE WHEN A.FARE_DOM_SC_C = 'M' " +
				"THEN NVL(EVT_FARE_DOM_F, -1) " +
				"ELSE -1 END AS FARE_DOM, " +
				"CASE WHEN A.FARE_INTL_SC_C = 'M' " +
				"THEN NVL(EVT_FARE_INTL_F, -1) " +
				"ELSE -1 END AS FARE_INTL, " +
				"TRIM(TO_CHAR(A.FLT_DPTR_DATE_D, 'DAY')) AS DOW " +
				"FROM FDC_FARE A " +
				"JOIN CLASS_MAP C ON A.CLASS_ID_I=C.CLASS_ID_I " +
				"JOIN FD B ON A.FLT_ID_I=B.FLT_ID_I AND A.FLT_DPTR_DATE_D=B.FLT_DPTR_DATE_D " +				
				"WHERE A.CLASS_ID_I IN (60,10,100,180,40,90,230,160,250,80,110,130,120,220,70,190,140,170,150) " +
				"AND (A.FARE_DOM_SC_C='M' or A.FARE_INTL_SC_C='M') " +
				"AND A.FLT_DPTR_DATE_D = '05-JUN-20' " +
				"ORDER BY A.FLT_DPTR_DATE_D, A.FLT_ID_I, C.CABIN_CODE_C";

	}
}
