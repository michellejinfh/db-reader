package com.aa.rm.optimizer.dbreader.service;

import com.aa.rm.optimizer.common.model.FDCFareCabinData;
import com.aa.rm.optimizer.common.model.FDCFareData;
import com.aa.rm.optimizer.dbreader.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import java.lang.reflect.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Service
public class FareLoadDBService extends LoadService
{
    private Class spaceClassToFeed = FDCFareData.class;
    private HashMap<String, String> dowMap;
    private final static Logger log = LoggerFactory.getLogger( FareLoadDBService.class );

    public FareLoadDBService() {
        dowMap = new HashMap<String, String>();
        dowMap.put("MONDAY", "1");
        dowMap.put("TUESDAY", "2");
        dowMap.put("WEDNESDAY", "3");
        dowMap.put("THURSDAY", "4");
        dowMap.put("FRIDAY", "5");
        dowMap.put("SATURDAY", "6");
        dowMap.put("SUNDAY", "7");
    }

    @Override
    public void loadData(String fileName)
    {
        log.info("Start Loading FDC_FARE table into cache");
        log.info(Constants.Query.SELECT_FDC_FARE);
        jdbcTemplate.query(Constants.Query.SELECT_FDC_FARE, new FDCFareExtractor());
        log.info("End Loading FDC_FARE table into cache");
    }

    @Override
    public Class getSpaceClass() {
        return spaceClassToFeed;
    }

    class FDCFareExtractor implements ResultSetExtractor<Integer> {
        private SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");

        @Override
        public Integer extractData(ResultSet rs) throws SQLException, DataAccessException
        {
            String preFltId = null;
            String curFltId = null;
            String preFltDptrDate = null;
            String curFltDptrDate = null;
            String curCabinCode = null;
            int recordCount = 0;
            int flightCount = 0;
            FDCFareData fdcFare = null;
            List<FDCFareData> fdcFareList = new ArrayList<FDCFareData>();
            float curFareDom;
            float curFareIntl;

            log.info("Start extracting data..");

            while (rs.next())
            {
                curFltId = rs.getString("FLT_ID_I");
                curFltDptrDate = rs.getString("FLT_DPTR_DATE");
                curCabinCode = rs.getString("CABIN_CODE_C");
                curFareDom = rs.getFloat("FARE_DOM");
                curFareIntl = rs.getFloat("FARE_INTL");

                if(preFltId==null || !curFltId.equals(preFltId) || !curFltDptrDate.equals(preFltDptrDate))
                {
                    // Load list of FDCFareData as batch into space
                    //
                    if(fdcFareList.size() >= spaceBatchLoadSize)
                    {
                        // Sort fare in each FDCFareData from low to high before writing to space
                        //
                        sortFareFromLowToHigh(fdcFareList);

//                        gigaSpace.writeMultiple(toArray(fdcFareList), WriteModifiers.UPDATE_OR_WRITE);
                        log.info(fdcFareList.size() + " FDCFareData space objects are written to space.. # of DB records read so far=" + recordCount);
                        fdcFareList.clear();
                    }

                    fdcFare = new FDCFareData();
                    fdcFare.setFlightId(curFltId);
                    try
                    {
                        fdcFare.setDptrDateAsDate(format.parse(curFltDptrDate));
                    }
                    catch(ParseException e)
                    {
                        e.printStackTrace();
                        throw new RuntimeException("Can not parse date:" + curFltDptrDate + " for flight ID:" + curFltId);
                    }

                    fdcFare.setOrigin(rs.getString("LEG_ORIG_S"));
                    fdcFare.setDest(rs.getString("LEG_DEST_S"));
                    fdcFare.setFcstDow(dowMap.get(rs.getString("DOW")));
                    fdcFare.setSpaceId(++flightCount);
                    fdcFare.setSpaceRoutingId(fdcFare.calculateRoutingId()); // compute
                    fdcFareList.add(fdcFare);
                }

                if(fdcFare.getCabinFareMap().containsKey(curCabinCode)==false)
                    fdcFare.getCabinFareMap().put(curCabinCode, new FDCFareCabinData());

                // Add domestic fare to that cabin if fare exists (-1 means fare does not exist or not applicable)
                //
                if(curFareDom != -1)
                    fdcFare.getCabinFareMap().get(curCabinCode).getCabinFareList().add(curFareDom);

                // Add international fare to that cabin if fare exists (-1 means fare does not exist or not applicable)
                //
                if(curFareIntl != -1)
                    fdcFare.getCabinFareMap().get(curCabinCode).getCabinFareList().add(curFareIntl);

                // Keep the max(dom/intl) for each cabin in the map
                //
                if(Math.max(curFareDom, curFareIntl) > fdcFare.getCabinFareMap().get(curCabinCode).getMaxLclFare())
                    fdcFare.getCabinFareMap().get(curCabinCode).setMaxLclFare(Math.max(curFareDom, curFareIntl));

                preFltId = curFltId;
                preFltDptrDate = curFltDptrDate;
                recordCount++;
            }

            // Load the last list of PNRData as batch into space if there is any
            //
            if(fdcFareList.size() > 0)
            {
                // Sort fare in each FDCFareData from low to high before writing to space
                //
                sortFareFromLowToHigh(fdcFareList);

//                gigaSpace.writeMultiple(toArray(fdcFareList), WriteModifiers.UPDATE_OR_WRITE);
                log.info(fdcFareList.size() + " FDCFareData space objects are written to space.. # of DB records read so far=" + recordCount);
                fdcFareList.clear();
            }

            log.info("End reading FDC_FARE from database. Total FDC_FARE read: " + recordCount + ", Total FDCFareData space object loaded: " + flightCount);

            // clear the result set
            //
            rs.close();

            return 1;
        }

        private FDCFareData[] toArray(List<FDCFareData> values) {
            return values.toArray((FDCFareData[]) Array.newInstance(FDCFareData.class, values.size()));
        }

        private void sortFareFromLowToHigh(List<FDCFareData> fdcFareList)
        {
            Map<String, FDCFareCabinData> cabinFareMap = null;

            for(FDCFareData fdcFareData : fdcFareList)
            {
                cabinFareMap = fdcFareData.getCabinFareMap();

                for (String cabinCode : cabinFareMap.keySet())
                {
                    // Sort fare from low to high for each cabin
                    //
                    Collections.sort(cabinFareMap.get(cabinCode).getCabinFareList());
                }
            }
        }
    } // End of Class FDCFareExtractor
}
